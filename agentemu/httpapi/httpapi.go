// Package httpapi implements the AgentAPI feedback loop over HTTP: the
// simulator posts the confirmed on-chain results of a finished round to an
// external agent service, which decides the next round's trace.
package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/HuangLab-SYSU/block-emulator-x/agentemu"
	"github.com/HuangLab-SYSU/block-emulator-x/agentemu/report"
)

// backoffBase and backoffCap bound the exponential retry delay.
const (
	backoffBase = 1 * time.Second
	backoffCap  = 8 * time.Second
)

// chainSummary is the on-chain part of the feedback payload: aggregate
// statistics plus per-transaction outcomes.
type chainSummary struct {
	*report.Stats
	Transactions []report.TxOutcome `json:"transactions"`
}

// nextTraceRequest is the JSON body posted to the agent service.
type nextTraceRequest struct {
	Round       int              `json:"round"`
	RecordCount int              `json:"record_count"`
	TxCount     int              `json:"tx_count"`
	Agents      []agentemu.Agent `json:"agents"`
	Chain       *chainSummary    `json:"chain,omitempty"`
}

// NextTraceResponse is the JSON body the agent service answers with. It is
// exported so agent service implementations (e.g. the bundled example) can
// decode/encode against the very same type.
type NextTraceResponse struct {
	Stop  bool              `json:"stop"`
	Trace []agentemu.Record `json:"trace"`
}

// HTTPAgentAPI implements agentemu.AgentAPI by posting each round's result to
// an external agent service. Failures are retried with exponential backoff;
// once the retries are spent the error surfaces and the simulation ends
// gracefully through the existing agent source stop path.
type HTTPAgentAPI struct {
	endpoint string
	client   *http.Client
	retries  int
	cfg      agentemu.Config
}

// New creates the HTTP feedback API from the agentemu config.
func New(cfg agentemu.Config) *HTTPAgentAPI {
	return &HTTPAgentAPI{
		endpoint: cfg.AgentAPI.Endpoint,
		client: &http.Client{
			Timeout: time.Duration(cfg.AgentAPI.RequestTimeoutSeconds) * time.Second,
		},
		retries: cfg.AgentAPI.Retries,
		cfg:     cfg,
	}
}

// NewAgentAPI is the config-driven factory for the feedback implementation:
// mode "file" keeps the round_<N>.jsonl story, mode "http" talks to an agent
// service.
func NewAgentAPI(cfg agentemu.Config) (agentemu.AgentAPI, error) {
	switch cfg.AgentAPI.Mode {
	case agentemu.AgentAPIModeFile:
		return agentemu.NewFileAgentAPI(filepath.Dir(cfg.Experiment.Trace)), nil
	case agentemu.AgentAPIModeHTTP:
		return New(cfg), nil
	default:
		return nil, fmt.Errorf("unsupported agent_api.mode %q", cfg.AgentAPI.Mode)
	}
}

// NextTrace posts the previous round's confirmed results and returns the next
// trace. A stop answer (or an empty trace) ends the simulation.
func (h *HTTPAgentAPI) NextTrace(ctx context.Context, prev agentemu.RoundResult) ([]agentemu.Record, error) {
	payload := h.buildPayload(prev)

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("marshal feedback payload: %w", err)
	}

	var (
		resp *NextTraceResponse
		last error
	)

	for attempt := 0; attempt <= h.retries; attempt++ {
		if attempt > 0 {
			if !sleepBackoff(ctx, attempt) {
				return nil, fmt.Errorf("agent api aborted: %w", context.Canceled)
			}
		}

		resp, last = h.tryOnce(ctx, body)
		if last == nil {
			break
		}

		slog.Warn("agent api request failed", "attempt", attempt+1, "retries", h.retries, "err", last)
	}

	if last != nil {
		return nil, fmt.Errorf("agent api at %s failed after %d attempts: %w", h.endpoint, h.retries+1, last)
	}

	if resp.Stop {
		return nil, nil
	}

	if err := validateTrace(resp.Trace); err != nil {
		return nil, fmt.Errorf("agent api returned an invalid trace: %w", err)
	}

	return resp.Trace, nil
}

func (h *HTTPAgentAPI) buildPayload(prev agentemu.RoundResult) nextTraceRequest {
	req := nextTraceRequest{
		Round:       prev.Round,
		RecordCount: prev.RecordCount,
		TxCount:     prev.TxCount,
		Agents:      loadRegistrySnapshot(h.cfg.Base.ResultDir),
	}

	planPath := filepath.Join(h.cfg.Base.ResultDir, agentemu.PlanFileName)

	stats, outcomes, err := report.Analyze(planPath, h.cfg.AgentAPI.ChainResultsCSV)
	if err != nil {
		// Local analysis failure is not the agent's fault: still ask for a
		// decision, just without the chain part of the payload.
		slog.Warn("analyze chain results for feedback failed", "err", err)

		return req
	}

	req.Chain = &chainSummary{Stats: stats, Transactions: outcomes}

	return req
}

func (h *HTTPAgentAPI) tryOnce(ctx context.Context, body []byte) (*NextTraceResponse, error) {
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, h.endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := h.client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("post: %w", err)
	}

	defer func() { _ = httpResp.Body.Close() }()

	if httpResp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %s", httpResp.Status)
	}

	resp := new(NextTraceResponse)
	if err := json.NewDecoder(httpResp.Body).Decode(resp); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	return resp, nil
}

func validateTrace(trace []agentemu.Record) error {
	if len(trace) == 0 {
		return errors.New("trace is empty (use \"stop\": true to end)")
	}

	for i, record := range trace {
		if record.AgentID == "" {
			return fmt.Errorf("record %d misses agent_id", i)
		}

		if record.Action == "" {
			return fmt.Errorf("record %d misses action", i)
		}
	}

	return nil
}

// sleepBackoff waits for the attempt's exponential delay; it returns false
// when the context fired first.
func sleepBackoff(ctx context.Context, attempt int) bool {
	delay := backoffBase << uint(attempt-1)
	if delay > backoffCap {
		delay = backoffCap
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// loadRegistrySnapshot reads the registry written by the agent source after
// every round, so the agent service always sees the current identities.
func loadRegistrySnapshot(resultDir string) []agentemu.Agent {
	b, err := os.ReadFile(filepath.Join(resultDir, agentemu.RegistryFileName))
	if err != nil {
		return nil
	}

	var agents []agentemu.Agent
	if json.Unmarshal(b, &agents) != nil {
		return nil
	}

	return agents
}
