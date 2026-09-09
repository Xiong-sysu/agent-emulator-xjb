// Command agentemu-example-agent is a template agent service for the
// AgentEmulator feedback loop. The simulator posts the confirmed on-chain
// results of every finished round to POST /next_trace, and this service
// answers with the next round's trace (or a stop).
//
// It ships two decision modes:
//
//		deterministic (default): pays between active agents until -max-rounds;
//		                         seeded, fully reproducible, no external calls.
//		-llm:                     asks an OpenAI-compatible chat API (DeepSeek by
//		                         default) to decide the next round; the API key is
//	                        read from the environment, never from a file.
//
// Use it as the starting point for your own agent service: keep the request
// and response types, replace the decision function.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/HuangLab-SYSU/block-emulator-x/agentemu"
	"github.com/HuangLab-SYSU/block-emulator-x/agentemu/httpapi"
	"github.com/HuangLab-SYSU/block-emulator-x/agentemu/report"
)

// chainSummary mirrors the feedback payload's on-chain part.
type chainSummary struct {
	*report.Stats
	Transactions []report.TxOutcome `json:"transactions"`
}

// feedbackRequest mirrors the simulator's POST body.
type feedbackRequest struct {
	Round       int              `json:"round"`
	RecordCount int              `json:"record_count"`
	TxCount     int              `json:"tx_count"`
	Agents      []agentemu.Agent `json:"agents"`
	Chain       *chainSummary    `json:"chain"`
}

const systemPrompt = `你是区块链仿真中的 Agent 决策者。用户消息是一份 JSON，包含：
- round：刚结束的轮次；agents：当前全部 agent（agent_id/did/active）；
- chain：链上执行结果（confirmed/total/success_rate/avg_confirm_latency_ms 及逐笔 transactions）。

请基于这些结果决定下一轮（第 round+1 轮）的 agent 行为，输出严格的 JSON：
{"stop": <bool>, "trace": [<record>, ...]}

record 字段：agent_id, action, target, amount, ts, request_id, params_hash。
action 只能是 join / pay / append_log / leave。
约束：
- 只能引用 agents 列表里出现过的 agent_id；pay 的双方必须 active（否则先用 join）；
- amount 为正整数；ts 为非负整数（按轮内顺序递增即可）；
- 想结束仿真时输出 {"stop": true, "trace": []}。
不要输出 JSON 以外的任何内容。`

// llmOptions carries the DeepSeek (or any OpenAI-compatible) client settings.
type llmOptions struct {
	apiBase     string
	model       string
	temperature float64
	apiKeyEnv   string
	timeout     time.Duration
}

type chatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type chatRequest struct {
	Model          string          `json:"model"`
	Messages       []chatMessage   `json:"messages"`
	Temperature    float64         `json:"temperature"`
	ResponseFormat *responseFormat `json:"response_format,omitempty"`
	Stream         bool            `json:"stream"`
}

type responseFormat struct {
	Type string `json:"type"`
}

type chatResponse struct {
	Choices []struct {
		Message chatMessage `json:"message"`
	} `json:"choices"`
}

type server struct {
	maxRounds int
	seed      int64
	llm       bool
	llmOpts   llmOptions
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9092", "listen address")
	maxRounds := flag.Int(
		"max-rounds",
		3,
		"rounds before the deterministic policy stops (round 1 is the initial trace)",
	)
	seed := flag.Int64("seed", 42, "seed of the deterministic policy")
	llm := flag.Bool("llm", false, "decide via an LLM chat API instead of the deterministic policy")
	apiBase := flag.String("api-base", "https://api.deepseek.com/v1", "OpenAI-compatible API base (LLM mode)")
	model := flag.String("model", "deepseek-chat", "model name (LLM mode)")
	temperature := flag.Float64("temperature", 0, "sampling temperature (LLM mode)")
	apiKeyEnv := flag.String("api-key-env", "DEEPSEEK_API_KEY", "environment variable holding the API key (LLM mode)")

	flag.Parse()

	s := &server{
		maxRounds: *maxRounds,
		seed:      *seed,
		llm:       *llm,
		llmOpts: llmOptions{
			apiBase:     strings.TrimRight(*apiBase, "/"),
			model:       *model,
			temperature: *temperature,
			apiKeyEnv:   *apiKeyEnv,
			timeout:     60 * time.Second,
		},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/next_trace", s.handleNextTrace)

	slog.Info("example agent service listening", "addr", *addr, "mode", modeName(*llm), "max_rounds", *maxRounds)

	if err := http.ListenAndServe(*addr, mux); err != nil {
		slog.Error("agent service exited", "err", err)
		os.Exit(1)
	}
}

func modeName(llm bool) string {
	if llm {
		return "llm"
	}

	return "deterministic"
}

func (s *server) handleNextTrace(w http.ResponseWriter, r *http.Request) {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4<<20))
	if err != nil {
		http.Error(w, fmt.Sprintf("read body: %v", err), http.StatusBadRequest)

		return
	}

	var req feedbackRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		http.Error(w, fmt.Sprintf("decode request: %v", err), http.StatusBadRequest)

		return
	}

	resp := httpapi.NextTraceResponse{}
	if s.llm {
		resp, err = llmDecide(r.Context(), s.llmOpts, raw)
		if err != nil {
			slog.Error("llm decision failed", "round", req.Round, "err", err)
			http.Error(w, fmt.Sprintf("llm decision failed: %v", err), http.StatusInternalServerError)

			return
		}
	} else {
		resp = deterministicDecide(req, s.maxRounds, s.seed)
	}

	confirmed, total := 0, 0
	if req.Chain != nil {
		confirmed, total = req.Chain.Confirmed, req.Chain.Total
	}

	slog.Info("next trace decided",
		"finished_round", req.Round,
		"chain", fmt.Sprintf("%d/%d confirmed", confirmed, total),
		"next_stop", resp.Stop,
		"next_actions", len(resp.Trace),
		"deterministic", !s.llm,
	)

	writeJSON(w, resp)
}

// deterministicDecide pays between two random active agents every round until
// the round cap. The seed includes the round, so decisions are reproducible
// without any locking.
func deterministicDecide(req feedbackRequest, maxRounds int, seed int64) httpapi.NextTraceResponse {
	next := req.Round + 1
	if next > maxRounds {
		return httpapi.NextTraceResponse{Stop: true}
	}

	active := make([]string, 0, len(req.Agents))
	for _, agent := range req.Agents {
		if agent.Active {
			active = append(active, agent.AgentID)
		}
	}

	if len(active) < 2 { // nobody left to pay each other
		return httpapi.NextTraceResponse{Stop: true}
	}

	rnd := rand.New(rand.NewSource(seed + int64(next))) //nolint:gosec // simulation seeding, not crypto
	from := active[rnd.Intn(len(active))]

	to := active[rnd.Intn(len(active))]
	for to == from {
		to = active[rnd.Intn(len(active))]
	}

	requestID := fmt.Sprintf("round-%d-pay-1", next)
	ts := int64(next * 100)

	return httpapi.NextTraceResponse{
		Trace: []agentemu.Record{
			{
				AgentID: from, Action: agentemu.ActionPay, Target: to,
				Amount: 1 + uint64(rnd.Intn(9)), TS: ts, RequestID: requestID,
			},
			{
				AgentID: from, Action: agentemu.ActionAppendLog,
				ParamsHash: "log-" + requestID, TS: ts + 1, RequestID: requestID,
			},
		},
	}
}

// llmDecide asks the chat API for the next round, feeding the raw feedback
// JSON as the user message. Failures surface as errors (HTTP 500), which the
// simulator retries with backoff before ending the run gracefully.
func llmDecide(ctx context.Context, opts llmOptions, rawRequest []byte) (httpapi.NextTraceResponse, error) {
	apiKey := os.Getenv(opts.apiKeyEnv)
	if apiKey == "" {
		return httpapi.NextTraceResponse{}, fmt.Errorf("environment variable %s is not set", opts.apiKeyEnv)
	}

	body, err := json.Marshal(chatRequest{
		Model: opts.model,
		Messages: []chatMessage{
			{Role: "system", Content: systemPrompt},
			{Role: "user", Content: string(rawRequest)},
		},
		Temperature:    opts.temperature,
		ResponseFormat: &responseFormat{Type: "json_object"},
	})
	if err != nil {
		return httpapi.NextTraceResponse{}, fmt.Errorf("marshal chat request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		opts.apiBase+"/chat/completions",
		bytes.NewReader(body),
	)
	if err != nil {
		return httpapi.NextTraceResponse{}, fmt.Errorf("build chat request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+apiKey)

	client := &http.Client{Timeout: opts.timeout}

	httpResp, err := client.Do(httpReq)
	if err != nil {
		return httpapi.NextTraceResponse{}, fmt.Errorf("call chat api: %w", err)
	}

	defer func() { _ = httpResp.Body.Close() }()

	if httpResp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(httpResp.Body, 512))

		return httpapi.NextTraceResponse{}, fmt.Errorf("chat api status %s: %s", httpResp.Status, string(respBody))
	}

	var chat chatResponse
	if err := json.NewDecoder(httpResp.Body).Decode(&chat); err != nil {
		return httpapi.NextTraceResponse{}, fmt.Errorf("decode chat response: %w", err)
	}

	if len(chat.Choices) == 0 {
		return httpapi.NextTraceResponse{}, errors.New("chat api returned no choices")
	}

	content := stripJSONFences(chat.Choices[0].Message.Content)

	resp := httpapi.NextTraceResponse{}
	if err := json.Unmarshal([]byte(content), &resp); err != nil {
		return httpapi.NextTraceResponse{}, fmt.Errorf("decode decided trace %q: %w", content, err)
	}

	return resp, nil
}

// stripJSONFences removes a wrapping ```json ... ``` block that some models
// still emit even with response_format=json_object.
func stripJSONFences(s string) string {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "```") {
		return s
	}

	if idx := strings.IndexByte(s, '\n'); idx > 0 {
		s = s[idx+1:]
	}

	s = strings.TrimSpace(s)
	s = strings.TrimSuffix(s, "```")

	return strings.TrimSpace(s)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("encode response", "err", err)
	}
}
