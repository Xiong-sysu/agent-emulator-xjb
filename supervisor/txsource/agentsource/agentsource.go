package agentsource

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/HuangLab-SYSU/block-emulator-x/agentemu"
	"github.com/HuangLab-SYSU/block-emulator-x/agentemu/httpapi"
	"github.com/HuangLab-SYSU/block-emulator-x/agentemu/report"
	"github.com/HuangLab-SYSU/block-emulator-x/pkg/core/transaction"
)

const (
	Key = "agent_source"

	// confirmPollInterval is how often the chain CSV is re-read while waiting
	// for the finished round's transactions to be confirmed on chain.
	confirmPollInterval = 500 * time.Millisecond
)

// AgentSource implements TxSource by compiling agent traces into transactions
// inside the supervisor process: instead of shipping a plan file between two
// programs, the agentemu Host sits right here and serves transactions at the
// supervisor's injection speed.
//
// Whenever the transaction queue runs dry, the source asks its AgentAPI for
// the next round's trace and keeps serving, so a single chain run replays a
// multi-round story while nonces stay continuous (one Host lives for the
// whole run). The simulation stops when the next round file is missing, the
// round cap is reached, or tx_number is exhausted (the supervisor's own stop
// logic then winds everything down).
//
// In http feedback mode the next round is fetched on a background goroutine:
// the fetch waits for on-chain confirmation and for the agent service, and
// the supervisor's main loop must stay free meanwhile — it is the very loop
// that processes the block-info messages the confirmation wait depends on.
// The file mode keeps the original synchronous behavior.
type AgentSource struct {
	cfg       agentemu.Config
	host      *agentemu.Host
	api       agentemu.AgentAPI
	maxRounds int
	round     int

	queue []transaction.Transaction
	done  bool

	compiledTxs    int // transactions compiled by the Host so far (it returns cumulative results)
	compiledHashes []string
	rounds         []agentemu.RoundResult

	// waitConfirm and chainCSV drive the "results are only fed back once
	// confirmed on chain" guarantee in http feedback mode.
	waitConfirm time.Duration
	chainCSV    string

	// async is set in http feedback mode.
	async bool

	// mu guards every mutable field above; fetching marks an in-flight
	// background round fetch and fetchErr carries its failure to the next
	// ReadTxs call (surfaced exactly once).
	mu       sync.Mutex
	fetching bool
	fetchErr error
}

// NewAgentSource loads the agentemu config, compiles the initial trace
// (round 1) eagerly, and prepares the round source. maxRounds caps the total
// number of rounds including round 1; values below 1 mean exactly one round.
func NewAgentSource(configPath string, maxRounds int) (*AgentSource, error) {
	if maxRounds <= 0 {
		maxRounds = 1
	}

	cfg, err := agentemu.LoadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("load agentemu config: %w", err)
	}

	host, err := agentemu.NewHost(cfg)
	if err != nil {
		return nil, fmt.Errorf("create agentemu host: %w", err)
	}

	records, err := agentemu.LoadTrace(cfg.Experiment.Trace)
	if err != nil {
		return nil, fmt.Errorf("load initial trace: %w", err)
	}

	// The feedback implementation is config-driven: the default file mode
	// keeps the round_<N>.jsonl story, http mode talks to an agent service.
	api, err := httpapi.NewAgentAPI(cfg)
	if err != nil {
		return nil, fmt.Errorf("create agent api: %w", err)
	}

	a := &AgentSource{
		cfg:       cfg,
		host:      host,
		api:       api,
		maxRounds: maxRounds,
	}

	if cfg.AgentAPI.Mode == agentemu.AgentAPIModeHTTP {
		a.async = true
		a.waitConfirm = time.Duration(cfg.AgentAPI.WaitConfirmTimeoutSeconds) * time.Second
		a.chainCSV = cfg.AgentAPI.ChainResultsCSV
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if err := a.appendRound(1, records); err != nil {
		return nil, err
	}

	return a, nil
}

func (a *AgentSource) ReadTxs(size int64) ([]transaction.Transaction, error) {
	if !a.async {
		return a.readTxsSync(size)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Surface a background fetch failure exactly once, preserving the
	// one-error-then-graceful-stop behavior of the synchronous path.
	if a.fetchErr != nil {
		err := a.fetchErr
		a.fetchErr = nil

		return nil, err
	}

	a.startFetchLocked()

	return a.popLocked(size), nil
}

// readTxsSync is the original synchronous serving path (file mode): the next
// round file is read inline whenever the queue runs dry.
func (a *AgentSource) readTxsSync(size int64) ([]transaction.Transaction, error) {
	if err := a.fill(); err != nil {
		return nil, err
	}

	ret := make([]transaction.Transaction, 0, size)
	for int64(len(ret)) < size && len(a.queue) > 0 {
		ret = append(ret, a.queue[0])
		a.queue = a.queue[1:]

		if err := a.fill(); err != nil {
			return nil, err
		}
	}

	return ret, nil
}

// Exhausted implements txsource.Exhaustible: once the story is over and the
// queue is drained (with no fetch in flight), no further transactions will
// arrive, so the supervisor's empty-block stop logic may fire even with
// tx_number left over.
func (a *AgentSource) Exhausted() bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.done && len(a.queue) == 0
}

func (a *AgentSource) popLocked(size int64) []transaction.Transaction {
	ret := make([]transaction.Transaction, 0, size)

	for int64(len(ret)) < size && len(a.queue) > 0 {
		ret = append(ret, a.queue[0])
		a.queue = a.queue[1:]
	}

	return ret
}

// startFetchLocked launches the background round fetch when the queue ran
// dry. See the AgentSource doc comment for why this must not block.
func (a *AgentSource) startFetchLocked() {
	if len(a.queue) > 0 || a.done || a.fetching {
		return
	}

	nextRound := a.round + 1
	if nextRound > a.maxRounds {
		a.done = true

		return
	}

	hashes := slices.Clone(a.compiledHashes)
	waitConfirm, chainCSV := a.waitConfirm, a.chainCSV
	a.fetching = true

	go func() {
		waitForConfirmation(waitConfirm, chainCSV, hashes)

		records, err := a.api.NextTrace(context.Background(), agentemu.RoundResult{Round: nextRound - 1})

		a.mu.Lock()
		defer a.mu.Unlock()

		a.fetching = false

		switch {
		case err != nil:
			a.done = true
			a.fetchErr = fmt.Errorf("fetch round %d: %w", nextRound, err)
		case len(records) == 0:
			a.done = true // the agent said stop
		default:
			if err := a.appendRound(nextRound, records); err != nil {
				a.done = true
				a.fetchErr = err
			}
		}
	}()
}

// fill pulls the next round from the AgentAPI while the queue is empty (the
// synchronous file-mode path). An API error surfaces once and then ends the
// source, so the supervisor logs it once and still reaches its regular
// graceful stop.
func (a *AgentSource) fill() error {
	for len(a.queue) == 0 && !a.done {
		nextRound := a.round + 1
		if nextRound > a.maxRounds {
			a.done = true
			break
		}

		records, err := a.api.NextTrace(context.Background(), agentemu.RoundResult{Round: a.round})
		if err != nil {
			a.done = true
			return fmt.Errorf("fetch round %d: %w", nextRound, err)
		}

		if len(records) == 0 {
			a.done = true // no next round: the agent (or story) said stop
			break
		}

		if err := a.appendRound(nextRound, records); err != nil {
			a.done = true
			return err
		}
	}

	return nil
}

// waitForConfirmation blocks until every compiled transaction has appeared in
// the chain measurement CSV, so the agent decides on real on-chain results.
// A timeout only warns: the round continues with the data confirmed so far.
func waitForConfirmation(waitConfirm time.Duration, chainCSV string, hashes []string) {
	if waitConfirm <= 0 || len(hashes) == 0 {
		return
	}

	missing, err := report.WaitAllConfirmed(chainCSV, hashes, waitConfirm, confirmPollInterval)
	if err != nil {
		slog.Warn("waiting for on-chain confirmation failed", "err", err)

		return
	}

	if missing > 0 {
		slog.Warn("on-chain confirmation wait timed out; feeding back partial results",
			"missing", missing, "waited", waitConfirm.String())
	}
}

// appendRound compiles one round and refreshes the on-disk results, so even a
// killed run leaves the plan and registry of everything queued so far. The
// caller must hold a.mu.
func (a *AgentSource) appendRound(round int, records []agentemu.Record) error {
	result, err := a.host.Process(records)
	if err != nil {
		return fmt.Errorf("process round %d: %w", round, err)
	}

	a.round = round
	// The Host returns its cumulative compilation, so only the tail is new.
	newTxs := result.Transactions[a.compiledTxs:]
	roundTxCount := len(newTxs)

	// Re-stamp real creation times: the on-chain latency metrics derive from
	// tx.CreateTime, and the trace's logical timestamps (ts=1,2,...) would
	// poison them. Tx hashes do not cover CreateTime, so the plan written
	// below reconciles with the chain unchanged.
	now := time.Now()
	for i := range newTxs {
		newTxs[i].CreateTime = now
	}

	if err := a.collectHashes(newTxs); err != nil {
		return err
	}

	a.queue = append(a.queue, newTxs...)
	a.compiledTxs = len(result.Transactions)
	a.rounds = append(a.rounds, agentemu.RoundResult{Round: round, RecordCount: len(records), TxCount: roundTxCount})

	if err := a.host.WriteResult(a.cfg.Base.ResultDir, result); err != nil {
		return fmt.Errorf("write results of round %d: %w", round, err)
	}

	return writeRoundsMeta(a.cfg.Base.ResultDir, a.rounds)
}

// collectHashes remembers the new transactions' hashes for the confirmation
// wait of the following round.
func (a *AgentSource) collectHashes(txs []transaction.Transaction) error {
	for i := range txs {
		hash, err := txs[i].Hash()
		if err != nil {
			return fmt.Errorf("hash planned transaction: %w", err)
		}

		a.compiledHashes = append(a.compiledHashes, hex.EncodeToString(hash))
	}

	return nil
}

// writeRoundsMeta persists the per-round bookkeeping consumed by the report
// command (agentemu-report).
func writeRoundsMeta(dir string, rounds []agentemu.RoundResult) error {
	b, err := json.MarshalIndent(rounds, "", "  ")
	if err != nil {
		return fmt.Errorf("encode rounds meta: %w", err)
	}

	if err := os.WriteFile(filepath.Join(dir, agentemu.RoundsMetaFile), append(b, '\n'), 0o644); err != nil {
		return fmt.Errorf("write rounds meta: %w", err)
	}

	return nil
}
