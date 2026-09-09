// Package report reconciles the planned agent transactions against the
// transactions confirmed on chain and produces summary statistics.
package report

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/HuangLab-SYSU/block-emulator-x/agentemu"
)

// StatsFile is the report file name written next to the plan by default.
const StatsFile = "Agent_Stats.csv"

// Stats summarizes the reconciliation between the plan file
// (agent_transactions.jsonl) and the confirmed transactions
// (relay_stats_detail_tx_info.csv).
type Stats struct {
	Total               int     `json:"total"`
	Confirmed           int     `json:"confirmed"`
	Missing             int     `json:"missing"`
	SuccessRate         float64 `json:"success_rate"`
	Rounds              int     `json:"rounds"`
	DurationSec         float64 `json:"duration_s"`
	ThroughputTPS       float64 `json:"throughput_tps"`
	AvgConfirmLatencyMs float64 `json:"avg_confirm_latency_ms"`
}

// TxOutcome is the per-transaction piece of the feedback sent to an agent.
type TxOutcome struct {
	Hash       string  `json:"hash"`
	Confirmed  bool    `json:"confirmed"`
	LatencyMs  float64 `json:"latency_ms"`
	CrossShard bool    `json:"cross_shard"`
}

var statsHeader = []string{
	"total", "confirmed", "missing", "success_rate",
	"rounds", "duration_s", "throughput_tps", "avg_confirm_latency_ms",
}

// Reconcile compares the plan file against the relay detail CSV and returns
// the statistics. Rounds below 1 leaves the round count unset; pass the count
// from RoundsFromMeta to fill it in.
func Reconcile(planPath, chainCSV string, rounds int) (*Stats, error) {
	st, _, err := Analyze(planPath, chainCSV)
	if err != nil {
		return nil, err
	}

	st.Rounds = rounds

	return st, nil
}

// Analyze reconciles the plan against the chain CSV and additionally returns
// the per-transaction outcomes (ordered like the plan). It is the feedback
// payload source for agent services.
func Analyze(planPath, chainCSV string) (*Stats, []TxOutcome, error) {
	planHashes, err := loadPlanHashes(planPath)
	if err != nil {
		return nil, nil, err
	}

	confirmed, firstCreate, lastCommit, err := scanChainCSV(chainCSV, planHashes)
	if err != nil {
		return nil, nil, err
	}

	st := &Stats{
		Total:     len(planHashes),
		Confirmed: len(confirmed),
		Missing:   len(planHashes) - len(confirmed),
	}

	if st.Total > 0 {
		st.SuccessRate = float64(st.Confirmed) / float64(st.Total)
	}

	if st.Confirmed > 0 && lastCommit.After(firstCreate) {
		st.DurationSec = lastCommit.Sub(firstCreate).Seconds()
		if st.DurationSec > 0 {
			st.ThroughputTPS = float64(st.Confirmed) / st.DurationSec
		}
	}

	outcomes := make([]TxOutcome, 0, len(planHashes))
	latencySum := 0.0

	for _, hash := range planHashes {
		outcome := TxOutcome{Hash: hash}

		if detail, ok := confirmed[hash]; ok {
			outcome.Confirmed = true
			outcome.LatencyMs = detail.latencySec * 1000
			outcome.CrossShard = detail.crossShard
			latencySum += detail.latencySec
		}

		outcomes = append(outcomes, outcome)
	}

	if st.Confirmed > 0 {
		st.AvgConfirmLatencyMs = latencySum * 1000 / float64(st.Confirmed)
	}

	return st, outcomes, nil
}

type txDetail struct {
	latencySec float64
	crossShard bool
}

// loadPlanHashes reads the planned transaction hashes in plan order (one
// JSON object per line).
func loadPlanHashes(planPath string) ([]string, error) {
	f, err := os.Open(planPath)
	if err != nil {
		return nil, fmt.Errorf("open plan file: %w", err)
	}

	defer func() { _ = f.Close() }()

	var planHashes []string

	dec := json.NewDecoder(f)

	for {
		var line struct {
			Hash string `json:"hash"`
		}
		if err := dec.Decode(&line); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, fmt.Errorf("decode plan file: %w", err)
		}

		if line.Hash != "" {
			planHashes = append(planHashes, line.Hash)
		}
	}

	return planHashes, nil
}

// scanChainCSV collects the confirmed subset of the plan hashes (with latency
// and cross-shard details) plus the time span of the run. Column layout comes
// from relaystats (OriginalHash, "Tx create time", "Tx finally commit time",
// "Is cross-shard tx or not", ...); timestamps are RFC3339 with empty cells
// for missing phases.
func scanChainCSV(
	chainCSV string,
	planHashes []string,
) (map[string]txDetail, time.Time, time.Time, error) {
	wanted := make(map[string]struct{}, len(planHashes))
	for _, hash := range planHashes {
		wanted[hash] = struct{}{}
	}

	f, err := os.Open(chainCSV)
	if err != nil {
		return nil, time.Time{}, time.Time{}, fmt.Errorf("open chain csv: %w", err)
	}

	defer func() { _ = f.Close() }()

	r := csv.NewReader(f)

	confirmed := make(map[string]txDetail)

	var firstCreate, lastCommit time.Time

	header := true

	for {
		row, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, time.Time{}, time.Time{}, fmt.Errorf("read chain csv: %w", err)
		}

		if header {
			header = false
			continue
		}

		if len(row) < 3 {
			continue
		}

		hash := row[0]
		if _, ok := wanted[hash]; !ok {
			continue
		}

		createTime, err := time.Parse(time.RFC3339, row[1])
		if err != nil {
			return nil, time.Time{}, time.Time{}, fmt.Errorf("parse create time of %s: %w", hash, err)
		}

		commitTime, err := time.Parse(time.RFC3339, row[2])
		if err != nil {
			return nil, time.Time{}, time.Time{}, fmt.Errorf("parse commit time of %s: %w", hash, err)
		}

		confirmed[hash] = txDetail{
			latencySec: commitTime.Sub(createTime).Seconds(),
			crossShard: len(row) > 3 && row[3] == "true",
		}

		if firstCreate.IsZero() || createTime.Before(firstCreate) {
			firstCreate = createTime
		}

		if lastCommit.IsZero() || commitTime.After(lastCommit) {
			lastCommit = commitTime
		}
	}

	return confirmed, firstCreate, lastCommit, nil
}

// ConfirmedHashes returns the transaction hashes that have appeared in the
// chain CSV so far. A missing file means nothing is confirmed yet.
func ConfirmedHashes(chainCSV string) (map[string]struct{}, error) {
	f, err := os.Open(chainCSV)
	if os.IsNotExist(err) {
		return map[string]struct{}{}, nil
	}

	if err != nil {
		return nil, fmt.Errorf("open chain csv: %w", err)
	}

	defer func() { _ = f.Close() }()

	r := csv.NewReader(f)
	r.FieldsPerRecord = -1 // only the hash column matters here

	confirmed := make(map[string]struct{})

	header := true

	for {
		row, err := r.Read()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return nil, fmt.Errorf("read chain csv: %w", err)
		}

		if header {
			header = false
			continue
		}

		if len(row) > 0 && row[0] != "" {
			confirmed[row[0]] = struct{}{}
		}
	}

	return confirmed, nil
}

// WaitAllConfirmed polls the chain CSV until every hash has appeared or the
// timeout passes; it returns the number of hashes still missing (0 on
// success). The CSV is flushed line by line by the measure module, so rows
// are visible as soon as a transaction's lifecycle completes.
func WaitAllConfirmed(chainCSV string, hashes []string, timeout, poll time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)

	for {
		confirmed, err := ConfirmedHashes(chainCSV)
		if err != nil {
			return len(hashes), err
		}

		missing := 0

		for _, hash := range hashes {
			if _, ok := confirmed[hash]; !ok {
				missing++
			}
		}

		if missing == 0 {
			return 0, nil
		}

		if time.Now().After(deadline) {
			return missing, nil
		}

		time.Sleep(poll)
	}
}

// WriteStatsCSV writes the stats row (with header) to outPath.
func WriteStatsCSV(outPath string, st *Stats) error {
	row := []string{
		strconv.Itoa(st.Total),
		strconv.Itoa(st.Confirmed),
		strconv.Itoa(st.Missing),
		strconv.FormatFloat(st.SuccessRate, 'f', 4, 64),
		strconv.Itoa(st.Rounds),
		strconv.FormatFloat(st.DurationSec, 'f', 3, 64),
		strconv.FormatFloat(st.ThroughputTPS, 'f', 4, 64),
		strconv.FormatFloat(st.AvgConfirmLatencyMs, 'f', 3, 64),
	}

	f, err := os.Create(outPath)
	if err != nil {
		return fmt.Errorf("create stats csv: %w", err)
	}

	w := csv.NewWriter(f)
	if err := w.Write(statsHeader); err != nil {
		_ = f.Close()
		return fmt.Errorf("write stats header: %w", err)
	}

	if err := w.Write(row); err != nil {
		_ = f.Close()
		return fmt.Errorf("write stats row: %w", err)
	}

	w.Flush()

	if err := w.Error(); err != nil {
		_ = f.Close()
		return fmt.Errorf("flush stats csv: %w", err)
	}

	return f.Close()
}

// RoundsFromMeta reads the round count from the agent source's rounds meta
// file. A missing file is not an error (returns 0).
func RoundsFromMeta(dir string) (int, error) {
	b, err := os.ReadFile(filepath.Join(dir, agentemu.RoundsMetaFile))
	if os.IsNotExist(err) {
		return 0, nil
	}

	if err != nil {
		return 0, fmt.Errorf("read agent rounds meta: %w", err)
	}

	var rounds []struct {
		Round int `json:"round"`
	}
	if err := json.Unmarshal(b, &rounds); err != nil {
		return 0, fmt.Errorf("decode agent rounds meta: %w", err)
	}

	return len(rounds), nil
}
