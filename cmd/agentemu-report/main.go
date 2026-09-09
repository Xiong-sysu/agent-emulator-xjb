package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/HuangLab-SYSU/block-emulator-x/agentemu/report"
)

// agentemu-report reconciles the planned agent transactions
// (agent_transactions.jsonl) against the transactions confirmed on chain
// (relay_stats_detail_tx_info.csv) and writes Agent_Stats.csv.
func main() {
	planPath := flag.String("plan", "", "path to agent_transactions.jsonl")
	chainCSV := flag.String("chain", "", "path to relay_stats_detail_tx_info.csv")
	outPath := flag.String("out", "", "output csv path (default: Agent_Stats.csv next to the plan)")
	rounds := flag.Int("rounds", 0, "round count; 0 reads it from agent_rounds.json next to the plan")

	flag.Parse()

	if *planPath == "" || *chainCSV == "" {
		slog.Error("-plan and -chain are required")
		os.Exit(1)
	}

	roundCount := *rounds
	if roundCount <= 0 {
		var err error
		if roundCount, err = report.RoundsFromMeta(filepath.Dir(*planPath)); err != nil {
			slog.Error("read rounds meta", "err", err)
			os.Exit(1)
		}
	}

	st, err := report.Reconcile(*planPath, *chainCSV, roundCount)
	if err != nil {
		slog.Error("reconcile", "err", err)
		os.Exit(1)
	}

	out := *outPath
	if out == "" {
		out = filepath.Join(filepath.Dir(*planPath), report.StatsFile)
	}

	if err := report.WriteStatsCSV(out, st); err != nil {
		slog.Error("write stats", "err", err)
		os.Exit(1)
	}

	fmt.Printf(
		"agentemu-report: %d/%d confirmed (%.2f%%), %d missing, %d round(s), %.3fs, %.4f tps, %.3f ms avg latency -> %s\n",
		st.Confirmed,
		st.Total,
		st.SuccessRate*100,
		st.Missing,
		st.Rounds,
		st.DurationSec,
		st.ThroughputTPS,
		st.AvgConfirmLatencyMs,
		out,
	)
}
