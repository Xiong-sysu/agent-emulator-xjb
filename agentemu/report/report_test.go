package report

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/HuangLab-SYSU/block-emulator-x/agentemu"
)

const chainCSVFixture = `OriginalHash,Tx create time,Tx finally commit time,Is cross-shard tx or not,Inner shard tx block propose time,Relay1 block propose time,Relay1 tx commit time,Relay2 block propose time,Relay2 tx commit time
hash-1,2026-09-09T10:00:00Z,2026-09-09T10:00:02Z,true,,2026-09-09T10:00:01Z,2026-09-09T10:00:02Z,,
hash-2,2026-09-09T10:00:01Z,2026-09-09T10:00:06Z,true,,,2026-09-09T10:00:03Z,2026-09-09T10:00:05Z,
hash-other,2026-09-09T09:00:00Z,2026-09-09T09:00:01Z,false,2026-09-09T09:00:01Z,,,,
`

func writeFixture(t *testing.T) (planPath, chainPath, dir string) {
	t.Helper()

	dir = t.TempDir()
	planPath = filepath.Join(dir, agentemu.PlanFileName)
	chainPath = filepath.Join(dir, "relay_stats_detail_tx_info.csv")

	plan := `{"hash":"hash-1","sender":"0x01","recipient":"0x02","value":"5","nonce":0,"data":"0x"}
{"hash":"hash-2","sender":"0x01","recipient":"0x02","value":"7","nonce":1,"data":"0x"}
{"hash":"hash-3","sender":"0x02","recipient":"0x01","value":"1","nonce":0,"data":"0x"}
`
	require.NoError(t, os.WriteFile(planPath, []byte(plan), 0o644))
	require.NoError(t, os.WriteFile(chainPath, []byte(chainCSVFixture), 0o644))

	return planPath, chainPath, dir
}

func TestReconcile(t *testing.T) {
	planPath, chainPath, _ := writeFixture(t)

	st, err := Reconcile(planPath, chainPath, 2)
	require.NoError(t, err)

	require.Equal(t, 3, st.Total)
	require.Equal(t, 2, st.Confirmed)
	require.Equal(t, 1, st.Missing)
	require.InDelta(t, 2.0/3.0, st.SuccessRate, 1e-9)
	require.Equal(t, 2, st.Rounds)

	// Duration spans the first create (10:00:00) to the last commit (10:00:06).
	require.InDelta(t, 6.0, st.DurationSec, 1e-9)
	require.InDelta(t, 2.0/6.0, st.ThroughputTPS, 1e-9)

	// Latencies: 2s for hash-1, 5s for hash-2 -> 3.5s average.
	require.InDelta(t, 3500.0, st.AvgConfirmLatencyMs, 1e-6)
}

func TestReconcileNothingConfirmed(t *testing.T) {
	dir := t.TempDir()
	planPath := filepath.Join(dir, agentemu.PlanFileName)
	chainPath := filepath.Join(dir, "chain.csv")

	require.NoError(t, os.WriteFile(planPath, []byte("{\"hash\":\"gone\"}\n"), 0o644))
	require.NoError(t, os.WriteFile(chainPath, []byte("OriginalHash,Tx create time,Tx finally commit time\n"), 0o644))

	st, err := Reconcile(planPath, chainPath, 1)
	require.NoError(t, err)

	require.Equal(t, 1, st.Total)
	require.Equal(t, 0, st.Confirmed)
	require.Equal(t, 1, st.Missing)
	require.Zero(t, st.SuccessRate)
	require.Zero(t, st.DurationSec)
	require.Zero(t, st.ThroughputTPS)
	require.Zero(t, st.AvgConfirmLatencyMs)
}

func TestWriteStatsCSV(t *testing.T) {
	out := filepath.Join(t.TempDir(), "Agent_Stats.csv")

	require.NoError(t, WriteStatsCSV(out, &Stats{
		Total: 8, Confirmed: 8, Missing: 0, SuccessRate: 1,
		Rounds: 2, DurationSec: 12.5, ThroughputTPS: 0.64, AvgConfirmLatencyMs: 3100.5,
	}))

	raw, err := os.ReadFile(out)
	require.NoError(t, err)
	require.Contains(t, string(raw), "total,confirmed,missing,success_rate,rounds,duration_s,throughput_tps,avg_confirm_latency_ms\n8,8,0,1.0000,2,12.500,0.6400,3100.500\n")
}

func TestRoundsFromMeta(t *testing.T) {
	dir := t.TempDir()

	rounds, err := RoundsFromMeta(dir) // missing file
	require.NoError(t, err)
	require.Zero(t, rounds)

	meta := []struct {
		Round int `json:"round"`
	}{{Round: 1}, {Round: 2}}
	b, err := json.Marshal(meta)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(dir, agentemu.RoundsMetaFile), b, 0o644))

	rounds, err = RoundsFromMeta(dir)
	require.NoError(t, err)
	require.Equal(t, 2, rounds)
}

func TestAnalyzeReturnsOrderedOutcomes(t *testing.T) {
	planPath, chainPath, _ := writeFixture(t)

	st, outcomes, err := Analyze(planPath, chainPath)
	require.NoError(t, err)
	require.Equal(t, 3, st.Total)
	require.Equal(t, 2, st.Confirmed)

	require.Len(t, outcomes, 3)
	require.Equal(t, "hash-1", outcomes[0].Hash)
	require.True(t, outcomes[0].Confirmed)
	require.True(t, outcomes[0].CrossShard)
	require.InDelta(t, 2000.0, outcomes[0].LatencyMs, 1e-6)

	require.Equal(t, "hash-2", outcomes[1].Hash)
	require.True(t, outcomes[1].Confirmed)
	require.InDelta(t, 5000.0, outcomes[1].LatencyMs, 1e-6)

	require.Equal(t, "hash-3", outcomes[2].Hash)
	require.False(t, outcomes[2].Confirmed)
	require.Zero(t, outcomes[2].LatencyMs)
}

func TestConfirmedHashes(t *testing.T) {
	_, chainPath, _ := writeFixture(t)

	confirmed, err := ConfirmedHashes(chainPath)
	require.NoError(t, err)
	require.Len(t, confirmed, 3) // every row, plan membership not required here
	require.Contains(t, confirmed, "hash-other")

	missing, err := ConfirmedHashes(filepath.Join(t.TempDir(), "not_there.csv"))
	require.NoError(t, err)
	require.Empty(t, missing)
}

func TestWaitAllConfirmedSeesLateArrivals(t *testing.T) {
	dir := t.TempDir()
	chainPath := filepath.Join(dir, "chain.csv")

	header := "OriginalHash,Tx create time,Tx finally commit time,Is cross-shard tx or not\n"
	require.NoError(t, os.WriteFile(chainPath, []byte(header), 0o644))

	// The chain confirms one hash after a short delay.
	go func() {
		time.Sleep(150 * time.Millisecond)
		f, err := os.OpenFile(chainPath, os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return
		}
		defer f.Close()
		_, _ = f.WriteString("late-hash,2026-09-09T10:00:00Z,2026-09-09T10:00:02Z,false,,,,\n")
	}()

	missing, err := WaitAllConfirmed(chainPath, []string{"late-hash"}, 3*time.Second, 50*time.Millisecond)
	require.NoError(t, err)
	require.Zero(t, missing)
}

func TestWaitAllConfirmedTimesOut(t *testing.T) {
	dir := t.TempDir()
	chainPath := filepath.Join(dir, "chain.csv")
	require.NoError(t, os.WriteFile(chainPath, []byte("OriginalHash,Tx create time\n"), 0o644))

	missing, err := WaitAllConfirmed(chainPath, []string{"gone-1", "gone-2"}, 120*time.Millisecond, 40*time.Millisecond)
	require.NoError(t, err) // a timeout is reported via the missing count
	require.Equal(t, 2, missing)
}
