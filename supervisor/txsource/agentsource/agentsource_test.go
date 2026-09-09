package agentsource

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/HuangLab-SYSU/block-emulator-x/agentemu"
	"github.com/HuangLab-SYSU/block-emulator-x/pkg/core/account"
	"github.com/HuangLab-SYSU/block-emulator-x/pkg/core/transaction"
	"github.com/HuangLab-SYSU/block-emulator-x/pkg/utils"
)

const (
	identityContract = "0x0000000000000000000000000000000000000010"
	auditContract    = "0x0000000000000000000000000000000000000020"
	seed             = 42
)

const round1 = `{"agent_id":"alice","action":"join","params_hash":"doc-a","ts":1}
{"agent_id":"bob","action":"join","params_hash":"doc-b","ts":2}
{"agent_id":"alice","action":"pay","target":"bob","amount":5,"request_id":"p1","ts":3}
`

const round2 = `{"agent_id":"alice","action":"pay","target":"bob","amount":7,"request_id":"p2","ts":4}
{"agent_id":"bob","action":"leave","params_hash":"exit-b","ts":5}
`

func didAddr(t *testing.T, agentID string) account.Address {
	t.Helper()

	h := sha256.Sum256([]byte(fmt.Sprintf("%d:%s", seed, agentID)))
	addr, err := utils.Hex2Addr("0x" + hex.EncodeToString(h[len(h)-20:]))
	require.NoError(t, err)

	return addr
}

func identityAddr(t *testing.T) account.Address {
	t.Helper()

	addr, err := utils.Hex2Addr(identityContract)
	require.NoError(t, err)

	return addr
}

func auditAddr(t *testing.T) account.Address {
	t.Helper()

	addr, err := utils.Hex2Addr(auditContract)
	require.NoError(t, err)

	return addr
}

// newTestSource writes a minimal agentemu config plus two round files and
// returns a source that serves both rounds (or one, if maxRounds is 1).
func newTestSource(t *testing.T, maxRounds int, extraRound2 string) *AgentSource {
	t.Helper()

	dir := t.TempDir()
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	require.NoError(t, os.WriteFile(filepath.Join(dir, "round_1.jsonl"), []byte(round1), 0o644))

	round2Body := round2
	if extraRound2 != "" {
		round2Body = extraRound2
	}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "round_2.jsonl"), []byte(round2Body), 0o644))

	cfg := `base:
  result_dir: ` + filepath.Join(dir, "results") + `
experiment:
  seed: ` + fmt.Sprintf("%d", seed) + `
  trace: ` + filepath.Join(dir, "round_1.jsonl") + `
protocols:
  pay:
    plugin: direct-pay
  audit:
    plugin: merkle-audit
    contract_address: "` + auditContract + `"
    batch_size: 2
  identity:
    plugin: did-simple
    contract_address: "` + identityContract + `"
`
	cfgPath := filepath.Join(dir, "agentEmuConfig.yaml")
	require.NoError(t, os.WriteFile(cfgPath, []byte(cfg), 0o644))

	src, err := NewAgentSource(cfgPath, maxRounds)
	require.NoError(t, err)

	return src
}

func readTxs(t *testing.T, src *AgentSource, size int64) []transaction.Transaction {
	t.Helper()

	txs, err := src.ReadTxs(size)
	require.NoError(t, err)

	return txs
}

// Round 1 compiles to: alice.register, bob.register, bob.anchor, alice.pay(5),
// alice.anchor. Round 2 compiles to: alice.pay(7), bob.revoke, bob.anchor.
func TestAgentSourceServesRoundsInOrderWithContinuousNonces(t *testing.T) {
	src := newTestSource(t, 10, "")

	alice, bob := didAddr(t, "alice"), didAddr(t, "bob")
	idAddr, aAddr := identityAddr(t), auditAddr(t)

	first := readTxs(t, src, 5)
	require.Len(t, first, 5)

	require.Equal(t, alice, first[0].Sender) // register alice
	require.Equal(t, idAddr, first[0].Recipient)
	require.EqualValues(t, 0, first[0].Nonce)
	require.Len(t, first[0].Data, 68)

	require.Equal(t, bob, first[1].Sender) // register bob
	require.EqualValues(t, 0, first[1].Nonce)

	require.Equal(t, aAddr, first[2].Recipient) // audit anchor (batch of 2)
	require.Equal(t, bob, first[2].Sender)

	require.Equal(t, alice, first[3].Sender) // pay alice -> bob
	require.Equal(t, bob, first[3].Recipient)
	require.EqualValues(t, 5, first[3].Value.Int64())
	require.Empty(t, first[3].Data)
	require.EqualValues(t, 1, first[3].Nonce)

	require.Equal(t, aAddr, first[4].Recipient) // end-of-round anchor
	require.Equal(t, alice, first[4].Sender)
	require.EqualValues(t, 2, first[4].Nonce)

	second := readTxs(t, src, 5)
	require.Len(t, second, 3)

	require.Equal(t, alice, second[0].Sender) // pay across rounds keeps the nonce going
	require.Equal(t, bob, second[0].Recipient)
	require.EqualValues(t, 7, second[0].Value.Int64())
	require.Empty(t, second[0].Data)
	require.EqualValues(t, 3, second[0].Nonce)

	require.Equal(t, bob, second[1].Sender) // revoke
	require.Equal(t, idAddr, second[1].Recipient)
	require.EqualValues(t, 2, second[1].Nonce)
	require.Len(t, second[1].Data, 36)

	require.Equal(t, aAddr, second[2].Recipient)

	// Round file missing -> exhausted forever.
	require.Empty(t, readTxs(t, src, 5))
	require.Empty(t, readTxs(t, src, 5))
}

func TestAgentSourceServesInBatches(t *testing.T) {
	src := newTestSource(t, 10, "")

	var all []transaction.Transaction
	for range 5 {
		all = append(all, readTxs(t, src, 2)...)
	}

	require.Len(t, all, 8)
	require.Empty(t, readTxs(t, src, 2))
}

func TestAgentSourceSingleBatchServesAllRounds(t *testing.T) {
	src := newTestSource(t, 10, "")

	all := readTxs(t, src, 100)
	require.Len(t, all, 8) // round 1 + round 2 pipelined in one call
	require.Empty(t, readTxs(t, src, 100))
}

func TestAgentSourceStopsAtMaxRounds(t *testing.T) {
	src := newTestSource(t, 1, "")

	all := readTxs(t, src, 100)
	require.Len(t, all, 5) // round 2 never appended
	require.Empty(t, readTxs(t, src, 100))
	require.True(t, src.Exhausted()) // lets the supervisor's stop logic fire
}

func TestAgentSourceNotExhaustedWhileQueueHoldsTxs(t *testing.T) {
	src := newTestSource(t, 1, "")

	require.False(t, src.Exhausted()) // round 1 queued but not served

	require.Len(t, readTxs(t, src, 2), 2)
	require.False(t, src.Exhausted()) // queue still holds transactions

	require.Len(t, readTxs(t, src, 100), 3)
	require.True(t, src.Exhausted())
}

func TestAgentSourceBadRoundJSONReportsOnceThenStops(t *testing.T) {
	src := newTestSource(t, 10, "{broken json}\n")

	require.Len(t, readTxs(t, src, 2), 2) // round 1 tail still served

	_, err := src.ReadTxs(100)
	require.ErrorContains(t, err, "fetch round 2")

	// The source is terminal afterwards, so the supervisor stops cleanly.
	require.Empty(t, readTxs(t, src, 100))
}

func TestAgentSourceWritesResultsPerRound(t *testing.T) {
	src := newTestSource(t, 10, "")

	require.Len(t, readTxs(t, src, 100), 8)

	planRaw, err := os.ReadFile(filepath.Join(src.cfg.Base.ResultDir, agentemu.PlanFileName))
	require.NoError(t, err)
	require.Equal(t, 8, countLines(string(planRaw)))

	metaRaw, err := os.ReadFile(filepath.Join(src.cfg.Base.ResultDir, agentemu.RoundsMetaFile))
	require.NoError(t, err)

	var rounds []agentemu.RoundResult
	require.NoError(t, json.Unmarshal(metaRaw, &rounds))
	require.Len(t, rounds, 2)
	require.Equal(t, 1, rounds[0].Round)
	require.Equal(t, 2, rounds[1].Round)
}

func countLines(s string) int {
	n := 0
	for _, r := range s {
		if r == '\n' {
			n++
		}
	}

	return n
}

// newHTTPTestSource wires an AgentSource whose feedback goes to the given
// agent service handler: round 2+ come from HTTP instead of round files, and
// the confirmation wait is 1s (the chain CSV never exists in tests).
func newHTTPTestSource(t *testing.T, maxRounds int, agentHandler http.HandlerFunc) (*AgentSource, *httptest.Server) {
	t.Helper()

	srv := httptest.NewServer(agentHandler)
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "round_1.jsonl"), []byte(round1), 0o644))

	cfg := `base:
  result_dir: ` + filepath.Join(dir, "results") + `
experiment:
  seed: ` + fmt.Sprintf("%d", seed) + `
  trace: ` + filepath.Join(dir, "round_1.jsonl") + `
agent_api:
  mode: http
  endpoint: ` + srv.URL + `
  request_timeout_seconds: 2
  retries: 1
  wait_confirm_timeout_seconds: 1
  chain_results_csv: ` + filepath.Join(dir, "missing_chain.csv") + `
protocols:
  pay:
    plugin: direct-pay
  audit:
    plugin: merkle-audit
    contract_address: "` + auditContract + `"
    batch_size: 2
  identity:
    plugin: did-simple
    contract_address: "` + identityContract + `"
`
	cfgPath := filepath.Join(dir, "agentEmuConfig.yaml")
	require.NoError(t, os.WriteFile(cfgPath, []byte(cfg), 0o644))

	src, err := NewAgentSource(cfgPath, maxRounds)
	require.NoError(t, err)

	return src, srv
}

// drainAsync polls the async (http mode) source until the story ends,
// collecting everything it serves; the fetches happen on a background
// goroutine, so rounds appear across several ReadTxs calls.
func drainAsync(t *testing.T, src *AgentSource, timeout time.Duration) ([]transaction.Transaction, error) {
	t.Helper()

	deadline := time.Now().Add(timeout)

	var all []transaction.Transaction

	for time.Now().Before(deadline) {
		txs, err := src.ReadTxs(100)
		if err != nil {
			return all, err
		}

		all = append(all, txs...)

		if len(txs) == 0 {
			if src.Exhausted() {
				return all, nil
			}

			time.Sleep(20 * time.Millisecond)
		}
	}

	t.Fatal("drainAsync timed out")

	return all, nil
}

func TestAgentSourceHTTPFeedbackDrivesRounds(t *testing.T) {
	var mu sync.Mutex
	rounds := make([]int, 0, 2)

	src, _ := newHTTPTestSource(t, 5, func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Round  int `json:"round"`
			Agents []struct {
				AgentID string `json:"agent_id"`
				Active  bool   `json:"active"`
			} `json:"agents"`
		}
		require.NoError(t, json.NewDecoder(r.Body).Decode(&req))

		mu.Lock()
		rounds = append(rounds, req.Round)
		mu.Unlock()

		// Stop after driving one extra round.
		if req.Round >= 2 {
			_, _ = w.Write([]byte(`{"stop":true,"trace":[]}`))

			return
		}

		require.NotEmpty(t, req.Agents) // registry snapshot travels with the feedback
		_, _ = w.Write([]byte(`{"stop":false,"trace":[{"agent_id":"alice","action":"append_log","params_hash":"r2-log","ts":1}]}`))
	})

	all, err := drainAsync(t, src, 15*time.Second)
	require.NoError(t, err)
	require.Len(t, all, 6) // round 1 (5 txs) + one HTTP-decided round (1 tx)
	require.True(t, src.Exhausted())

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []int{1, 2}, rounds)
}

func TestAgentSourceHTTPErrorStopsTheStory(t *testing.T) {
	src, _ := newHTTPTestSource(t, 5, func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "agent is down", http.StatusInternalServerError)
	})

	all, err := drainAsync(t, src, 15*time.Second)
	require.ErrorContains(t, err, "fetch round 2")
	require.Len(t, all, 5) // round 1 still served before the failure
	require.True(t, src.Exhausted())
}
