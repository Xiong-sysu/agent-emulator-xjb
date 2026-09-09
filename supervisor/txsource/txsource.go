package txsource

import (
	"fmt"

	"github.com/HuangLab-SYSU/block-emulator-x/config"
	"github.com/HuangLab-SYSU/block-emulator-x/pkg/core/transaction"
	"github.com/HuangLab-SYSU/block-emulator-x/supervisor/txsource/agentsource"
	"github.com/HuangLab-SYSU/block-emulator-x/supervisor/txsource/csvsource"
	"github.com/HuangLab-SYSU/block-emulator-x/supervisor/txsource/randomsource"
)

// TxSource provides a transaction source for the supervisor (as the client / wallet).
// Transactions will be read from TxSource and sent to the consensus nodes periodically.
type TxSource interface {
	// ReadTxs reads transactions from the TxSource. If the source is exhausted, it returns (nil, nil).
	ReadTxs(size int64) ([]transaction.Transaction, error)
}

// Exhaustible is an optional TxSource capability: the source knows that no
// further transactions will ever be served (e.g. agent_source after its last
// round). Committees treat an exhausted source like a fully spent tx_number,
// which lets the regular empty-block stop logic fire even when the configured
// tx_number is larger than the actual transaction count. Sources without this
// capability keep the legacy behavior.
type Exhaustible interface {
	TxSource
	Exhausted() bool
}

type NoOperationTxSource struct{}

func (NoOperationTxSource) ReadTxs(int64) ([]transaction.Transaction, error) {
	return nil, nil
}

// NewTxSource creates a TxSource by the given config.
func NewTxSource(cfg config.TxSourceCfg) (TxSource, error) {
	var ts TxSource

	switch cfg.TxSourceType {
	case csvsource.Key:
		cs, err := csvsource.NewCSVSource(cfg.TxSourceFile, cfg.ExcludeContractTxs)
		if err != nil {
			return nil, fmt.Errorf("failed to create CSV source: %w", err)
		}

		ts = cs
	case randomsource.Key:
		ts = randomsource.NewRandomSource()
	case agentsource.Key:
		as, err := agentsource.NewAgentSource(cfg.AgentEmuConfig, cfg.AgentRounds)
		if err != nil {
			return nil, fmt.Errorf("failed to create agent source: %w", err)
		}

		ts = as
	default:
		ts = NoOperationTxSource{}
	}

	return ts, nil
}
