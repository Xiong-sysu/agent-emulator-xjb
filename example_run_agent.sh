#!/bin/bash

# AgentEmulator MVP run: 1 shard x 4 consensus nodes + supervisor.
# The supervisor compiles the agent story (traces/rounds/round_*.jsonl) into
# transactions via tx_source=agent_source and replays them on chain.
# Results land in ./exp/agentemu-results (plan, events, registry) and
# ./exp/results (chain measurements).

SHARD_NUM=1
NODE_NUM=4

# Delete the old experiment directory.
rm -rf ./exp/
mkdir -p ./exp/

set -ex

# Download modules and pre-compile.
go mod download
go build ./...

# Start the consensus nodes of shard 0 (the ip_table.json entries of the other
# shards are simply not used).
for ((j=0; j<NODE_NUM; j++)); do
  go run cmd/consensusnode/main.go -shard_id=0 -node_id="${j}" -config=config_agent.yaml &
done

# Start the supervisor.
go run cmd/supervisor/main.go -shard_id=0x7fffffff -node_id=0 -config=config_agent.yaml &

wait
