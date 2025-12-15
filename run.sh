cargo run --release -- \
  --readonly-metashrew-db-dir /data/.metashrew/v9/.metashrew-v9 \
  --port 5778 \
  --electrs-esplora-url http://127.0.0.1:4332 \
  --bitcoind-rpc-url http://127.0.0.1:8332 \
  --bitcoind-rpc-user admin \
  --bitcoind-rpc-pass admin \
  --network mainnet \
  --block-source-mode rpc
