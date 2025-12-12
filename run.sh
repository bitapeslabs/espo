cargo run --release -- \
  --readonly-metashrew-db-dir /data/.metashrew/v9/.metashrew-v9 \
  --port 5778 \
  --electrum-rpc-url 0.0.0.0:50015 \
  --bitcoind-rpc-url http://127.0.0.1:8332 \
  --bitcoind-rpc-user admin \
  --bitcoind-rpc-pass admin \
  --bitcoind-blocks-dir "$HOME/.bitcoin/blocks" \
  --network mainnet \
  --metashrew-version v9 



