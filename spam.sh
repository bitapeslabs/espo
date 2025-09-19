#!/usr/bin/env bash
# rpc_spam.sh
# Send requests at a fixed RPS for a given duration, log each request time_total (seconds),
# and print summary stats when finished.
#
# Usage:
#   ./rpc_spam.sh                # use defaults
#   RPS=100 DURATION=10 PAYLOAD=file.json URL=http://127.0.0.1:5778/rpc ./rpc_spam.sh
#
# Default configurable constants:
RPS=${RPS:-100000}               # requests per second
DURATION=${DURATION:-10}     # test duration in seconds
URL=${URL:-http://127.0.0.1:5778/rpc}
# If PAYLOAD is a filename it will be used; if it's empty we use the inline default below
PAYLOAD=${PAYLOAD:-}

# Default JSON payload (if PAYLOAD file is not provided)
read -r -d '' DEFAULT_JSON <<'EOF'
{
  "jsonrpc": "2.0",
  "method": "ammdata.get_candles",
  "params": {
    "pool": "2:56801",
    "timeframe": "10m",
    "page": 1,
    "limit": 1000,
    "side": "quote"
  },
  "id": 1
}
EOF

TMPDIR=$(mktemp -d /tmp/rpc_spam.XXXX)
TIMES_FILE="$TMPDIR/times.txt"
LOG_FILE="$TMPDIR/run.log"

trap 'echo; echo "Interrupted. Waiting for outstanding requests..."; wait; summarize; cleanup; exit' INT TERM

# compute interval between request launches (in seconds, decimal)
if [ "$RPS" -le 0 ]; then
  echo "RPS must be > 0" >&2
  exit 1
fi
INTERVAL=$(awk -v r="$RPS" 'BEGIN { printf("%.6f", 1.0 / r) }')
TOTAL_REQS=$(( RPS * DURATION ))

echo "[rpc_spam] RPS=$RPS, DURATION=${DURATION}s, TOTAL_REQS=$TOTAL_REQS, URL=$URL"
echo "[rpc_spam] writing logs to $TMPDIR"
echo "[rpc_spam] interval between starts = ${INTERVAL}s"

# prepare payload
if [ -n "$PAYLOAD" ] && [ -f "$PAYLOAD" ]; then
  PAYLOAD_DATA=$(cat "$PAYLOAD")
else
  PAYLOAD_DATA="$DEFAULT_JSON"
fi

# function to compute and print summary
summarize() {
  if [ ! -s "$TIMES_FILE" ]; then
    echo "[rpc_spam] no timings recorded"
    return
  fi

  # times file contains one time_total per line (seconds as float)
  awk '
  BEGIN { min = 1e99; max = 0; sum = 0; n = 0; }
  { t = $1; n++; sum += t; if (t < min) min = t; if (t > max) max = t; a[n]=t; }
  END {
    if (n==0) { print "no samples"; exit }
    avg = sum / n;
    # percentiles
    PROCINFO["sorted_in"] = "@ind_num_asc";
    asort(a);
    p50 = a[int(n*0.50 + 0.5)];
    p90 = a[int(n*0.90 + 0.5)];
    printf("[rpc_spam] samples=%d\n", n);
    printf("[rpc_spam] avg = %.3f ms\n", avg*1000);
    printf("[rpc_spam] min = %.3f ms\n", min*1000);
    printf("[rpc_spam] max = %.3f ms\n", max*1000);
    printf("[rpc_spam] p50 = %.3f ms\n", p50*1000);
    printf("[rpc_spam] p90 = %.3f ms\n", p90*1000);
  }' "$TIMES_FILE"
}

cleanup() {
  echo "[rpc_spam] logs at $TMPDIR"
  # keep logs for inspection (don't auto-delete)
  # rm -rf "$TMPDIR"
}

# spawn loop: launch TOTAL_REQS curls, spaced by INTERVAL seconds.
i=0
start_epoch=$(date +%s.%N)
while [ $i -lt $TOTAL_REQS ]; do
  i=$((i+1))
  # launch curl in background; output time_total to times file (one line)
  (
    # measure and print only the time_total to stdout
    curl -s -o /dev/null -w "%{time_total}\n" \
      -X POST "$URL" \
      -H "Content-Type: application/json" \
      -d "$PAYLOAD_DATA"
  ) >> "$TIMES_FILE" 2>>"$LOG_FILE" &

  # sleep the interval (allow fractions); use usleep if available for slightly better precision
  if command -v usleep >/dev/null 2>&1; then
    # usleep expects microseconds
    usleep $(awk -v s="$INTERVAL" 'BEGIN { printf("%d", s*1000000) }')
  else
    sleep "$INTERVAL"
  fi
done

echo "[rpc_spam] launched $TOTAL_REQS requests, waiting for completion..."
wait

end_epoch=$(date +%s.%N)
elapsed=$(awk -v a="$start_epoch" -v b="$end_epoch" 'BEGIN{printf("%.6f", b-a)}')

echo "[rpc_spam] wall time: ${elapsed}s"
summarize
echo "[rpc_spam] full per-request timings: $TIMES_FILE"
echo "[rpc_spam] raw curl stderr/logs: $LOG_FILE"
# do not delete logs — user said they want them
