#!/usr/bin/env bash
set -euo pipefail

# ---- usage ----
if [[ $# -lt 3 ]]; then
  echo "Usage: $0 <DB_PATH> <POOL_BLOCK_DEC> <POOL_TX_DEC>"
  echo "Example: $0 ./db/espo 2 68441"
  exit 1
fi

DB="$1"
PBLK="$2"
PTX="$3"

if ! command -v ldb >/dev/null 2>&1; then
  echo "[!] 'ldb' not found. Install RocksDB tools (often package 'rocksdb-tools')."
  exit 2
fi

TS="$(date +%s)"
OUT="rocksprobe_${PBLK}_${PTX}_${TS}"
mkdir -p "$OUT"

log() { echo "[i] $*"; }

# ---- helpers ----
asc2hex() { echo -n "$1" | xxd -p -c 999 | tr -d '\n' | tr 'a-f' 'A-F'; }
range_to() { echo -n "${1}FF"; }   # append 0xFF sentinel to build open range

scan_range() {
  local title="$1"; local from_hex="$2"; local to_hex="$3"; local out="$4"
  log "$title"
  echo "[from]=0x${from_hex}"   > "$out"
  echo "[to  ]=0x${to_hex}"    >> "$out"
  ldb --db="$DB" scan --hex --from="0x${from_hex}" --to="0x${to_hex}" >> "$out" 2>>"$OUT/_errors.log" || true
  local cnt
  cnt=$(grep -c '==>' "$out" || true)
  log "  -> ${cnt} entries  (file: $out)"
}

scan_prefix() {
  local title="$1"; local prefix_ascii="$2"; local out="$3"
  local phex; phex="$(asc2hex "$prefix_ascii")"
  local tohex; tohex="$(range_to "$phex")"
  scan_range "$title [$prefix_ascii]" "$phex" "$tohex" "$out"
}

# ---- namespaces (RELATIVE vs FULL) ----
# Reader/writer now expect RELATIVE ("trades:*"), with DB layer adding "ammdata:" once.
BASE_NS="ammdata:"
TR_REL="trades:v1:${PBLK}:${PTX}:"
IDX_REL="trades:idx:v1:${PBLK}:${PTX}:"

TR_FULL="${BASE_NS}${TR_REL}"
IDX_FULL="${BASE_NS}${IDX_REL}"

# Also probe for the accidental double prefix form:
TR_DBL="${BASE_NS}${BASE_NS}${TR_REL}"
IDX_DBL="${BASE_NS}${BASE_NS}${IDX_REL}"

log "DB=$DB"
log "Pool (dec): ${PBLK}:${PTX}"
log "Output dir : ${OUT}"
echo

# ---- global head sample (first 200 keys) ----
log "[probe] first 200 keys (global) → ${OUT}/db_head.raw.txt"
ldb --db="$DB" scan --hex --max_keys=200 > "${OUT}/db_head.raw.txt" 2>>"$OUT/_errors.log" || true
echo

# ---- primaries ----
scan_prefix "[primary FULL]"    "${TR_FULL}" "${OUT}/primary_full.raw.txt"
scan_prefix "[primary REL]"     "${TR_REL}"  "${OUT}/primary_rel.raw.txt"
scan_prefix "[primary DOUBLE]"  "${TR_DBL}"  "${OUT}/primary_double.raw.txt"

# ---- secondaries: all flavors (FULL / REL / DOUBLE) ----
for flavor in ts absb absq sb_absb sq_absq sb_ts sq_ts; do
  scan_prefix "[idx ${flavor} FULL]"   "${IDX_FULL}${flavor}:"   "${OUT}/idx_${flavor}_full.raw.txt"
  scan_prefix "[idx ${flavor} REL]"    "${IDX_REL}${flavor}:"    "${OUT}/idx_${flavor}_rel.raw.txt"
  scan_prefix "[idx ${flavor} DOUBLE]" "${IDX_DBL}${flavor}:"    "${OUT}/idx_${flavor}_double.raw.txt"
done

# ---- __count keys (FULL / REL / DOUBLE) ----
COUNT_REL="${IDX_REL}__count"
COUNT_FULL="${IDX_FULL}__count"
COUNT_DBL="${IDX_DBL}__count"

scan_prefix "[count FULL]"   "${COUNT_FULL}"   "${OUT}/count_full.raw.txt"
scan_prefix "[count REL]"    "${COUNT_REL}"    "${OUT}/count_rel.raw.txt"
scan_prefix "[count DOUBLE]" "${COUNT_DBL}"    "${OUT}/count_double.raw.txt"

# ---- simple summary ----
summ() {
  local label="$1"; local path="$2"
  local n=0
  [[ -f "$path" ]] && n=$(grep -c '==>' "$path" || true)
  printf "  %-28s %6d  %s\n" "$label" "$n" "$path"
}

echo
echo "[summary] entries per dump:"
summ "primary_full"        "${OUT}/primary_full.raw.txt"
summ "primary_rel"         "${OUT}/primary_rel.raw.txt"
summ "primary_double"      "${OUT}/primary_double.raw.txt"

for flavor in ts absb absq sb_absb sq_absq sb_ts sq_ts; do
  summ "idx_${flavor}_full"   "${OUT}/idx_${flavor}_full.raw.txt"
  summ "idx_${flavor}_rel"    "${OUT}/idx_${flavor}_rel.raw.txt"
  summ "idx_${flavor}_double" "${OUT}/idx_${flavor}_double.raw.txt"
done

summ "count_full"          "${OUT}/count_full.raw.txt"
summ "count_rel"           "${OUT}/count_rel.raw.txt"
summ "count_double"        "${OUT}/count_double.raw.txt"

echo
log "Done. Attach these files if anything is zero where it shouldn't be:"
echo "  - ${OUT}/primary_full.raw.txt"
echo "  - ${OUT}/idx_ts_full.raw.txt (and one of absb/absq)"
echo "  - ${OUT}/count_full.raw.txt"
echo "Also include ${OUT}/db_head.raw.txt (first 200 keys)."
