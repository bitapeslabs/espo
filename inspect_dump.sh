#!/bin/bash
set -euo pipefail

DB="/data/.metashrew/mainnetnew884"
OUT="./dump.txt"

# ASCII "/alkanes/" and "/balances/" in hex (lowercase, no 0x)
HEX_ALKANES="2f616c6b616e65732f"
HEX_BALANCES="2f62616c616e6365732f"

# ---------------------------
# AlkaneId(2,16) and (2,61993)
# Two candidate encodings: VARINT64 and FIXED64 (little-endian)
# ---------------------------

# --- VARINT64 encoding we tried before ---
# block=2 -> 0a04 08 00 10 02
# tx=16   -> 12 04 08 00 10 10
WHAT_HEX_VAR="0a0408001002120408001010"

# block=2 -> 0a04 08 00 10 02
# tx=61993 (0xF229 varint = a9 e4 03) -> 12 06 08 00 10 a9 e4 03
WHO_HEX_VAR="0a04080010021206080010a9e403"

# --- FIXED64 encoding (wire-type 1) ---
# For Uint128 { hi: fixed64, lo: fixed64 } inside a nested message:
# field tags: hi -> 0x09, lo -> 0x11
# submessage length = 1+8 + 1+8 = 18 (0x12)
# block=2 => hi=0, lo=2 -> lo bytes (LE) = 02 00 00 00 00 00 00 00
# tx=16   => lo bytes (LE) = 10 00 00 00 00 00 00 00
# tx=61993=> 61993=0xF229 -> LE 29 f2 00 00 00 00 00 00

ZERO8="0000000000000000"
LO2_LE="0200000000000000"
LO16_LE="1000000000000000"
LO61993_LE="29f2000000000000"

BLOCK_FIXED="0a12""09${ZERO8}""11${LO2_LE}"
TX16_FIXED="1212""09${ZERO8}""11${LO16_LE}"
TX61993_FIXED="1212""09${ZERO8}""11${LO61993_LE}"

WHAT_HEX_FIX="${BLOCK_FIXED}${TX16_FIXED}"
WHO_HEX_FIX="${BLOCK_FIXED}${TX61993_FIXED}"

# Build keys/prefixes for both encodings (ldb expects 0x with --key_hex)
KEY_VAR="0x${HEX_ALKANES}${WHAT_HEX_VAR}${HEX_BALANCES}${WHO_HEX_VAR}"
PFX_VAR="0x${HEX_ALKANES}${WHAT_HEX_VAR}${HEX_BALANCES}"
TO_VAR="${PFX_VAR}ff"

KEY_FIX="0x${HEX_ALKANES}${WHAT_HEX_FIX}${HEX_BALANCES}${WHO_HEX_FIX}"
PFX_FIX="0x${HEX_ALKANES}${WHAT_HEX_FIX}${HEX_BALANCES}"
TO_FIX="${PFX_FIX}ff"

echo "== Trying EXACT GET (VARINT encoding) =="
echo "Key: $KEY_VAR"
ldb --db="$DB" get --key_hex "$KEY_VAR" --hex | tee "$OUT"
if grep -qi "key not found" "$OUT"; then
  echo
  echo "== Range scan (VARINT) =="
  echo "Range: [$PFX_VAR , $TO_VAR)"
  if ! ldb --db="$DB" scan --hex --from="$PFX_VAR" --to="$TO_VAR" | tee -a "$OUT" | grep -qi "${PFX_VAR#0x}"; then
    echo "== Fallback full scan + grep (VARINT) ==" | tee -a "$OUT"
    ldb --db="$DB" scan --hex | grep -i "${PFX_VAR#0x}" | tee -a "$OUT" || true
  fi
else
  echo "Found exact VARINT key (see $OUT)."
fi

echo
echo "== Trying EXACT GET (FIXED64 encoding) =="
echo "Key: $KEY_FIX"
ldb --db="$DB" get --key_hex "$KEY_FIX" --hex | tee -a "$OUT"
if grep -qi "key not found" "$OUT"; then
  echo
  echo "== Range scan (FIXED64) =="
  echo "Range: [$PFX_FIX , $TO_FIX)"
  if ! ldb --db="$DB" scan --hex --from="$PFX_FIX" --to="$TO_FIX" | tee -a "$OUT" | grep -qi "${PFX_FIX#0x}"; then
    echo "== Fallback full scan + grep (FIXED64) ==" | tee -a "$OUT"
    ldb --db="$DB" scan --hex | grep -i "${PFX_FIX#0x}" | tee -a "$OUT" || true
  fi
else
  echo "Found exact FIXED64 key (see $OUT)."
fi

echo
echo "Done. Full results -> $OUT"
