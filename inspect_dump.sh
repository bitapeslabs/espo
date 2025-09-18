NEEDLE_ASC='ammdata:fc1:2:10b59:1h:'

# Convert to lowercase hex (no spaces/newlines)
NEEDLE_HEX=$(echo -n "$NEEDLE_ASC" | xxd -p -c 999 | tr 'A-F' 'a-f')

# Filter lines whose **key** contains that hex prefix (left side of ==>)
awk -v n="$NEEDLE_HEX" -F '==>' 'tolower($1) ~ ("0x" n)' dump.txt > pool_2_10b59_1h.hex