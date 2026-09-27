#!/bin/bash
# Rebuild a report from its results and compare it byte for byte with the published HTML.
# usage: test_report.sh RESULTS_DIR EXPECTED.html
set -eu
R=${1:?results dir}; X=${2:?expected html}
out=$(mktemp -t gold-report.XXXXXX)
trap 'rm -f "$out"' EXIT
python3 "$(dirname "$0")/build_report.py" "$R" -o "$out" >/dev/null
cmp "$out" "$X" && echo "OK: identical to $X"
# A results dir with a row missing must fail and name the row.
tmp=$(mktemp -d -t gold-missing.XXXXXX); trap 'rm -f "$out"; rm -rf "$tmp"' EXIT
cp "$R"/*.json "$tmp"/; rm "$tmp/L7.json"
if python3 "$(dirname "$0")/build_report.py" "$tmp" -o "$tmp/x.html" 2>"$tmp/err"; then echo "FAIL: built without L7"; exit 1; fi
grep -q "L7" "$tmp/err" && echo "OK: missing row reported: $(cat "$tmp/err")"
