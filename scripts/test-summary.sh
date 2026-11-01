#!/usr/bin/env bash
# Summarises `go test -json` output.
#
#   scripts/test-summary.sh <title> <report.json>
#
# Prints one line per package and the output of every failed test to stdout.
# When GITHUB_STEP_SUMMARY is set (GitHub Actions), also writes a Markdown
# table to the job summary. Always exits 0: the go test exit code decides
# whether the job fails.
set -euo pipefail

title=$1
report=$2

if [ ! -s "$report" ]; then
  echo "no test output in $report" >&2
  exit 0
fi

# Drop lines that are not JSON (for example build errors printed by go vet).
events=$(jq -cR 'fromjson? // empty' "$report")

packages=$(jq -rs '
  def cov: [ .[] | select(.Test == null and .Action == "output") | .Output
            | (capture("coverage: (?<c>[0-9.]+)% of statements") | .c)? ] | last // "-";
  group_by(.Package)[]
  | . as $ev
  | ($ev | map(select(.Test == null and (.Action == "pass" or .Action == "fail" or .Action == "skip"))) | last) as $end
  | select($end != null)
  | ($ev | map(select(.Test != null and (.Action == "pass" or .Action == "fail" or .Action == "skip")))) as $tests
  # Leave out packages without tests, unless they failed (for example to build).
  | select(($tests | length) > 0 or $end.Action == "fail")
  | [ ($ev[0].Package | sub("^github.com/anicoll/screamer/?"; "") | if . == "" then "." else . end),
      $end.Action,
      ($tests | map(select(.Action == "pass")) | length),
      ($tests | map(select(.Action == "fail")) | length),
      ($tests | map(select(.Action == "skip")) | length),
      (($end.Elapsed // 0) * 10 | round / 10),
      ($ev | cov) ]
  | @tsv' <<<"$events")

failures=$(jq -rs '
  group_by([.Package, .Test])[]
  | select(.[0].Test != null and any(.[]; .Action == "fail"))
  | "--- FAIL: \(.[0].Package | sub("^github.com/anicoll/screamer/?"; "")) \(.[0].Test)\n"
    + (map(select(.Action == "output") | .Output) | join("") | .[-20000:])' <<<"$events")

icon() { case $1 in pass) echo "✅" ;; fail) echo "❌" ;; *) echo "⏭️" ;; esac; }

echo "== $title"
printf '%-24s %-5s %6s %6s %6s %8s %8s\n' package result pass fail skip time cover
while IFS=$'\t' read -r pkg result pass fail skip elapsed cov; do
  [ -z "$pkg" ] && continue
  [ "$cov" != "-" ] && cov="$cov%"
  printf '%-24s %-5s %6s %6s %6s %7ss %8s\n' "$pkg" "$result" "$pass" "$fail" "$skip" "$elapsed" "$cov"
done <<<"$packages"
if [ -n "$failures" ]; then
  echo
  echo "$failures"
fi

[ -z "${GITHUB_STEP_SUMMARY:-}" ] && exit 0

{
  totals=$(awk -F'\t' '{p+=$3; f+=$4; s+=$5} END {printf "%d passed, %d failed, %d skipped", p, f, s}' <<<"$packages")
  echo "### $title"
  echo
  echo "$totals"
  echo
  echo "| Package | Result | Passed | Failed | Skipped | Time | Coverage |"
  echo "| --- | :---: | ---: | ---: | ---: | ---: | ---: |"
  while IFS=$'\t' read -r pkg result pass fail skip elapsed cov; do
    [ -z "$pkg" ] && continue
    [ "$cov" != "-" ] && cov="$cov%"
    echo "| \`$pkg\` | $(icon "$result") | $pass | $fail | $skip | ${elapsed}s | $cov |"
  done <<<"$packages"
  if [ -n "$failures" ]; then
    echo
    echo "<details><summary>Failed tests</summary>"
    echo
    echo '```'
    echo "$failures" | head -c 60000
    echo '```'
    echo "</details>"
  fi
  echo
} >>"$GITHUB_STEP_SUMMARY"
