#!/usr/bin/env bash
# Runs pip-audit with the given arguments, retrying when PyPI itself is unavailable.
# A run that finds vulnerabilities, or fails for any other reason, stops straight away.
set -uo pipefail

attempts=3
delay_seconds=30
output=$(mktemp)

for attempt in $(seq 1 "$attempts"); do
  pip-audit "$@" 2>&1 | tee "$output"
  status=${PIPESTATUS[0]}
  if [ "$status" -eq 0 ]; then
    exit 0
  fi
  # pip-audit reports PyPI outages, timeouts and rate limits as a ServiceError or ConnectionError
  if ! grep -qE "ServiceError|ConnectionError" "$output"; then
    exit "$status"
  fi
  if [ "$attempt" -lt "$attempts" ]; then
    echo "::warning::pip-audit could not reach PyPI (attempt $attempt of $attempts), retrying in ${delay_seconds}s"
    sleep "$delay_seconds"
  fi
done

echo "::error::pip-audit could not reach PyPI after $attempts attempts"
exit "$status"
