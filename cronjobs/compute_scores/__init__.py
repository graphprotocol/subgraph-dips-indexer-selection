"""
CronJob for computing indexer scores.

This job runs daily and computes:
- Latency linear regression coefficients and scores
- Uptime scores
- Success rates
- Stake-to-fees ratios
- A pre-normalized latency score (the IISA service scales the other metrics itself)

Results are POSTed to the IISA HTTP service, which persists them to its own cache
file (no shared filesystem).
"""
