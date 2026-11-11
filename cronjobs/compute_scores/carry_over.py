"""When each run measured quality, so a later run can tell how old a measurement is.

Latency, uptime and success are measured from the replay of gateway queries. The scores carry
the time of that measurement to iisa beside them, the way they carry the time stake was read.
"""

# When the quality scores in a row were measured from the replay. Missing in the fallback
# scores, which measure nothing and give every indexer the same.
QUALITY_MEASURED_AT = "quality_measured_at"
