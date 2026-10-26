"""Shared column-name contract between the CronJob output and the selector.

The CronJob emits scores under one set of column names; IndexerSelector reads
them under another, with a rename step bridging the two. Naming both ends here
means a column rename is one edit, not a silent mismatch split across modules.
"""

# Columns as emitted by the CronJob — the pushed payload / wire format.
INDEXER = "indexer"
COMPUTED_AT = "computed_at"
LAT_NORMALIZED_SCORE = "lat_normalized_score"
LAT_COEFFICIENT_UPPER_BOUND = "lat_coefficient_upper_bound"
UPTIME_SCORE = "uptime_score"
SUCCESS_RATE = "success_rate"
STAKE_TO_FEES = "stake_to_fees"
DST_LAT = "dst_lat"
DST_LON = "dst_lon"
ORG = "org"

# Columns as consumed by IndexerSelector after the transform.
SEL_LATENCY_CI = "Latency Coefficient + Error Confidence Interval"
SEL_UPTIME_PERCENT = "% up_x"
SEL_AVERAGE_STATUS = "average_status"
SEL_DESTINATION_LOC = "destination_loc"
NORM_LAT_LIN_REG_COEFFICIENT = "norm_lat_lin_reg_coefficient"

# Direct CronJob -> selector renames with no value change. uptime (x100) and
# location (lat,lon join) need arithmetic, so the transform handles those.
CRONJOB_TO_SELECTOR_RENAME = {
    LAT_COEFFICIENT_UPPER_BOUND: SEL_LATENCY_CI,
    SUCCESS_RATE: SEL_AVERAGE_STATUS,
    LAT_NORMALIZED_SCORE: NORM_LAT_LIN_REG_COEFFICIENT,
}
