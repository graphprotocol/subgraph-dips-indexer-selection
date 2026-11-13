# How an indexer is scored

When IISA picks indexers for a deployment, it gives every candidate a total score between 0 and 1 and prefers the higher totals. This page sets out how that total is worked out, for anyone who would rather check the maths than read the code. Tests check the page against the code: the weights in the equations for quality and value, including value with a part left out, every row of the constants table in the last section, the latency table and the worked example's table. The prose is not checked, so a figure written only in a sentence, such as the 0.85 in the cut-offs section or the sums written out under the worked example, could fall out of date unnoticed.

## The formula

The total is the product of 4 factors, each between 0 and 1:

$$
\text{total} = U \times F \times Q \times V
$$

$U$ is uptime, $F$ is freshness, $Q$ is quality and $V$ is value. Quality and value are each a weighted average of smaller parts:

$$
Q = 0.2 \, S + 0.8 \, L
$$

$$
V = 0.45 \, K + 0.45 \, P + 0.10 \, E
$$

$S$ is success rate and $L$ is latency. $K$ is economic security (stake-to-fees), $P$ is the price per 30 days and $E$ is the price per billion entities.

Multiplying rather than adding is what makes an indexer earn its place on every front at once. An indexer that is down, or serving stale data, scores 0 however cheap or fast it is, because a 0 on uptime or freshness makes the whole product 0. Inside quality and value the parts are added, so a weak part only pulls that factor down: an indexer with a latency score of 0 still keeps 20% of its quality from its success rate. No factor is raised to a further power. Products run lower than sums, so a good indexer totals about 0.5 rather than about 0.9.

## Uptime, U

Uptime is the share of the observed window in which the indexer was available, raised to the 6th power:

$$
U = u^{6}
$$

The power spreads out indexers that all look good: 99% uptime scores about 0.94, 95% about 0.74, 90% about 0.53 and 80% about 0.26. An indexer's score depends only on its own uptime, never on the other candidates'.

Uptime is measured on each chain the indexer serves. On a chain where it answered fewer than 10,000 queries in the window, its value there is blended with its average over all its chains: with $n$ queries on the chain, the chain's own value counts with weight $n / (n + 250)$ and the average makes up the rest. Selection uses the request chain's value where there is one, and the overall average otherwise. An indexer with no uptime data scores 0, since indexers have to prove themselves.

## Freshness, F

Freshness is the share of the last 28 days of freshness checks the indexer passed on the request's chain, taken over at least 10,080 checks (7 days of minutes, so a newcomer can't score 1 on a good hour), raised to the 6th power:

$$
F = \left( \frac{\text{passed}}{\max(\text{counted},\ 10{,}080)} \right)^{6}
$$

On a chain with at least 7 days of checks, an indexer with no record there scores 0. Freshness is left out, which counts as $F = 1$, wherever it can't be trusted to tell indexers apart: on a chain whose checks cover less than 7 days or that has none, on a chain exempt from freshness, on a chain where no check has passed or failed within the age limit (as when the prober stops or the gateway is down), and in the network-wide scores, which don't belong to any chain.

## Quality, Q

Success rate is scored like uptime, as the share of the indexer's queries that returned a usable result, raised to the 6th power and blended across chains the same way: $S = s^{6}$.

Latency is scored on a fixed curve of the indexer's latency in milliseconds:

$$
L = \max\left(0.001,\ \frac{1 + e^{(0 - 250)/80}}{1 + e^{(\text{ms} - 250)/80}}\right)
$$

The curve has the shape of the gateway's own latency score, made steeper: it is 1 at 0 ms, falls fastest around its midpoint of 250 ms, and its width of 80 ms sets how quickly. A measured latency never scores below the floor of 0.001.

| Latency | Score |
| --- | --- |
| 0 ms | 1.000 |
| 50 ms | 0.965 |
| 120 ms | 0.872 |
| 250 ms | 0.522 |
| 400 ms | 0.139 |
| 500 ms | 0.044 |
| 1,000 ms | 0.001 |

The daily score job works out each indexer's latency from real gateway traffic. Its regression gives each indexer's gap to the median indexer, which accounts for how far away the indexer is and for the queries it was sent, plus a margin of 1.5 standard errors so that an estimate backed by little data looks slower. Adding the median indexer's own latency to that gap gives the indexer's latency in milliseconds, never below 0. The median indexer's latency is the median response time once each query's own indexer gap is taken out, since the median query alone would lean towards the fast indexers the gateway sends most queries to. An indexer without a latency scores 0. Scores with no latency in milliseconds at all, as from a score job run without GeoIP, a degraded run or a job older than the measure, keep the job's own latency score, scaled across every indexer the job measured, and the service logs a warning while they do.

## Value, V

$K$, economic security, places each indexer's slashable stake divided by its query fees on a straight line between the lowest and the highest among the indexers the request can keep or pick. An indexer that has earned no fees scores 1.

$P$, the price per 30 days on the request's chain, scores $1 - \text{price} / \text{ceiling}$, where the ceiling is the most the payer will pay, `max_grt_per_30_days` in the request. Without a ceiling the highest price among the candidates takes its place.

$E$, the price per billion entities, scores the same way against the payer's per-entity ceiling, `max_grt_per_billion_entities_per_30_days`, held between 0 and 1, so a price at the ceiling scores 0. Without a ceiling the highest per-entity price among the candidates takes its place.

The network-wide scores belong to no chain and leave both prices out, so there value is $K$ alone.

## When a part is missing

Within quality and value, a part that is left out drops out and the others are rescaled to make up its weight. Value without $E$, for example, is

$$
V = \frac{0.45 \, K + 0.45 \, P}{0.90}
$$

That only happens where a part is left out on purpose, such as the prices in the network-wide scores. Missing data is never given a neutral value: an indexer with no uptime, no success rate or no latency scores 0 on it. Freshness is the one exception, as above, since leaving it out is how a chain whose checks can't be trusted ignores it.

## Selection's cut-offs

Selection keeps a working group stable unless something is clearly better, through 3 cut-offs. Each is a fraction of $B$, the best total among the candidates the request can pick from at that moment, because fixed numbers would bite much harder on products than they did on sums.

A group member can be replaced only when its total is under $0.15 \, B$, and only by a candidate whose total beats it by more than $0.50 \, B$. Once the group has an indexer that says it has synced the deployment, another candidate that says so keeps its head start over the rest only while its total is at least $0.60 \, B$; below that it competes on its total alone.

Since $B$ is the best candidate's own total, the best candidate always beats a member under $0.15 \, B$ by more than $0.85 \, B$. The margin therefore matters only when a lower candidate is tried first, as a synced one is. It also means a synced indexer that gets into a group on its claim alone is swapped out in the same selection if its total is under $0.15 \, B$.

On a chain that enforces freshness, a candidate marked stale is passed over while a fresh one is left. Once none is, the stale candidates with the best totals fill the remaining places, and for those picks $B$ is the best stale candidate's total, since they are what the pick can choose from. A stale candidate never replaces a member, so a replacement's $B$ is always the best fresh candidate's total, and with no fresh candidate left nothing is replaced.

## A worked example

2 indexers on the same chain, with the same success rate and latency:

| | Indexer A | Indexer B |
| --- | --- | --- |
| Uptime | 99.5% | 95% |
| Freshness checks passed | 99% | 90% |
| Success rate | 98% | 98% |
| Latency | 120 ms | 120 ms |
| K, economic security | 0.6 | 0.6 |
| P, price per 30 days | 0.6 | 0.9 |
| E, price per billion entities | 0.7 | 0.9 |
| U, uptime | 0.970 | 0.735 |
| F, freshness | 0.941 | 0.531 |
| Q, quality | 0.875 | 0.875 |
| V, value | 0.610 | 0.765 |
| Total | 0.488 | 0.261 |

Both have $S = 0.98^{6} = 0.886$ and $L = 0.872$, so $Q = 0.2 \times 0.886 + 0.8 \times 0.872 = 0.875$. B is the cheaper of the 2, which lifts its value to 0.765 against A's 0.610, but its 95% uptime and 90% freshness cost it far more than that: A totals $0.970 \times 0.941 \times 0.875 \times 0.610 = 0.488$ and B $0.735 \times 0.531 \times 0.875 \times 0.765 = 0.261$.

## Constants

| Constant | Code | Value |
| --- | --- | --- |
| Power uptime, success rate and freshness are raised to | `iisa.share_score.SHARE_EXPONENT` | 6 |
| Days of freshness checks kept | `iisa.freshness.HISTORY_DAYS` | 28 |
| Fewest checks a freshness share is taken over | `iisa.freshness.MIN_SCORED_MINUTES` | 10,080 |
| Queries on a chain at which its own uptime and success rate count in full | `chain_quality.FULL_WEIGHT_QUERIES` | 10,000 |
| Queries in the blend weight n / (n + 250) | `chain_quality.PRIOR_QUERIES` | 250 |
| Standard errors added to a latency gap | `processing.LATENCY_COEFFICIENT_STANDARD_ERROR_MULTIPLIER` | 1.5 |
| Latency curve's midpoint, in ms | `iisa.indexer_selection.LATENCY_MIDPOINT_MS` | 250 |
| Latency curve's width, in ms | `iisa.indexer_selection.LATENCY_WIDTH_MS` | 80 |
| Lowest score a measured latency gets | `iisa.indexer_selection.LATENCY_FLOOR_SCORE` | 0.001 |
| Weight of success rate in quality | `iisa.indexer_selection.QUALITY_SUCCESS_RATE_WEIGHT` | 0.2 |
| Weight of latency in quality | `iisa.indexer_selection.QUALITY_LATENCY_WEIGHT` | 0.8 |
| Weight of economic security in value | `iisa.indexer_selection.VALUE_STAKE_TO_FEES_WEIGHT` | 0.45 |
| Weight of the price per 30 days in value | `iisa.indexer_selection.VALUE_BASE_PRICE_WEIGHT` | 0.45 |
| Weight of the price per billion entities in value | `iisa.indexer_selection.VALUE_ENTITY_PRICE_WEIGHT` | 0.10 |
| Fraction of B under which a member can be replaced | `iisa.indexer_selection.MIN_INDEXER_SCORE` | 0.15 |
| Fraction of B a replacement must beat a member by | `iisa.indexer_selection.REPLACEMENT_MARGIN` | 0.50 |
| Fraction of B a synced candidate needs to keep its head start | `iisa.indexer_selection.MIN_SYNCED_THRESHOLD` | 0.60 |
