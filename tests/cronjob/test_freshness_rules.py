"""Tests for the freshness prober's pure rules: classifying a reply, eligibility, the general-fault
rule and which pairs are checked."""

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "cronjobs" / "compute_scores"))

from freshness_rules import (  # noqa: E402
    FAIL,
    NO_EVIDENCE,
    PASS,
    VOID,
    Allocation,
    CheckResult,
    GatewayReply,
    GatewayUnreachable,
    apply_general_fault,
    dips_pairs,
    eligible_deployments,
    first_allocation_lookups,
    format_minute,
    is_general_fault,
    judge,
    minute_payload,
    supported_chains,
)

NOW = 1_794_345_636.0
BAR = 36.0
INDEXER = "0x" + "a" * 40
DAY = 86_400
WEEK = 7 * DAY


def _answer(block, *, errors=None):
    body = {"data": {"_meta": {"block": block}}}
    if errors is not None:
        body["errors"] = errors
    return json.dumps(body).encode()


def _gateway_message(message):
    return json.dumps({"errors": [{"message": message}]}).encode()


def _judge(body, *, attested, status=200, arrived_at=NOW):
    return judge(GatewayReply(status, attested, body, arrived_at), BAR)


def _bad_indexer(error):
    return _gateway_message(f"bad indexers: {{{INDEXER}: {error}}}")


# ---------------------------------------------------------------------------
# Classifying a reply: 1 test per row of the table, in its order
# ---------------------------------------------------------------------------


def test_signed_answer_within_the_bar_passes():
    judgement = _judge(_answer({"number": 5, "timestamp": int(NOW) - 1}), attested=True)

    assert (judgement.outcome, judgement.reason) == (PASS, None)
    assert judgement.age_seconds == pytest.approx(1.0)
    assert judgement.attested is True


def test_signed_answer_older_than_the_bar_is_stale():
    judgement = _judge(_answer({"number": 5, "timestamp": int(NOW) - 1779}), attested=True)

    assert (judgement.outcome, judgement.reason) == (FAIL, "stale")
    assert judgement.age_seconds == pytest.approx(1779.0)
    assert judgement.attested is True


@pytest.mark.parametrize(
    ("age", "outcome"),
    [(BAR - 0.001, PASS), (BAR, PASS), (BAR + 0.001, FAIL)],
)
def test_the_bar_itself_still_passes(age, outcome):
    timestamp = 1_794_345_600
    judgement = _judge(
        _answer({"number": 5, "timestamp": timestamp}), attested=True, arrived_at=timestamp + age
    )

    assert judgement.outcome == outcome


@pytest.mark.parametrize("ahead", [30, 60])
def test_a_block_up_to_60_seconds_in_the_future_counts_as_0_seconds_old(ahead):
    judgement = _judge(_answer({"number": 5, "timestamp": int(NOW) + ahead}), attested=True)

    assert (judgement.outcome, judgement.age_seconds) == (PASS, 0.0)


def test_a_block_dated_further_in_the_future_fails_as_indexer_error():
    judgement = _judge(_answer({"number": 5, "timestamp": int(NOW) + 61}), attested=True)

    assert (judgement.outcome, judgement.reason) == (FAIL, "indexer_error")
    assert "in the future" in judgement.detail
    assert judgement.age_seconds is None


@pytest.mark.parametrize(
    "timestamp",
    [10**400, 2**63, 253_402_300_800, -1, 1_794_345_600.5],
    ids=["huge", "2^63", "after-9999", "negative", "fractional"],
)
def test_a_timestamp_that_isnt_a_unix_time_in_seconds_fails_without_an_exception(timestamp):
    """A number too big for a float once broke the check with an exception, voiding it."""
    judgement = _judge(_answer({"number": 5, "timestamp": timestamp}), attested=True)

    assert (judgement.outcome, judgement.reason) == (FAIL, "indexer_error")
    assert judgement.age_seconds is None


def test_signed_block_without_a_timestamp_fails_as_no_timestamp():
    judgement = _judge(_answer({"number": 5, "timestamp": None}), attested=True)

    assert (judgement.outcome, judgement.reason) == (FAIL, "no_timestamp")
    assert judgement.age_seconds is None
    assert judgement.attested is True


def test_signed_graphql_errors_fail_as_indexer_error():
    body = json.dumps({"errors": [{"message": "Store error: database unavailable"}]}).encode()

    judgement = _judge(body, attested=True)

    assert (judgement.outcome, judgement.reason) == (FAIL, "indexer_error")
    assert judgement.detail == "Store error: database unavailable"
    assert judgement.attested is True


@pytest.mark.parametrize(
    "body",
    [
        b"<html>not json</html>",
        b'{"data": {"_meta": {"block": {"number": 1, "timestamp": "soon"}}}}',
        b'{"data": {"_meta": {"block": {"number": 1, "timestamp": NaN}}}}',
        b'{"data": {"_meta": {"block": {"number": 1, "timestamp": true}}}}',
        b'{"data": null}',
    ],
)
def test_signed_answers_that_cannot_be_read_fail_as_indexer_error(body):
    judgement = _judge(body, attested=True)

    assert (judgement.outcome, judgement.reason) == (FAIL, "indexer_error")
    assert judgement.attested is True


@pytest.mark.parametrize(
    "message",
    [
        "auth error: API key out of funds",
        "no indexers found",
        "internal error: something broke",
        f"bad indexers: {{{INDEXER}: BadResponse(402)}}",
    ],
)
def test_a_signed_body_that_reads_like_a_gateway_error_is_the_indexers_answer(message):
    """Only the header decides: an indexer can't void its own check by signing gateway words."""
    judgement = _judge(_gateway_message(message), attested=True)

    assert (judgement.outcome, judgement.reason) == (FAIL, "indexer_error")
    assert judgement.attested is True


@pytest.mark.parametrize("error", ["BadResponse(400)", "BadResponse(402)", 'BadResponse("402")'])
def test_refused_payment_is_void_until_blame_is_known(error):
    judgement = _judge(_bad_indexer(error), attested=False)

    assert (judgement.outcome, judgement.reason) == (VOID, "payment_refused")
    assert judgement.attested is False


def test_indexer_timeout_fails():
    judgement = _judge(_bad_indexer("Timeout"), attested=False)

    assert (judgement.outcome, judgement.reason) == (FAIL, "indexer_timeout")


@pytest.mark.parametrize(
    "error", ["Unavailable(blocked)", "Unavailable(missing block)", "Unavailable(not supported)"]
)
def test_indexer_unavailable_fails(error):
    judgement = _judge(_bad_indexer(error), attested=False)

    assert (judgement.outcome, judgement.reason) == (FAIL, "indexer_unavailable")


@pytest.mark.parametrize(
    "error", ["Unavailable(internal error: indexer not found)", "Unavailable(internal error)"]
)
def test_unavailable_for_the_gateways_own_internal_error_is_void(error):
    """The gateway's own code calls this case one that should not happen; it isn't the
    indexer's doing."""
    judgement = _judge(_bad_indexer(error), attested=False)

    assert (judgement.outcome, judgement.reason) == (VOID, "gateway_error")


@pytest.mark.parametrize(
    "error",
    [
        "BadResponse(failed to connect)",
        "BadResponse(no attestation)",
        "BadResponse(4000)",
        "BadResponse(internal error: the words don't matter here)",
        "SomethingNew",
    ],
)
def test_any_other_bad_indexer_error_fails_as_indexer_error(error):
    judgement = _judge(_bad_indexer(error), attested=False)

    assert (judgement.outcome, judgement.reason) == (FAIL, "indexer_error")
    assert judgement.detail.startswith("bad indexers:")


@pytest.mark.parametrize("message", ["no indexers found", "subgraph not found: QmX"])
def test_unknown_pairing_is_void(message):
    judgement = _judge(_gateway_message(message), attested=False)

    assert (judgement.outcome, judgement.reason) == (VOID, "pairing_unknown")


def test_key_out_of_funds_is_void():
    judgement = _judge(_gateway_message("auth error: insufficient funds"), attested=False)

    assert (judgement.outcome, judgement.reason) == (VOID, "key_out_of_funds")
    assert judgement.detail == "auth error: insufficient funds"


@pytest.mark.parametrize(
    "message",
    ["internal error: oops", "bad query: unexpected token", "something the gateway never said"],
)
def test_other_gateway_messages_are_void(message):
    judgement = _judge(_gateway_message(message), attested=False)

    assert (judgement.outcome, judgement.reason) == (VOID, "gateway_error")
    assert judgement.detail == message


@pytest.mark.parametrize(
    "result",
    [
        GatewayUnreachable("no answer within 25s"),
        GatewayUnreachable("connection error: ClientConnectorError"),
        GatewayReply(503, False, b"busy", NOW),
        GatewayReply(500, True, _answer({"number": 1, "timestamp": int(NOW)}), NOW),
        GatewayReply(200, False, b"<html>proxy page</html>", NOW),
        GatewayReply(200, False, b'{"data": {}}', NOW),
    ],
)
def test_no_usable_http_answer_is_a_void_gateway_error(result):
    judgement = judge(result, BAR)

    assert (judgement.outcome, judgement.reason) == (VOID, "gateway_error")
    assert judgement.detail


def test_gateway_messages_are_cut_to_300_characters():
    judgement = _judge(_gateway_message("internal error: " + "x" * 1000), attested=False)

    assert len(judgement.detail) == 300


def test_an_indexers_error_message_holding_half_a_surrogate_pair_becomes_text_utf8_can_hold():
    """A JSON escape like \\ud800 decodes to half a pair, which the service refuses in detail."""
    body = b'{"errors": [{"message": "store error \\ud800 here"}]}'

    judgement = _judge(body, attested=True)

    assert judgement.detail == "store error \\ud800 here"
    judgement.detail.encode("utf-8")


def test_a_gateway_message_quoting_half_a_surrogate_pair_becomes_text_utf8_can_hold():
    judgement = _judge(_bad_indexer("BadResponse(\ud800)"), attested=False)

    assert judgement.detail.endswith("BadResponse(\\ud800)}")
    judgement.detail.encode("utf-8")


def test_detail_is_cut_to_300_characters_after_it_is_cleaned():
    judgement = _judge(_gateway_message("x" * 298 + "\udfff\udfff"), attested=False)

    assert len(judgement.detail) == 300
    judgement.detail.encode("utf-8")


# ---------------------------------------------------------------------------
# Eligibility
# ---------------------------------------------------------------------------


def _allocation(deployment_letter, created_at, chain="arbitrum-one", indexer=INDEXER):
    return Allocation(
        indexer=indexer,
        chain=chain,
        deployment_id="0x" + deployment_letter.lower() * 64,
        ipfs_hash="Qm" + deployment_letter * 44,
        created_at=int(created_at),
    )


def test_an_old_open_allocation_is_eligible_without_a_lookup():
    allocations = [_allocation("A", NOW - 8 * DAY)]

    assert first_allocation_lookups(allocations, NOW, WEEK, {}) == []
    assert eligible_deployments(allocations, {}, NOW, WEEK) == {
        (INDEXER, "arbitrum-one"): ("Qm" + "A" * 44,)
    }


def test_a_young_first_allocation_is_not_eligible():
    allocation = _allocation("A", NOW - 2 * DAY)
    key = (INDEXER, allocation.deployment_id)

    assert first_allocation_lookups([allocation], NOW, WEEK, {}) == [key]
    assert eligible_deployments([allocation], {key: allocation.created_at}, NOW, WEEK) == {}


def test_a_young_reopened_allocation_counts_from_the_first_allocation():
    allocation = _allocation("A", NOW - DAY)
    key = (INDEXER, allocation.deployment_id)

    eligible = eligible_deployments([allocation], {key: int(NOW - 30 * DAY)}, NOW, WEEK)

    assert eligible == {(INDEXER, "arbitrum-one"): ("Qm" + "A" * 44,)}


def test_a_young_allocation_whose_first_allocation_is_unknown_is_not_eligible():
    assert eligible_deployments([_allocation("A", NOW - DAY)], {}, NOW, WEEK) == {}


def test_known_first_allocations_are_not_looked_up_again():
    allocation = _allocation("A", NOW - DAY)
    known = {(INDEXER, allocation.deployment_id): int(NOW - DAY)}

    assert first_allocation_lookups([allocation], NOW, WEEK, known) == []


def test_eligibility_is_per_chain_and_uses_the_oldest_open_allocation():
    allocations = [
        _allocation("A", NOW - DAY),
        _allocation("A", NOW - 9 * DAY),
        _allocation("B", NOW - 9 * DAY, chain="mainnet"),
        _allocation("C", NOW - 9 * DAY),
    ]

    assert first_allocation_lookups(allocations, NOW, WEEK, {}) == []
    assert eligible_deployments(allocations, {}, NOW, WEEK) == {
        (INDEXER, "arbitrum-one"): ("Qm" + "A" * 44, "Qm" + "C" * 44),
        (INDEXER, "mainnet"): ("Qm" + "B" * 44,),
    }


# ---------------------------------------------------------------------------
# The general-fault rule
# ---------------------------------------------------------------------------


def _indexer(n):
    return f"0x{n:040x}"


def _silent(n, reason="indexer_timeout"):
    return CheckResult(_indexer(n), "mainnet", FAIL, reason, asked=True)


def _answered(n, outcome=FAIL):
    reason = "stale" if outcome == FAIL else None
    return CheckResult(_indexer(n), "mainnet", outcome, reason, attested=True, asked=True)


def test_3_asked_and_none_answered_voids_every_unsigned_fail():
    results = [_silent(1), _silent(2), _silent(3, "indexer_unavailable")]

    voided = apply_general_fault(results)

    assert [(r.outcome, r.reason) for r in voided] == [(VOID, "general_fault")] * 3
    assert voided[2].detail == "indexer_unavailable"


def test_2_asked_and_none_answered_is_not_a_general_fault():
    results = [_silent(1), _silent(2)]

    assert apply_general_fault(results) == results


def test_10_asked_and_6_silent_voids_only_the_unsigned_fails():
    results = [_silent(n) for n in range(6)] + [_answered(n, FAIL) for n in range(6, 10)]

    voided = apply_general_fault(results)

    assert [r.outcome for r in voided[:6]] == [VOID] * 6
    assert voided[6:] == results[6:]


def test_10_asked_and_5_silent_is_not_a_general_fault():
    results = [_silent(n) for n in range(5)] + [_answered(n, PASS) for n in range(5, 10)]

    assert not is_general_fault(results)
    assert apply_general_fault(results) == results


def test_9_asked_with_a_silent_majority_is_not_a_general_fault():
    results = [_silent(n) for n in range(5)] + [_answered(n, PASS) for n in range(5, 9)]

    assert not is_general_fault(results)


def test_the_rule_counts_indexers_not_checks_and_ignores_unasked_pairs():
    """1 signed answer on any chain makes the indexer an answering one."""
    results = [
        _silent(1),
        CheckResult(_indexer(1), "base", PASS, attested=True, asked=True),
        _silent(2),
        _silent(3),
        CheckResult(_indexer(4), "base", NO_EVIDENCE, "no_eligible_subgraph"),
    ]

    assert not is_general_fault(results)


def test_void_and_no_evidence_outcomes_are_left_alone_in_a_general_fault():
    results = [
        _silent(1),
        CheckResult(_indexer(2), "mainnet", VOID, "gateway_error", asked=True),
        CheckResult(_indexer(3), "mainnet", VOID, "pairing_unknown", asked=True),
        CheckResult(_indexer(4), "mainnet", NO_EVIDENCE, "no_eligible_subgraph"),
    ]

    voided = apply_general_fault(results)

    assert voided[0].reason == "general_fault"
    assert voided[1:] == results[1:]


# ---------------------------------------------------------------------------
# Which pairs are checked, and what is pushed
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "chains"),
    [
        ('["arbitrum-one", "mainnet"]', ["arbitrum-one", "mainnet"]),
        (["arbitrum-one", "mainnet"], ["arbitrum-one", "mainnet"]),
        ('["mainnet", "mainnet", 7, "", null]', ["mainnet"]),
        (["x" * 65, "base"], ["base"]),
        ("[]", []),
        ("", []),
        (None, []),
        (float("nan"), []),
        ("x{", []),
        ('{"mainnet": true}', []),
    ],
)
def test_supported_chains_accepts_the_shapes_the_service_does(value, chains):
    assert supported_chains(value) == chains


@pytest.mark.parametrize(
    "chain",
    ["\ud800", "main\nnet", "", "x" * 65, "arbitrum one", "mainnet\t", "réseau", "\x7f"],
    ids=["half-surrogate", "line-break", "empty", "65-chars", "space", "tab", "non-ascii", "del"],
)
def test_a_chain_name_the_service_or_a_log_line_cannot_take_is_dropped(chain):
    """1 such name from 1 indexer used to make the service refuse every outcome of the minute."""
    assert supported_chains(["mainnet", chain]) == ["mainnet"]
    assert supported_chains(json.dumps(["mainnet", chain])) == ["mainnet"]


def test_a_chain_name_of_64_printable_characters_is_kept():
    assert supported_chains(["!" + "x" * 62 + "~"]) == ["!" + "x" * 62 + "~"]


def _offer(indexer, chains, prices):
    return {
        "indexer": indexer,
        "dips_supported_networks": json.dumps(chains),
        "dips_min_grt_per_30_days": json.dumps(prices),
    }


@pytest.mark.parametrize(
    ("prices", "priced"),
    [
        ('{"mainnet": 100.5, "base": 0}', {"mainnet", "base"}),
        ({"mainnet": 100.5}, {"mainnet"}),
        ('{"mainnet": "12.5"}', {"mainnet"}),
        ('{"a": NaN, "b": -1, "c": true, "d": null, "e": "cheap", "f": [1], "g": 1e400}', set()),
        ("{}", set()),
        ("", set()),
        ("x{", set()),
        ('["mainnet"]', set()),
    ],
)
def test_an_indexer_is_paired_only_with_chains_it_has_priced_as_the_service_reads_prices(
    prices, priced
):
    """Selection can't pick an indexer on a chain without a finite price of at least 0, so
    checking it there is wasted."""
    record = {
        "indexer": INDEXER,
        "dips_supported_networks": ["mainnet", "base", "a", "b", "c", "d", "e", "f", "g"],
        "dips_min_grt_per_30_days": prices,
    }

    assert {chain for _, chain in dips_pairs([record])} == priced


def test_without_a_price_field_in_any_record_every_listed_chain_is_paired():
    """As selection does when the scores have no price column at all."""
    records = [
        {"indexer": INDEXER, "dips_supported_networks": ["mainnet", "base"]},
        {"indexer": "0x" + "b" * 40, "dips_supported_networks": '["matic"]'},
    ]

    assert dips_pairs(records) == [
        (INDEXER, "base"),
        (INDEXER, "mainnet"),
        ("0x" + "b" * 40, "matic"),
    ]


def test_with_a_price_field_in_some_records_a_record_without_one_has_no_priced_chains():
    records = [
        {"indexer": INDEXER, "dips_supported_networks": ["mainnet"]},
        _offer("0x" + "b" * 40, ["matic"], {"matic": 1}),
    ]

    assert dips_pairs(records) == [("0x" + "b" * 40, "matic")]


def test_an_indexers_chains_keep_their_order_without_repeats():
    record = _offer(INDEXER, ["mainnet", "base", "matic", "mainnet"], {"matic": 5, "mainnet": 10})

    assert dips_pairs([record]) == [(INDEXER, "mainnet"), (INDEXER, "matic")]


def test_an_indexer_listing_150_chains_is_checked_on_the_first_100_it_lists():
    chains = [f"chain-{n:03d}" for n in reversed(range(150))]

    pairs = dips_pairs([_offer(INDEXER, chains, dict.fromkeys(chains, 1))])

    assert {chain for _, chain in pairs} == set(chains[:100])


def test_the_100_chain_limit_counts_only_usable_chains():
    chains = ["\ud800", "unpriced", *(f"chain-{n:03d}" for n in range(100))]
    prices = {chain: 1 for chain in chains if chain != "unpriced"}

    pairs = dips_pairs([_offer(INDEXER, chains, prices)])

    assert len(pairs) == 100
    assert (INDEXER, "chain-099") in pairs


def test_dips_pairs_lowercases_indexers_and_skips_records_without_an_address():
    prices = {"mainnet": 1, "base": 1}
    records = [
        _offer("0x" + "B" * 40, ["mainnet", "base"], prices),
        _offer("not-an-address", ["mainnet"], prices),
        _offer(None, ["mainnet"], prices),
        _offer("0x" + "c" * 40, [], prices),
        "not a record",
    ]

    assert dips_pairs(records) == [("0x" + "b" * 40, "base"), ("0x" + "b" * 40, "mainnet")]


def test_minute_payload_has_the_contract_shape():
    result = CheckResult(INDEXER, "mainnet", PASS, deployment="Qm" + "A" * 44, age_seconds=0.8)

    payload = minute_payload(1_794_349_860.0, [result])

    assert payload == {
        "minute": "2026-11-10T22:31:00Z",
        "outcomes": [
            {
                "indexer": INDEXER,
                "chain": "mainnet",
                "outcome": "pass",
                "reason": None,
                "detail": None,
                "deployment": "Qm" + "A" * 44,
                "age_seconds": 0.8,
            }
        ],
    }


def test_format_minute_is_utc_iso_with_a_z():
    assert format_minute(0.0) == "1970-01-01T00:00:00Z"
