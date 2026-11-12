"""An indexer's claim to have synced a deployment is checked before selection relies on it.

Selection prefers an indexer that says it has already synced the deployment. That is the
indexer's own word, so before such a pick goes back to dipper the service can ask the indexer
through the gateway for the deployment's latest block, as a paying customer would. The claim
counts only if the indexer signed the answer and the block is recent. A claim that is not
confirmed, for whatever reason, loses the preference and the indexer competes on its score.
Every candidate whose claim could win it a place is asked at once; group members never are.
"""

import json
import threading
import time
import urllib.error
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from types import SimpleNamespace
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from iisa import DataManager, sync_check
from iisa import iisa_http_endpoints as endpoints
from iisa.iisa_http_endpoints import SelectionRequest, Settings, _select_checking_sync_claims
from iisa.sync_check import GatewayReply, SyncClaimChecker, post_once

CHAIN = "arbitrum-one"
DEPLOYMENT = "QmSyncClaimCheck"
A, B, C, D, M, X = ("0x" + pair * 20 for pair in ("a1", "b2", "c3", "d4", "e5", "f6"))
NOW = 1_794_400_000.0
API_KEY = "gateway-key-123"


def _reply(age: float = 5.0, **changes: Any) -> GatewayReply:
    """A gateway answer the indexer signed, whose latest block is age seconds old."""
    body = {"data": {"_meta": {"block": {"number": 100, "timestamp": int(NOW - age)}}}}
    reply = {"status": 200, "attested": True, "body": json.dumps(body).encode()}
    return GatewayReply(**{**reply, **changes})


def _checker(send: Any, **changes: Any) -> SyncClaimChecker:
    settings = {"timeout_seconds": 5.0, "bar_seconds": 36.0, **changes}
    return SyncClaimChecker(
        "https://gateway.example/api/", API_KEY, send=send, clock=lambda: NOW, **settings
    )


class TestOneCheck:
    """What the gateway's answer must show for a claim to be confirmed."""

    def test_a_signed_answer_with_a_recent_block_confirms_the_claim(self):
        assert _checker(lambda *_a: _reply()).why_not_confirmed(DEPLOYMENT, A) is None

    def test_it_asks_that_indexer_for_that_deployment_with_the_api_key(self):
        sent = []

        def send(url: str, headers: dict, body: bytes, timeout: float) -> GatewayReply:
            sent.append((url, headers, json.loads(body), timeout))
            return _reply()

        _checker(send).why_not_confirmed(DEPLOYMENT, A)

        assert sent == [
            (
                f"https://gateway.example/api/deployments/id/{DEPLOYMENT}/indexers/id/{A}",
                {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"},
                {"query": "{ _meta { block { number timestamp } } }"},
                5.0,
            )
        ]

    @pytest.mark.parametrize(
        ("reply", "reason"),
        [
            (_reply(age=37.0), "latest block is 37s old"),
            (_reply(attested=False), "not signed by the indexer"),
            (_reply(status=402), "gateway answered 402"),
            (_reply(status=500), "gateway answered 500"),
            (_reply(age=-61.0), "dated 61s in the future"),
            (_reply(body=b"not json"), "no block timestamp"),
            (_reply(body=b'{"errors": [{"message": "indexing error"}]}'), "no block timestamp"),
            (_reply(body=b'{"data": {"_meta": {"block": {"number": 1}}}}'), "no block timestamp"),
            (
                _reply(body=b'{"data": {"_meta": {"block": {"timestamp": "soon"}}}}'),
                "no block timestamp",
            ),
            (_reply(body=b'{"data": {"_meta": {"block": {"timestamp": true}}}}'), "no block"),
            (_reply(body=b"[" * 100_000), "no block timestamp"),
        ],
    )
    def test_any_other_answer_leaves_the_claim_unconfirmed(self, reply, reason):
        assert reason in _checker(lambda *_a: reply).why_not_confirmed(DEPLOYMENT, A)

    def test_a_block_at_the_bar_still_confirms(self):
        assert _checker(lambda *_a: _reply(age=36.0)).why_not_confirmed(DEPLOYMENT, A) is None

    @pytest.mark.parametrize(
        "failure",
        [urllib.error.URLError("refused"), TimeoutError("timed out"), ValueError("too large")],
    )
    def test_a_gateway_that_cannot_be_read_leaves_the_claim_unconfirmed(self, failure):
        def send(*_a: Any) -> GatewayReply:
            raise failure

        assert "could not be read" in _checker(send).why_not_confirmed(DEPLOYMENT, A)

    def test_the_api_key_never_appears_in_a_reason(self):
        def send(*_a: Any) -> GatewayReply:
            raise urllib.error.URLError(f"https://gateway.example/api/{API_KEY} refused")

        assert API_KEY not in _checker(send).why_not_confirmed(DEPLOYMENT, A)


class TestSeveralChecks:
    """Checks for 1 selection all start at once and never outlast the time limit."""

    async def test_each_unconfirmed_claim_comes_back_with_its_reason(self):
        replies = {A: _reply(), B: _reply(age=90.0), C: _reply(attested=False)}

        def send(url: str, *_a: Any) -> GatewayReply:
            return replies[url.rsplit("/", 1)[1]]

        unconfirmed = await _checker(send).unconfirmed(DEPLOYMENT, [A, B, C])

        assert sorted(unconfirmed) == [B, C]
        assert "90s old" in unconfirmed[B] and "not signed" in unconfirmed[C]

    async def test_a_check_that_breaks_is_unconfirmed_and_logged(self, caplog):
        """A fault in the check must never fail the selection it was made for."""

        def send(*_a: Any) -> GatewayReply:
            raise RuntimeError("a fault nobody planned for")

        unconfirmed = await _checker(send).unconfirmed(DEPLOYMENT, [A])

        assert unconfirmed == {A: "the check itself failed"}
        assert any(r.levelname == "ERROR" and r.exc_info for r in caplog.records)

    async def test_a_check_still_waiting_at_the_time_limit_is_unconfirmed(self):
        release = threading.Event()

        def send(url: str, *_a: Any) -> GatewayReply:
            if url.endswith(B):
                release.wait(5)
            return _reply()

        started = time.monotonic()
        unconfirmed = await _checker(send, timeout_seconds=0.2).unconfirmed(DEPLOYMENT, [A, B])
        waited = time.monotonic() - started
        release.set()

        assert list(unconfirmed) == [B] and "no answer within 0.2s" in unconfirmed[B]
        assert waited < 2

    async def test_every_check_1_selection_may_make_starts_at_once(self):
        """Each check here waits until all 50 are under way, so they only finish, and confirm
        their claims, if none of them had to wait for another to end first.
        """
        indexers = [f"0x{n:040x}" for n in range(sync_check.MAX_CHECKS_PER_SELECTION)]
        all_started = threading.Barrier(len(indexers), timeout=3)

        def send(*_a: Any) -> GatewayReply:
            all_started.wait()
            return _reply()

        assert await _checker(send).unconfirmed(DEPLOYMENT, indexers) == {}

    async def test_checks_are_sent_in_the_order_given(self, monkeypatch):
        """The caller lists the best scores first, and that is the order they are sent in when
        the service is too busy to start them all together.
        """
        monkeypatch.setattr(sync_check, "CHECK_THREADS", 1)
        sent: list[str] = []

        def send(url: str, *_a: Any) -> GatewayReply:
            sent.append(url.rsplit("/", 1)[1])
            return _reply()

        assert await _checker(send).unconfirmed(DEPLOYMENT, [C, A, B]) == {}
        assert sent == [C, A, B]

    async def test_a_check_that_never_started_is_reported_as_not_sent(self, monkeypatch):
        """With every thread busy the second check is never sent, and its reason says so
        instead of blaming the indexer for not answering.
        """
        monkeypatch.setattr(sync_check, "CHECK_THREADS", 1)
        release = threading.Event()
        sent: list[str] = []

        def send(url: str, *_a: Any) -> GatewayReply:
            sent.append(url.rsplit("/", 1)[1])
            release.wait(5)
            return _reply()

        unconfirmed = await _checker(send, timeout_seconds=0.2).unconfirmed(DEPLOYMENT, [A, B])
        release.set()

        assert sent == [A]
        assert "no answer within 0.2s" in unconfirmed[A]
        assert "was not sent" in unconfirmed[B]


class _Gateway(BaseHTTPRequestHandler):
    """A stand-in gateway whose behaviour each test sets on the class."""

    behaviour = "answer"
    seen: list[tuple[str, Optional[str], bytes]] = []
    agents: list[Optional[str]] = []

    def do_POST(self) -> None:
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        type(self).seen.append((self.path, self.headers.get("Authorization"), body))
        type(self).agents.append(self.headers.get("User-Agent"))
        if self.behaviour == "redirect" and self.path != "/elsewhere":
            self.send_response(307)
            self.send_header("Location", "/elsewhere")
            self.end_headers()
            return
        if self.behaviour == "slow":
            time.sleep(1.5)
        payload = b"x" * 200_000 if self.behaviour == "huge" else b'{"data": {}}'
        self.send_response(200)
        if self.behaviour != "unsigned":
            self.send_header("graph-attestation", '{"signature": "0x01"}')
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        try:
            if self.behaviour == "drip":
                # 1 byte every 0.1s: no single read waits long, but the answer never ends in time.
                for byte in payload:
                    self.wfile.write(bytes([byte]))
                    self.wfile.flush()
                    time.sleep(0.1)
            else:
                self.wfile.write(payload)
        except OSError:
            pass  # the client gave up, as the test meant it to

    def log_message(self, *_args: Any) -> None:
        pass


@pytest.fixture
def gateway() -> Any:
    _Gateway.behaviour, _Gateway.seen, _Gateway.agents = "answer", [], []
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Gateway)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()
    server.server_close()


class TestTheRequestItself:
    """post_once against a real local server: 1 request, no redirects, bounded size and time."""

    HEADERS = {"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"}

    def test_it_posts_once_and_reports_whether_the_answer_was_signed(self, gateway):
        signed = post_once(f"{gateway}/route", self.HEADERS, b'{"query": "q"}', 2.0)
        _Gateway.behaviour = "unsigned"
        unsigned = post_once(f"{gateway}/route", self.HEADERS, b'{"query": "q"}', 2.0)

        assert (signed.status, signed.attested, signed.body) == (200, True, b'{"data": {}}')
        assert (unsigned.status, unsigned.attested) == (200, False)
        assert _Gateway.seen[0] == ("/route", f"Bearer {API_KEY}", b'{"query": "q"}')

    def test_a_redirect_is_not_followed_so_the_key_goes_nowhere_else(self, gateway):
        _Gateway.behaviour = "redirect"

        reply = post_once(f"{gateway}/route", self.HEADERS, b"{}", 2.0)

        assert reply.status == 307 and reply.attested is False
        assert [path for path, _key, _body in _Gateway.seen] == ["/route"]

    def test_an_answer_over_the_size_limit_is_refused(self, gateway):
        _Gateway.behaviour = "huge"

        with pytest.raises(ValueError, match="over the"):
            post_once(f"{gateway}/route", self.HEADERS, b"{}", 2.0)

    def test_an_answer_that_trickles_in_gives_up_at_the_time_limit(self, gateway):
        _Gateway.behaviour = "drip"

        started = time.monotonic()
        with pytest.raises(TimeoutError, match="did not finish"):
            post_once(f"{gateway}/route", self.HEADERS, b"{}", 0.35)

        assert time.monotonic() - started < 1.0

    def test_it_names_itself_to_the_gateway(self, gateway):
        """The library's own name is refused by some gateways' front doors."""
        post_once(f"{gateway}/route", self.HEADERS, b"{}", 2.0)

        assert _Gateway.agents == ["iisa-sync-check"]

    def test_a_slow_answer_gives_up_at_the_time_limit(self, gateway):
        _Gateway.behaviour = "slow"

        started = time.monotonic()
        with pytest.raises((TimeoutError, OSError)):
            post_once(f"{gateway}/route", self.HEADERS, b"{}", 0.3)

        assert time.monotonic() - started < 1.2


def _record(indexer: str, success_rate: float, **changes: Any) -> dict[str, Any]:
    """One scored indexer, in its own organisation and place, so any 2 make a group selection
    accepts; a higher success rate gives a higher score, all else equal.
    """
    record = {
        "indexer": indexer,
        "url": f"https://{indexer[2:8]}.example/",
        "lat_lin_reg_coefficient": 0.012,
        "lat_coefficient_upper_bound": 0.016,
        "lat_normalized_score": 0.7,
        "uptime_score": 0.99,
        "success_rate": success_rate,
        "stake_to_fees": 100.0,
        "total_query_fees": 10.0,
        "last_known_slashable_stake": 1_000.0,
        "org": f"org-{indexer[2:4]}",
        "dst_lat": int(indexer[2:4], 16) / 4,
        "dst_lon": 4.9,
        "computed_at": "2026-11-12T09:00:00Z",
        "dips_info_available": True,
        "dips_min_grt_per_30_days": f'{{"{CHAIN}": "450"}}',
        "dips_min_grt_per_billion_entities_per_30_days": "200",
        "dips_supported_networks": f'["{CHAIN}"]',
    }
    return {**record, **changes}


# By score alone D is the best pick, then C, then B; A is the worst. The rates sit close
# together because a success rate under 97% of the best scores 0, which would tie A, B and C.
RECORDS = [_record(A, 0.970), _record(B, 0.975), _record(C, 0.980), _record(D, 0.990)]
# M is a weak indexer scoring under 0.45: less than the 0.60 a synced candidate needs once the
# group has a synced member, and not enough to replace anyone. Beside it A, B, C and D all score
# over 0.60. X is a failing indexer scoring about 0.03, which selection replaces when a candidate
# beats it by more than 0.50.
_WEAK = {"uptime_score": 0.5, "stake_to_fees": 0.0001}
_FAILING = {
    **_WEAK,
    "lat_normalized_score": 0.0,
    "dips_min_grt_per_30_days": f'{{"{CHAIN}": "100000"}}',
}
WITH_WEAK = RECORDS + [_record(M, 0.5, **_WEAK)]
WITH_WEAK_AND_FAILING = WITH_WEAK + [_record(X, 0.5, **_FAILING)]


class FakeChecker:
    """Stands in for the gateway: the claims in `fail` are not confirmed."""

    def __init__(self, fail: tuple[str, ...] = ()) -> None:
        self.fail = set(fail)
        self.asked: list[tuple[str, list[str]]] = []

    async def unconfirmed(self, deployment_id: str, indexers: list[str]) -> dict[str, str]:
        self.asked.append((deployment_id, list(indexers)))
        return {indexer: "latest block is 900s old" for indexer in indexers if indexer in self.fail}


def _state(
    claimed: set[str],
    checker: Optional[FakeChecker],
    records: Optional[list[dict[str, Any]]] = None,
) -> SimpleNamespace:
    """A loaded service in which the indexers in `claimed` say they have synced DEPLOYMENT."""
    history = DataManager(MagicMock()).transform_scores_df(pd.DataFrame(records or RECORDS))
    sync_status = SimpleNamespace(synced_indexers_for=lambda _deployment: set(claimed))
    return SimpleNamespace(
        history=history, sync_status=sync_status, sync_checker=checker, is_ready=True
    )


async def _pick(
    claimed: set[str],
    checker: Optional[FakeChecker],
    records: Optional[list[dict[str, Any]]] = None,
    **request: Any,
) -> list[str]:
    """The indexers selection answers with; 1 is asked for unless the request says otherwise."""
    selection = SelectionRequest(
        **{"deployment_id": DEPLOYMENT, "chain_id": CHAIN, "num_candidates": 1, **request}
    )
    with patch.object(endpoints, "_state", _state(claimed, checker, records)):
        response = await _select_checking_sync_claims(selection)
    return [indexer.id for indexer in response.indexers]


class TestSelection:
    """When a claim puts candidates ahead, each of them is asked at once, and selection then
    counts only the claims that were confirmed. Group members are never asked.
    """

    async def test_with_no_checker_the_claim_is_taken_on_trust_as_before(self):
        assert await _pick({A}, None) == [A]

    async def test_a_confirmed_claim_keeps_its_place(self):
        checker = FakeChecker()

        assert await _pick({A}, checker) == [A]
        assert checker.asked == [(DEPLOYMENT, [A])]

    async def test_an_unconfirmed_claim_loses_the_preference(self):
        """A only led because of its claim; without it the best score, D, is picked."""
        checker = FakeChecker(fail=(A,))

        assert await _pick({A}, checker) == [D]
        assert checker.asked == [(DEPLOYMENT, [A])]

    async def test_with_no_claim_in_play_nothing_is_checked(self):
        checker = FakeChecker()

        assert await _pick(set(), checker) == [D]
        assert checker.asked == []

    async def test_every_claim_is_checked_together_in_1_round(self):
        """C leads on its claim, so A, B and C are all asked at once, not only C, listed with
        the best score first.
        """
        checker = FakeChecker()

        assert await _pick({A, B, C}, checker) == [C]
        assert checker.asked == [(DEPLOYMENT, [C, B, A])]

    async def test_the_best_confirmed_claim_wins_when_better_ones_fail(self):
        """C and B both claim falsely. A's claim is true, so A still gets its head start."""
        checker = FakeChecker(fail=(B, C))

        assert await _pick({A, B, C}, checker) == [A]
        assert checker.asked == [(DEPLOYMENT, [C, B, A])]

    async def test_when_every_claim_fails_the_best_score_wins(self):
        checker = FakeChecker(fail=(A, B, C))

        assert await _pick({A, B, C}, checker) == [D]
        assert len(checker.asked) == 1

    async def test_a_group_that_needs_nobody_new_costs_no_check(self):
        """A already fills the 1 place and is doing well, so selection never looks at the
        candidates and no claim, A's or B's, is weighed.
        """
        checker = FakeChecker(fail=(A, B))

        picked = await _pick({A, B}, checker, existing_indexers=[A])

        assert picked == [A] and checker.asked == []

    async def test_a_members_claim_alone_costs_no_check(self):
        """A place is open but no candidate claims, so D's claim has nothing to decide."""
        checker = FakeChecker(fail=(D,))

        picked = await _pick({D}, checker, existing_indexers=[D], num_candidates=2)

        assert picked == [D, C] and checker.asked == []

    async def test_a_failed_claim_gives_way_in_a_group_that_already_has_a_member(self):
        """D is serving and 1 more is wanted. A leads on a claim that fails the check, so the
        place goes to the best score, C.
        """
        checker = FakeChecker(fail=(A,))

        assert await _pick({A}, checker, existing_indexers=[D], num_candidates=2) == [D, C]

    async def test_a_true_claim_beats_a_false_one_beside_a_member(self):
        """B leads on a false claim. A's claim is true, so A takes the open place, not B."""
        checker = FakeChecker(fail=(B,))

        assert await _pick({A, B}, checker, existing_indexers=[D], num_candidates=2) == [D, A]

    async def test_a_claim_that_blocks_a_swap_is_checked_though_nobody_is_picked_on_it(self):
        """X is failing and D should replace it. M says it is synced, so it is tried first, and
        M is too small an improvement, so on trust X would stay. M's claim fails, D replaces X.
        """
        checker = FakeChecker(fail=(M,))

        assert await _pick({M}, checker, WITH_WEAK_AND_FAILING, existing_indexers=[X]) == [D]

    async def test_a_group_member_is_never_asked(self):
        """D is in the group and says it is synced. That is taken as given, even here where
        the check would have refused it, and only the candidate A is asked.
        """
        checker = FakeChecker(fail=(D,))
        group = {"existing_indexers": [D], "num_candidates": 2}

        assert await _pick({D, A}, checker, WITH_WEAK, **group) == [D, A]
        assert checker.asked == [(DEPLOYMENT, [A])]

    async def test_a_member_being_replaced_is_not_asked(self):
        """X fails on its score and D replaces it. X's own claim is not checked."""
        checker = FakeChecker()

        picked = await _pick({X, D}, checker, WITH_WEAK_AND_FAILING, existing_indexers=[X])

        assert picked == [D] and checker.asked == [(DEPLOYMENT, [D])]

    async def test_a_claim_too_weak_to_count_is_not_asked(self):
        """With D, a synced member, in the group, a synced candidate needs a score of 0.60 to
        be put ahead. M is under it, so its claim wins nothing and costs no query; A is asked.
        """
        checker = FakeChecker()
        group = {"existing_indexers": [D], "num_candidates": 2}

        assert await _pick({D, A, M}, checker, WITH_WEAK, **group) == [D, A]
        assert checker.asked == [(DEPLOYMENT, [A])]

    async def test_a_weak_claim_is_asked_when_it_would_be_the_first_synced(self):
        """No member is synced, so the first synced candidate gets in at any score and M's weak
        claim does decide the pick. It is asked, fails, and the place goes to C.
        """
        checker = FakeChecker(fail=(M,))
        group = {"existing_indexers": [D], "num_candidates": 2}

        assert await _pick({M}, checker, WITH_WEAK, **group) == [D, C]
        assert checker.asked == [(DEPLOYMENT, [M])]

    async def test_after_a_failed_check_only_members_and_confirmed_claims_count(self):
        """B leads on a claim that fails, so selection runs again. It counts the claim of D, a
        member, and of A, which was confirmed. M's was too weak to be asked and is left out.
        """
        checker = FakeChecker(fail=(B,))
        group = {"existing_indexers": [D], "num_candidates": 2}

        with patch.object(endpoints, "_build_selector", wraps=endpoints._build_selector) as build:
            picked = await _pick({D, A, B, M}, checker, WITH_WEAK, **group)

        assert picked == [D, A] and checker.asked == [(DEPLOYMENT, [B, A])]
        assert build.call_args_list[1].args[2] == {D, A}

    async def test_a_claimant_that_cannot_be_picked_is_not_checked(self):
        """B is blocked by the caller, so its claim can win nothing and costs no query."""
        checker = FakeChecker()

        assert await _pick({A, B}, checker, blocklist=[B]) == [A]
        assert checker.asked == [(DEPLOYMENT, [A])]

    async def test_no_more_than_the_limit_are_checked_best_scores_first(self, monkeypatch):
        """With a limit of 2, the 2 best-scoring claimants are asked; A's claim is left out
        unchecked, so it counts for nothing.
        """
        monkeypatch.setattr(endpoints, "MAX_CHECKS_PER_SELECTION", 2)
        checker = FakeChecker(fail=(B, C))

        assert await _pick({A, B, C}, checker) == [D]
        assert checker.asked == [(DEPLOYMENT, [C, B])]

    async def test_a_failed_check_is_logged_with_its_reason(self, caplog):
        with caplog.at_level("INFO"):
            await _pick({A}, FakeChecker(fail=(A,)))

        warning = next(r for r in caplog.records if r.levelname == "WARNING" and A in r.message)
        assert DEPLOYMENT in warning.message and "latest block is 900s old" in warning.message


def test_the_endpoint_answers_with_the_checked_selection():
    body = {"deployment_id": DEPLOYMENT, "chain_id": CHAIN, "num_candidates": 1}
    with patch.object(endpoints, "_state", _state({A}, FakeChecker(fail=(A,)))):
        response = TestClient(endpoints.app).post("/select-indexers", json=body)

    assert response.status_code == 200
    assert [indexer["id"] for indexer in response.json()["indexers"]] == [D]


class TestSettings:
    """The check is off unless the gateway's address and key are both set."""

    GATEWAY = {
        "sync_check_gateway_url": "https://gateway.example/api",
        "sync_check_gateway_api_key": API_KEY,
    }

    def test_it_is_off_by_default(self):
        assert endpoints.sync_claim_checker(Settings(_env_file=None)) is None

    def test_an_address_and_a_key_switch_it_on(self):
        assert endpoints.sync_claim_checker(Settings(_env_file=None, **self.GATEWAY)) is not None

    @pytest.mark.parametrize(
        "changes",
        [
            {"sync_check_gateway_api_key": None},
            {"sync_check_gateway_url": None},
            {"sync_check_gateway_url": "gateway.example/api"},
            {"sync_check_gateway_url": "file:///etc/hosts"},
            {"sync_check_timeout_seconds": 0},
            {"sync_check_timeout_seconds": 11},
            {"sync_check_bar_seconds": 0},
        ],
    )
    def test_a_half_set_or_unusable_setting_stops_the_service_starting(self, changes):
        with pytest.raises(ValidationError):
            Settings(_env_file=None, **{**self.GATEWAY, **changes})

    @pytest.mark.parametrize(
        "changes",
        [{"sync_check_gateway_url": None}, {"sync_check_gateway_url": "gateway.example/api"}],
    )
    def test_the_startup_error_shows_no_part_of_any_secret(self, changes):
        """The error is logged when startup fails. Left alone, it prints the settings it was
        given, cut short, and the API key and tokens are among them.
        """
        secrets = {
            "push_token": "push-token-0123456789abcdef",
            "read_token": "read-token-0123456789abcdef",
            "sync_check_gateway_api_key": "gateway-key-0123456789abcdef0123456789abcdef",
        }
        settings = {"host": "0.0.0.0", "port": 8080, **self.GATEWAY, **secrets, **changes}

        with pytest.raises(ValidationError) as error:
            Settings(_env_file=None, **settings)

        shown = str(error.value)
        for secret in secrets.values():
            assert not any(secret[i : i + 6] in shown for i in range(len(secret) - 5))
