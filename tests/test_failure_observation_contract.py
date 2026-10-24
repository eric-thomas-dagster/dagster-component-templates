"""Failure observation contract: ONE drift test over every inlined copy of the helper, the redaction, and a built-in mutation check.

Open-source Dagster never emits ASSET_FAILED_TO_MATERIALIZE, so a failing component asset step logs an AssetObservation (failure_status/
failure_reason/error_class/error_message/will_retry/failed_step/run_id + `<prefix>_failures`) for every asset it was producing and then
re-raises the ORIGINAL exception. The helper is inlined per component (no helper files); the copies must stay byte-identical.
Per-component behaviour (real Dagster, real component) lives in each component's tests/test_failure_observation.py.
"""
import re
import textwrap
from pathlib import Path
from typing import Any, Dict, Optional

import dagster as dg
import pytest

ROOT = Path(__file__).resolve().parent.parent
_BEGIN, _END = "# BEGIN failure-observation contract", "# END failure-observation contract"
PREFIX = {"agentic_pipeline": "agentic_failures", "context_engineering_pipeline": "context_pipeline_failures", "rag_pipeline": "rag_failures",
          "context_receipt_bigquery_sync": "receipts_sync_failures", "receipt_outcome_ingestion": "outcome_ingestion_failures"}


def _block(p: Path) -> str:
    t = p.read_text()
    return t[t.index(_BEGIN): t.index(_END) + len(_END)]


def _files():
    for p in sorted((ROOT / "assets").glob("**/component.py")):
        if _BEGIN in p.read_text():
            yield p


FILES = list(_files())
REF = ROOT / "assets/ai/rag_pipeline/component.py"


def _ns(src: str) -> Dict[str, Any]:
    ns: Dict[str, Any] = {"Any": Any, "Dict": Dict, "Optional": Optional}
    exec(compile(src, "<failure-observation block>", "exec"), ns)
    return ns


def test_every_component_that_has_the_block_is_one_of_the_expected():
    assert {p.parent.name for p in FILES} >= set(PREFIX)


@pytest.mark.parametrize("path", FILES, ids=lambda p: p.parent.name)
def test_copies_are_identical(path):
    assert _block(path) == _block(REF), f"{path.relative_to(ROOT)} drifted from {REF.relative_to(ROOT)}: edit one copy and sync the rest"


@pytest.mark.parametrize("comp,key", sorted(PREFIX.items()))
def test_each_component_has_its_numeric_counter(comp, key):
    t = (ROOT / "assets/ai" / comp / "component.py").read_text()
    assert re.search(r'_FO_COUNTER = \{"%s": (dg\.)?MetadataValue\.int\(1\)\}' % key, t), f"{comp}: _FO_COUNTER must be literally {{'{key}': MetadataValue.int(1)}}"


# ---- redaction ---------------------------------------------------------------------------------------------------------------

REDACT = _ns(_block(REF))["_fo_redact"]
SECRETS = [
    ("sk-ABCDEF0123456789abcdef0123456789", "sk-ABCDEF"),
    ("sk-ant-api03-ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789", "ABCDEFGHIJKLMNOP"),
    ("ghp_" + "A1b2C3d4E5f6G7h8I9j0K1l2M3n4O5p6Q7r8", "ghp_A1b2"),
    ("AKIAABCDEFGHIJKLMNOP", "AKIAABCDEF"),
    ("xoxb-1234567890-abcdefghij", "xoxb-12345"),
    ("Authorization: Bearer abcdef123456789xyz", "abcdef12345"),
    ("authorization: token ghp_abcdefghijklmnopqrstuvwxyz0123456789", "ghp_abcdef"),
    ('headers={"Authorization": "Basic dXNlcjpwYXNzd29yZA=="}', "dXNlcjpw"),
    ("Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abcDEF123", "eyJhbGci"),
    ("postgres://svc:hunter2pass@db.internal:5432/prod", "hunter2pass"),
    ("https://user:p%40ss@api.example.com/v1", "p%40ss"),
    ("password=hunter2", "hunter2"),
    ('{"password": "p@ssw0rd!"}', "p@ssw0rd"),
    ("DB_PASSWORD: s3cr3t-value", "s3cr3t-value"),
    ("api_key=abc123def456", "abc123def456"),
    ("client_secret = abcdef", "abcdef"),
    ("blob dGhpcyBpcyBhIHZlcnkgbG9uZyBiYXNlNjQgc2VjcmV0IHZhbHVlIDEyMw==", "dGhpcyBpcyBhIHZlcnkg"),
    ("hash 5d41402abc4b2a76b9719d911017c592ffff5d41402abc4b2a76b9719d911017", "5d41402abc4b2a76"),
]


@pytest.mark.parametrize("text,leak", SECRETS)
def test_redaction_masks_secret(text, leak):
    out = REDACT(f"call failed: {text} (attempt 2)")
    assert leak not in out, out
    assert "[REDACTED]" in out and out.startswith("call failed:")


def test_redaction_keeps_ordinary_text_and_paths():
    s = "ValueError: id_column='ticket_id' not in upstream: ['a', 'b'] at /Users/me/projects/dagster/assets/ai/rag_pipeline/component.py line 42"
    assert REDACT(s) == s


def test_redaction_truncates_to_500_after_redacting():
    out = REDACT("x " * 400 + " token=" + "S" * 40)
    assert len(out) == 500
    # a secret that straddles the cut can never survive half-redacted: redaction runs before the cut
    pad = "y" * 480
    out = REDACT(pad + " password=" + "Z9" * 20)
    assert len(out) <= 500 and "Z9Z9" not in out


def test_redaction_survives_garbage():
    assert REDACT(None) == "None" and REDACT(b"bytes \xff") and REDACT("") == ""
    assert len(REDACT("a" * 10**6)) == 500


# ---- reason mapping ----------------------------------------------------------------------------------------------------------

class PolicyViolation(RuntimeError):
    pass


class ApprovalRejected(RuntimeError):
    pass


class UpstreamError(ValueError):
    pass


@pytest.mark.parametrize("exc,reason", [
    (RuntimeError("boom"), "FAILED_TO_MATERIALIZE"),
    (ValueError("x"), "FAILED_TO_MATERIALIZE"),
    (PolicyViolation("[policy:filesystem] write outside"), "POLICY_VIOLATION"),
    (PolicyViolation("[policy:network] host"), "POLICY_VIOLATION"),
    (PolicyViolation("[policy:budget] max_llm_calls=1 reached"), "BUDGET_EXCEEDED"),
    (PolicyViolation("[policy:breaker] circuit breaker open"), "BUDGET_EXCEEDED"),
    (ApprovalRejected("[approve:g] rejected"), "APPROVAL_REJECTED"),
    (UpstreamError("column missing"), "UPSTREAM_ERROR"),
    (dg.Failure("nope"), "FAILED_TO_MATERIALIZE"),
    (dg.Failure("nope", metadata={"failure_reason": "BUDGET_EXCEEDED"}), "BUDGET_EXCEEDED"),
    (dg.Failure("nope", metadata={"failure_reason": "MADE_UP"}), "FAILED_TO_MATERIALIZE"),
])
def test_reason_mapping(exc, reason):
    assert _ns(_block(REF))["_fo_reason"](exc) == reason


# ---- the contract end to end on the shared helper, and a mutation check of it ----------------------------------------------------

COUNTER = {"demo_failures": dg.MetadataValue.int(1)}
SECRET = "sk-ABCDEF0123456789abcdef0123456789"


def contract_violations(src: str) -> list:
    """Run a failing multi-asset (a, b) built on the helper in `src` with real Dagster; list every promise the helper broke."""
    h = _ns(src)
    bad = []

    @dg.multi_asset(specs=[dg.AssetSpec("a"), dg.AssetSpec("b")], can_subset=True)
    def two(context: dg.AssetExecutionContext):
        def body():
            yield dg.MaterializeResult(asset_key="a")
            raise ValueError(f"the original, key={SECRET}")
        yield from h["_fo_gen"](context, body(), counter=COUNTER, asset_keys=lambda: context.selected_asset_keys, failed_step="s1")

    @dg.asset
    def plain(context: dg.AssetExecutionContext):
        try:
            raise KeyError("plain original")
        except Exception as exc:
            h["_fo_emit"](context, exc, counter=COUNTER, asset_keys=[dg.AssetKey("plain")], failed_step="p")
            raise

    inst = dg.DagsterInstance.ephemeral()
    r = dg.materialize([two], instance=inst, raise_on_error=False)
    r2 = dg.materialize([plain], instance=inst, raise_on_error=False)

    def chain(res):
        err = [e for e in res.all_events if e.is_failure][0].event_specific_data.error
        out = []
        while err is not None:
            out.append((err.cls_name, err.message))
            err = err.cause
        return out

    def obs(res):
        o = {}
        for e in inst.all_logs(res.run_id, of_type=dg.DagsterEventType.ASSET_OBSERVATION):
            a = e.dagster_event.asset_observation_data.asset_observation
            if "failure_status" in a.metadata:
                o[a.asset_key.to_user_string()] = {k: v.value for k, v in a.metadata.items()}
        return o

    if r.success or r2.success:
        bad.append("failure swallowed")
        return bad
    if chain(r)[1][0] != "ValueError" or chain(r2)[1][0] != "KeyError":
        bad.append(f"original exception replaced: {chain(r)[1][0]}/{chain(r2)[1][0]}")
    o = obs(r)
    if set(o) != {"b"}:
        bad.append(f"attribution: expected only the unfinished asset b, got {sorted(o)}")      # a was yielded (done); b was not
    elif SECRET in o["b"]["error_message"]:
        bad.append("secret leaked")
    elif o["b"]["failure_reason"] != "FAILED_TO_MATERIALIZE" or o["b"]["demo_failures"] != 1 or o["b"]["failed_step"] != "s1":
        bad.append(f"wrong metadata {o['b']}")
    if set(obs(r2)) != {"plain"}:
        bad.append("plain asset not attributed")
    return bad


def test_helper_keeps_the_contract():
    assert contract_violations(_block(REF)) == []


MUTATIONS = {
    "reraise_replaced_by_a_new_exception": ("        _fo_emit(context, exc, counter=counter, asset_keys=asset_keys, failed_step=failed_step, skip=done)\n        raise\n",
                                            "        _fo_emit(context, exc, counter=counter, asset_keys=asset_keys, failed_step=failed_step, skip=done)\n        raise RuntimeError('masked')\n"),
    "failure_swallowed": ("        _fo_emit(context, exc, counter=counter, asset_keys=asset_keys, failed_step=failed_step, skip=done)\n        raise\n",
                          "        _fo_emit(context, exc, counter=counter, asset_keys=asset_keys, failed_step=failed_step, skip=done)\n        return\n"),
    "redaction_removed": ("    return s[:limit]\n", "    return str(text)[:limit]\n"),
    "redaction_after_truncation_only_skips_tokens": ("    s = _FO_TOKENS.sub(\"[REDACTED]\", s)\n", ""),
    "attribution_ignores_finished_assets": ("asset_keys=asset_keys, failed_step=failed_step, skip=done)", "asset_keys=asset_keys, failed_step=failed_step, skip=())"),
    "attribution_marks_nothing": ("        keys = [k for k in (asset_keys() if callable(asset_keys) else asset_keys) if k not in skip]\n",
                                  "        keys = []\n"),
}


@pytest.mark.parametrize("name", sorted(MUTATIONS))
def test_mutated_helper_is_caught(name):
    old, new = MUTATIONS[name]
    src = _block(REF)
    assert src.count(old) == 1, f"mutation {name} no longer applies to the helper: update this table"
    assert contract_violations(src.replace(old, new)), f"mutation {name} went unnoticed"


def test_emission_failure_is_a_warning_not_an_error():
    h = _ns(_block(REF))
    warned = []

    class Ctx:
        run = type("R", (), {"run_id": "r"})()
        has_partition_key = False
        has_partition_key_range = False
        log = type("L", (), {"warning": staticmethod(warned.append)})()

        def log_event(self, ev):
            raise OSError("down")
    h["_fo_emit"](Ctx(), ValueError("x"), counter=COUNTER, asset_keys=[dg.AssetKey("a")], failed_step="s")   # must not raise
    assert warned and "down" in warned[0]
    h["_fo_emit"](object(), ValueError("x"), counter=COUNTER, asset_keys=[dg.AssetKey("a")])                   # a context without log/log_event: still no raise


def test_retry_requested_is_not_a_failure():
    h = _ns(_block(REF))
    seen = []

    class Ctx:
        run = type("R", (), {"run_id": "r"})()
        has_partition_key = False
        has_partition_key_range = False
        log = type("L", (), {"warning": staticmethod(lambda m: None)})()

        def log_event(self, ev):
            seen.append(ev)
    h["_fo_emit"](Ctx(), dg.RetryRequested(max_retries=2), counter=COUNTER, asset_keys=[dg.AssetKey("a")])
    assert seen == []
    h["_fo_emit"](Ctx(), ValueError("x"), counter=COUNTER, asset_keys=[dg.AssetKey("a")])
    assert len(seen) == 1
