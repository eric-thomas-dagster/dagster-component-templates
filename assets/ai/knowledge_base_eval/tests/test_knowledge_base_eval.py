"""Committed tests for KnowledgeBaseEvalComponent. The search resource is a plain fake object
(both KnowledgeSearchResource and HybridKnowledgeSearchResource are themselves just thin
dispatchers over real vector-store SDKs already covered by their own components' tests) --
what's under test here is this component's own scoring/metadata/asset-check logic."""
import dagster as dg
import pytest

CLS = "KnowledgeBaseEvalComponent"


@pytest.fixture()
def mod():
    from .conftest import load_component_module
    return load_component_module()


class _FakeChunksResource:
    """Mimics KnowledgeSearchResource.search() -> {"chunks": [...]}."""

    def __init__(self, results_by_question):
        self.results_by_question = results_by_question
        self.calls = []

    def search(self, query, top_k=5):
        self.calls.append((query, top_k))
        return {"chunks": self.results_by_question.get(query, [])}


class _FakePassagesResource:
    """Mimics HybridKnowledgeSearchResource.search() -> {"passages": [...], "triples": [...]}."""

    def __init__(self, results_by_question):
        self.results_by_question = results_by_question

    def search(self, query, top_k=5):
        return {"passages": self.results_by_question.get(query, []), "triples": []}


def _hit(chunk_id, text, score=0.9, source=None, doc_id=None):
    return {"chunk_id": chunk_id, "text": text, "score": score, "source": source, "doc_id": doc_id}


# ─── _search_resource_results dispatch ──────────────────────────────────────

def test_dispatches_chunks_shape(mod):
    resource = _FakeChunksResource({"q": [_hit("c1", "hello")]})
    out = mod._search_resource_results(resource, "q", 5)
    assert out == [_hit("c1", "hello")]


def test_dispatches_passages_shape(mod):
    resource = _FakePassagesResource({"q": [_hit("c1", "hello")]})
    out = mod._search_resource_results(resource, "q", 5)
    assert out == [_hit("c1", "hello")]


def test_dispatch_raises_clearly_on_unknown_shape(mod):
    class _Bad:
        def search(self, query, top_k=5):
            return {"something_else": []}
    with pytest.raises(ValueError, match="neither 'passages' nor 'chunks'"):
        mod._search_resource_results(_Bad(), "q", 5)


# ─── _score_question ─────────────────────────────────────────────────────────

def test_score_is_fraction_of_expected_terms_found(mod):
    resource = _FakeChunksResource({"q": [_hit("c1", "The retry_policy controls max_retries.")]})
    result = mod._score_question(resource, "q", ["retry_policy", "max_retries", "backoff"], [], 5)
    assert result["score"] == pytest.approx(2 / 3)
    assert set(result["matched_terms"]) == {"retry_policy", "max_retries"}
    assert result["missing_terms"] == ["backoff"]


def test_no_expected_terms_scores_1(mod):
    resource = _FakeChunksResource({"q": [_hit("c1", "anything")]})
    result = mod._score_question(resource, "q", [], [], 5)
    assert result["score"] == 1.0
    assert result["matched_terms"] == []
    assert result["missing_terms"] == []


def test_term_matching_is_case_insensitive(mod):
    resource = _FakeChunksResource({"q": [_hit("c1", "RETRY_POLICY is here")]})
    result = mod._score_question(resource, "q", ["retry_policy"], [], 5)
    assert result["score"] == 1.0


def test_hit_and_rank_via_source(mod):
    resource = _FakeChunksResource({"q": [
        _hit("c1", "irrelevant", source="other_doc"),
        _hit("c2", "irrelevant", source="docs/retry-policies"),
    ]})
    result = mod._score_question(resource, "q", [], ["docs/retry-policies"], 5)
    assert result["hit"] is True
    assert result["rank"] == 2  # 1-indexed


def test_hit_via_doc_id_or_chunk_id_also_counts(mod):
    resource = _FakeChunksResource({"q": [_hit("c1", "x", doc_id="doc-42")]})
    result = mod._score_question(resource, "q", [], ["doc-42"], 5)
    assert result["hit"] is True
    assert result["rank"] == 1


def test_no_expected_sources_hit_and_rank_are_none(mod):
    resource = _FakeChunksResource({"q": [_hit("c1", "x")]})
    result = mod._score_question(resource, "q", [], [], 5)
    assert result["hit"] is None
    assert result["rank"] is None


def test_expected_sources_with_no_match_is_hit_false_rank_none(mod):
    resource = _FakeChunksResource({"q": [_hit("c1", "x", source="other")]})
    result = mod._score_question(resource, "q", [], ["docs/retry-policies"], 5)
    assert result["hit"] is False
    assert result["rank"] is None


def test_sources_list_never_includes_passage_text(mod):
    resource = _FakeChunksResource({"q": [_hit("c1", "a very secret internal sentence", source="docs/x", doc_id="d1")]})
    result = mod._score_question(resource, "q", [], [], 5)
    assert result["sources"] == [{"source": "docs/x", "doc_id": "d1", "chunk_id": "c1"}]
    assert "a very secret internal sentence" not in str(result["sources"])


# ─── full asset materialization ──────────────────────────────────────────────

def _build(mod, **overrides):
    defaults = dict(
        asset_name="kb_eval", search_resource_key="search", upstream_asset_key="kb_base",
        golden_set=[
            {"question": "q1", "expected_terms": ["retry_policy"], "expected_sources": ["docs/x"]},
            {"question": "q2", "expected_terms": ["partition_key"]},
        ],
    )
    defaults.update(overrides)
    return getattr(mod, CLS)(**defaults)


def _execute_with_checks(component, resource, run_config=None, instance=None):
    """dg.materialize() doesn't run/report asset checks (confirmed live:
    result.get_asset_check_evaluations() is always empty for it) -- build a full Definitions
    (asset + its asset_check) and execute it as a real job instead, same pattern
    rag_grounding_check's own tests use."""
    @dg.asset(name="kb_base")
    def kb_base():
        return None

    defs = component.build_defs(context=None)
    assets = list(defs.assets) + [kb_base]
    full_defs = dg.Definitions(
        assets=assets, asset_checks=list(defs.asset_checks),
        resources={"search": resource, "io_manager": dg.InMemoryIOManager()},
    )
    job = full_defs.get_implicit_global_asset_job_def()
    return job.execute_in_process(run_config=run_config, instance=instance)


def _materialize(component, resource, run_config=None):
    return _execute_with_checks(component, resource, run_config=run_config)


def test_golden_set_run_reports_eval_summary_and_eval_results(mod):
    resource = _FakeChunksResource({
        "q1": [_hit("c1", "the retry_policy lives here", source="docs/x")],
        "q2": [_hit("c2", "partition_key explained")],
    })
    component = _build(mod)
    result = _materialize(component, resource)
    assert result.success
    md = result.asset_materializations_for_node("kb_eval")[0].metadata
    summary = md["eval_summary"].value
    assert summary["questions"] == 2
    assert summary["score"] == 1.0
    assert summary["hit_rate"] == 1.0  # only q1 has expected_sources, and it hit
    assert summary["passed"] is True
    assert summary["k"] == 5
    results = md["eval_results"].value
    assert len(results) == 2
    assert results[0]["question"] == "q1"
    assert results[0]["hit"] is True
    assert results[1]["hit"] is None  # q2 has no expected_sources


def test_passage_text_never_lands_in_metadata(mod):
    secret = "a very secret internal passage that must never leak"
    resource = _FakeChunksResource({"q1": [_hit("c1", secret, source="docs/x")], "q2": [_hit("c2", secret)]})
    component = _build(mod)
    result = _materialize(component, resource)
    md = result.asset_materializations_for_node("kb_eval")[0].metadata
    serialized = str(md["eval_summary"].value) + str(md["eval_results"].value)
    assert secret not in serialized


def test_quality_check_min_score_is_still_enforced_on_the_very_first_eval(mod):
    """'It passes the first time' (the spec's own words) means the REGRESSION clause can't
    fire yet (nothing to regress against) -- NOT that min_score is skipped. Same precedent as
    rag_eval's own check, which enforces its absolute floor unconditionally and only skips the
    prior-score comparison when there is no prior."""
    resource = _FakeChunksResource({"q1": [_hit("c1", "nothing relevant")], "q2": [_hit("c2", "nothing relevant")]})
    component = _build(mod, min_score=0.9)
    result = _materialize(component, resource)
    assert result.success
    checks = {c.check_name: c for c in result.get_asset_check_evaluations()}
    assert checks["kb_eval_quality_check"].passed is False
    assert "min_score" in checks["kb_eval_quality_check"].metadata["reason"].value


def test_quality_check_passes_on_first_real_eval_when_score_is_fine(mod):
    """The regression clause genuinely can't fire on the first eval (no prior to compare
    against) -- a good score with no history at all passes cleanly."""
    resource = _FakeChunksResource({"q1": [_hit("c1", "retry_policy here")], "q2": [_hit("c2", "partition_key here")]})
    component = _build(mod, min_score=0.6)
    result = _materialize(component, resource)
    assert result.success
    checks = {c.check_name: c for c in result.get_asset_check_evaluations()}
    assert checks["kb_eval_quality_check"].passed is True
    assert "prior_score" not in checks["kb_eval_quality_check"].metadata


def test_quality_check_fails_below_min_score_on_a_later_run(mod):
    """First run establishes a baseline (passes unconditionally); a second run below
    min_score must fail -- requires a real DagsterInstance shared across both runs so
    get_event_records sees the first materialization."""
    with dg.instance_for_test() as instance:
        resource = _FakeChunksResource({"q1": [_hit("c1", "retry_policy here")], "q2": [_hit("c2", "partition_key here")]})
        component = _build(mod, min_score=0.9)

        first = _execute_with_checks(component, resource, instance=instance)
        assert first.success
        first_checks = {c.check_name: c for c in first.get_asset_check_evaluations()}
        assert first_checks["kb_eval_quality_check"].passed is True

        bad_resource = _FakeChunksResource({"q1": [_hit("c1", "nothing relevant")], "q2": [_hit("c2", "nothing relevant")]})
        second = _execute_with_checks(component, bad_resource, instance=instance)
        assert second.success  # the ASSET succeeds; the CHECK fails
        second_checks = {c.check_name: c for c in second.get_asset_check_evaluations()}
        assert second_checks["kb_eval_quality_check"].passed is False


def test_quality_check_regression_skips_an_ad_hoc_question_run_as_the_baseline(mod):
    """A middling, real eval establishes the baseline. An ad hoc {question} run happens in
    between (no eval_summary at all). A THIRD real run that's only a little worse than the
    FIRST real eval (within regression_pct of it) must still pass -- proving the ad hoc run
    was skipped, not mistaken for "no regression possible since the immediately-prior run
    had no score"."""
    with dg.instance_for_test() as instance:
        component = _build(mod, min_score=0.0, regression_pct=10.0)

        good_resource = _FakeChunksResource({
            "q1": [_hit("c1", "retry_policy here", source="docs/x")],
            "q2": [_hit("c2", "partition_key here")],
        })
        first = _execute_with_checks(component, good_resource, instance=instance)
        assert first.success
        assert first.asset_materializations_for_node("kb_eval")[0].metadata["eval_summary"].value["score"] == 1.0

        ad_hoc_resource = _FakeChunksResource({"ad hoc?": [_hit("c9", "whatever")]})
        ad_hoc = _execute_with_checks(
            component, ad_hoc_resource, instance=instance,
            run_config={"ops": {"kb_eval": {"config": {"question": "ad hoc?"}}}},
        )
        assert ad_hoc.success
        ad_hoc_md = ad_hoc.asset_materializations_for_node("kb_eval")[0].metadata
        assert "eval_summary" not in ad_hoc_md
        assert ad_hoc_md["ad_hoc_question"].value == "ad hoc?"

        # Still only 1 of 2 questions correct (score 0.5) -- within 10pp of the first run's 1.0? No:
        # 1.0 - 0.10 = 0.90 floor, 0.5 < 0.90, so THIS specific case should still fail -- use a much
        # smaller shortfall instead to prove the baseline really is the first run, not "no baseline".
        slightly_worse_resource = _FakeChunksResource({
            "q1": [_hit("c1", "retry_policy here", source="docs/x")],  # still correct
            "q2": [_hit("c2", "nothing relevant")],  # now wrong -> score 0.5 overall
        })
        third = _execute_with_checks(component, slightly_worse_resource, instance=instance)
        assert third.success
        third_checks = {c.check_name: c for c in third.get_asset_check_evaluations()}
        # score dropped from 1.0 to 0.5 -- a real regression of 50pp, well past regression_pct=10 --
        # proves the check DID find the first run as its baseline (if it had instead found "no
        # baseline" due to the ad hoc run, it would pass unconditionally; it must fail here).
        assert third_checks["kb_eval_quality_check"].passed is False
        reason = third_checks["kb_eval_quality_check"].metadata["reason"].value
        assert "regression" in reason


def test_ad_hoc_question_override_does_not_touch_golden_set(mod):
    resource = _FakeChunksResource({"custom question": [_hit("c1", "retry_policy here")]})
    component = _build(mod)
    result = _materialize(
        component, resource,
        run_config={"ops": {"kb_eval": {"config": {"question": "custom question"}}}},
    )
    assert result.success
    md = result.asset_materializations_for_node("kb_eval")[0].metadata
    assert "eval_summary" not in md
    assert "eval_results" not in md
    assert md["ad_hoc_question"].value == "custom question"
    ad_hoc_result = md["ad_hoc_result"].value
    assert ad_hoc_result["question"] == "custom question"
    assert ad_hoc_result["score"] == 1.0  # no expected_terms given for an ad hoc question


def test_depends_on_upstream_asset_key(mod):
    component = _build(mod, upstream_asset_key="my_kb_base")
    defs = component.build_defs(context=None)
    asset_def = list(defs.assets)[0]
    deps = {d.to_user_string() for deps in asset_def.asset_deps.values() for d in deps}
    assert "my_kb_base" in deps
