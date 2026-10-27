"""The rate-limit/cooldown state is append-only (one file per event): concurrent claimants never lose an entry and the limit holds
exactly at the boundary. Real files, real threads; nothing faked."""
import json
import threading
from datetime import datetime, timedelta, timezone

from .conftest import load_component_module

mod = load_component_module()
NOW = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)


def _hammer(limiters, n, key_of, max_per_hour=5, cooldown=0.0, now=NOW):
    barrier, out, lock = threading.Barrier(n), [], threading.Lock()

    def go(i):
        barrier.wait()
        v = limiters[i % len(limiters)].admit(key_of(i), f"run-{i}", now, max_per_hour, cooldown)[0]
        with lock:
            out.append(v)

    ts = [threading.Thread(target=go, args=(i,)) for i in range(n)]
    [t.start() for t in ts]
    [t.join() for t in ts]
    return out


def _files(root, kind):
    return sorted((root / "_state" / "ratelimit").glob(f"*-{kind}.json"))


def test_threads_hammering_one_limiter_enforce_the_limit_exactly_and_lose_nothing(tmp_path):
    store = mod._Store(str(tmp_path))
    out = _hammer([mod._RateLimiter(store)], 40, lambda i: f"job|sig{i}", max_per_hour=7)
    assert out.count("ok") == 7 and out.count("rate_limited") == 33
    assert len(_files(tmp_path, "triage")) == 7                      # every winner's entry is on disk, every loser withdrew


def test_separate_limiter_objects_on_one_directory_stay_within_the_limit(tmp_path):
    """Distinct _Store objects over the same directory (the in-process lock is keyed by directory, so it is shared)."""
    stores = [mod._RateLimiter(mod._Store(str(tmp_path))) for _ in range(4)]
    out = _hammer(stores, 40, lambda i: f"job|sig{i}", max_per_hour=9)
    assert out.count("ok") == 9 and len(_files(tmp_path, "triage")) == 9


def test_cross_process_ranking_denies_a_claimant_when_rivals_already_filled_the_window(tmp_path):
    """No in-process lock between processes: simulate rivals by writing their event files directly, as another process would."""
    st = mod._Store(str(tmp_path))
    for i in range(3):
        st.create_exclusive(("_state", "ratelimit", f"{int(NOW.timestamp() * 1000) - 5 - i:013d}-other{i}-triage.json"), "{}")
    lim = mod._RateLimiter(st)
    assert lim.admit("j|s", "me", NOW, 3, 0.0)[0] == "rate_limited"
    assert len(_files(tmp_path, "triage")) == 3                      # the loser withdrew its own file, rivals' files untouched
    assert lim.admit("j|s", "me", NOW, 4, 0.0)[0] == "ok"


def test_exact_boundary_and_window_rolls(tmp_path):
    lim = mod._RateLimiter(mod._Store(str(tmp_path)))
    assert [lim.admit(f"j|{i}", f"r{i}", NOW, 3, 0.0)[0] for i in range(4)] == ["ok", "ok", "ok", "rate_limited"]
    later = NOW + timedelta(hours=1, seconds=1)
    assert lim.admit("j|x", "r9", later, 3, 0.0)[0] == "ok"           # the hour passed
    assert len(_files(tmp_path, "triage")) == 1                       # expired events were compacted away


def test_cooldown_exactly_one_winner_per_key_under_concurrency(tmp_path):
    store = mod._Store(str(tmp_path))
    out = _hammer([mod._RateLimiter(store)], 30, lambda i: "job|same", max_per_hour=100, cooldown=60.0)
    assert out.count("ok") == 1 and out.count("cooldown") == 29
    assert len(_files(tmp_path, "cooldown")) == 1 and len(_files(tmp_path, "triage")) == 1
    assert mod._RateLimiter(store).suppressed_count("job|same") == 29
    after = NOW + timedelta(minutes=61)
    assert mod._RateLimiter(store).admit("job|same", "late", after, 100, 60.0)[0] == "ok"
    assert len(_files(tmp_path, "cooldown")) == 1                     # the expired cooldown was compacted away


def test_works_on_an_fsspec_uri_too():
    st = mod._Store("memory://rl_test_dir")
    lim = mod._RateLimiter(st)
    assert [lim.admit(f"j|{i}", f"r{i}", NOW, 2, 0.0)[0] for i in range(3)] == ["ok", "ok", "rate_limited"]
    assert lim.triage_count(NOW) == 2


def test_verify_step_catches_rivals_that_landed_between_my_write_and_my_listing(tmp_path):
    """The cross-process case: rivals pass their pre-check at the same time, so the post-write ranking must deny the extra one."""
    st = mod._Store(str(tmp_path))
    lim = mod._RateLimiter(st)
    real = lim._append

    def racing(kind, run_id, now, payload):
        name = real(kind, run_id, now, payload)
        if kind == "triage":
            for i in range(3):                                  # three rivals complete their claims right after mine
                st.create_exclusive(("_state", "ratelimit", f"{int(NOW.timestamp() * 1000) - 1 - i:013d}-rival{i}-triage.json"), "{}")
        return name

    lim._append = racing
    assert lim.admit("j|s", "me", NOW, 3, 0.0)[0] == "rate_limited"
    assert len(_files(tmp_path, "triage")) == 3 and not any("-me-" in f.name for f in _files(tmp_path, "triage"))
