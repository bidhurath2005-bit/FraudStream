from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone

import pytest

from simulator.generator import SCENARIOS, SimulationConfig, TransactionGenerator
from streaming.schemas.events import TransactionEvent, parse_event

T0 = datetime(2026, 3, 1, 12, 0, tzinfo=timezone.utc)


def _events(**kw):
    cfg = SimulationConfig(**{"tps": 40, "duration_seconds": 60, "customer_count": 300, "seed": 11, **kw})
    return list(TransactionGenerator(cfg, T0).events())


def test_same_seed_and_start_is_deterministic():
    a, b = _events(), _events()
    assert [e.to_wire() for e in a] == [e.to_wire() for e in b]


def test_different_seed_changes_output_and_ids_are_unique():
    a, b = _events(seed=1), _events(seed=2)
    assert [e.amount for e in a[:50]] != [e.amount for e in b[:50]]
    ids = [e.event_id for e in a]
    assert len(ids) == len(set(ids))
    assert len({e.transaction_id for e in a}) == len(a)


def test_different_start_time_gives_fresh_ids_same_population():
    a = _events()
    cfg = SimulationConfig(tps=40, duration_seconds=60, customer_count=300, seed=11)
    b = list(TransactionGenerator(cfg, T0.replace(hour=13)).events())
    assert not {e.event_id for e in a} & {e.event_id for e in b}
    ga, gb = TransactionGenerator(cfg, T0), TransactionGenerator(cfg, T0.replace(hour=13))
    assert [c.customer_id for c in ga.customers] == [c.customer_id for c in gb.customers]
    assert [c.home_city for c in ga.customers] == [c.home_city for c in gb.customers]


def test_events_are_valid_synthetic_and_time_ordered():
    evs = _events(suspicious_pct=10)
    assert evs
    for e in evs:
        assert e.synthetic is True and e.location.simulated is True
        assert parse_event(e.to_wire()).event_id == e.event_id  # round-trips through the wire schema
    assert all(x.event_time <= y.event_time for x, y in zip(evs, evs[1:], strict=False))
    assert all(T0 <= e.event_time for e in evs)


def test_normal_mode_has_no_suspicious_events():
    evs = _events(mode="normal", suspicious_pct=50)
    assert not any(e.ground_truth.is_suspicious for e in evs)


def test_fraud_mode_has_many_suspicious_events():
    evs = _events(mode="fraud")
    share = sum(e.ground_truth.is_suspicious for e in evs) / len(evs)
    assert share > 0.3


def test_custom_mode_only_selected_scenarios():
    evs = _events(mode="custom", scenarios=["velocity_burst"], suspicious_pct=30)
    scen = Counter(e.ground_truth.scenario for e in evs if e.ground_truth.is_suspicious)
    assert set(scen) == {"velocity_burst"}


def test_tps_controls_volume_roughly():
    n = len(_events(mode="normal", tps=50, duration_seconds=100))
    assert 4000 < n < 6500  # Poisson arrivals + multi-purchase follow-ups


def test_max_events_bounds_stream():
    assert len(_events(max_events=37)) == 37


@pytest.mark.parametrize("scenario", SCENARIOS)
def test_inject_every_scenario(scenario):
    gen = TransactionGenerator(SimulationConfig(customer_count=50, seed=3), T0)
    evs = gen.inject(scenario, customer_id="C000007", when=T0.replace(hour=3))
    assert evs and all(isinstance(e, TransactionEvent) for e in evs)
    assert all(e.customer_id == "C000007" for e in evs)
    assert any(e.ground_truth.is_suspicious for e in evs)


@pytest.mark.parametrize("bad", [{"tps": 0}, {"customer_count": 0}, {"suspicious_pct": 101},
                                 {"avg_amount": -1}, {"scenarios": ["nope"]}])
def test_config_validation(bad):
    with pytest.raises(ValueError):
        SimulationConfig(**bad)


def test_schema_rejects_invalid_payloads():
    good = _events(max_events=1)[0].to_wire()
    from streaming.schemas.events import EventValidationError

    for mutate in ({"amount": -5}, {"currency": "usd"}, {"merchant_category": "weapons"},
                   {"event_time": "2026-01-01T00:00:00"}, {"unexpected": 1}):
        with pytest.raises(EventValidationError):
            parse_event({**good, **mutate})
    with pytest.raises(EventValidationError):
        parse_event(b"{not json")


def _profile(c):
    return (c.home_city, c.devices, round(c.typical_amount, 6), [m.merchant_id for m in c.favourite_merchants])


def test_customer_identity_is_stable_across_seeds_and_population_sizes():
    """Regression (Phase 8): the same customer id must be the same synthetic person in every run;
    otherwise persisted history makes normal purchases look like impossible travel."""
    a = TransactionGenerator(SimulationConfig(seed=1, customer_count=300), T0)
    b = TransactionGenerator(SimulationConfig(seed=999, customer_count=300), T0.replace(day=9))
    c = TransactionGenerator(SimulationConfig(seed=5, customer_count=5000), T0)
    for i in (0, 17, 299):
        assert _profile(a.customers[i]) == _profile(b.customers[i]) == _profile(c.customers[i])
    other = TransactionGenerator(SimulationConfig(seed=1, customer_count=300, population_seed=7), T0)
    assert [_profile(x) for x in other.customers[:20]] != [_profile(x) for x in a.customers[:20]]


def test_traffic_seed_still_changes_the_event_stream_but_not_customers():
    e1 = _events(seed=1)
    e2 = _events(seed=2)
    assert [e.amount for e in e1[:30]] != [e.amount for e in e2[:30]]
    home = {c.customer_id: c.home_city[0] for c in TransactionGenerator(SimulationConfig(customer_count=300, seed=1), T0).customers}
    normal = [e for e in e1 + e2 if e.ground_truth.scenario == "normal"]
    assert sum(e.location.city == home[e.customer_id] for e in normal) / len(normal) > 0.95


def test_legacy_population_mode_couples_population_to_seed():
    a = TransactionGenerator(SimulationConfig(seed=1, customer_count=100, population_seed=None), T0)
    b = TransactionGenerator(SimulationConfig(seed=2, customer_count=100, population_seed=None), T0)
    assert [_profile(x) for x in a.customers[:20]] != [_profile(x) for x in b.customers[:20]]


@pytest.mark.parametrize("scenario", ["account_takeover", "velocity_burst", "impossible_travel", "failed_attempts"])
def test_inject_is_never_future_dated_by_default(scenario):
    """Regression (Phase 9): future-dated injected events advance event-time watermarks and make
    genuine real-time events 'late'. Default injection ends exactly at `when`."""
    gen = TransactionGenerator(SimulationConfig(customer_count=50, seed=3), T0)
    evs = gen.inject(scenario, when=T0)
    assert evs[-1].event_time == T0 and all(e.event_time <= T0 for e in evs)
    spans = [(b.event_time - a.event_time) for a, b in zip(evs, evs[1:], strict=False)]
    gen2 = TransactionGenerator(SimulationConfig(customer_count=50, seed=3), T0)
    evs2 = gen2.inject(scenario, when=T0, align="start")
    assert evs2[0].event_time >= T0 and all(e.event_time >= T0 for e in evs2)   # scenario starts at `when`
    assert spans == [(b.event_time - a.event_time) for a, b in zip(evs2, evs2[1:], strict=False)]  # spacing kept
