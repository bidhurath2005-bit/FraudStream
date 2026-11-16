"""Deterministic synthetic transaction generator.

All output is SYNTHETIC (``synthetic=True``). Given the same ``seed`` and ``start_time``
the generator yields byte-identical events. Suspicious patterns are injected as
*scenarios* (multi-event sequences). The scenario name is stored only in
``ground_truth`` and must never be used as a model feature.
"""
from __future__ import annotations

import hashlib
import heapq
import itertools
import math
import random
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Literal

from simulator.geography import CITIES, haversine_km
from streaming.schemas.events import (
    HIGH_RISK_MERCHANT_CATEGORIES,
    MERCHANT_CATEGORIES,
    SCHEMA_VERSION,
    GeoLocation,
    GroundTruth,
    TransactionEvent,
)

SimulationMode = Literal["normal", "fraud", "mixed", "high_volume", "custom"]

SCENARIOS: tuple[str, ...] = (
    "large_amount", "velocity_burst", "failed_attempts", "account_takeover",
    "impossible_travel", "odd_hour", "suspicious_merchant",
)


_EVERYDAY = ("grocery", "restaurants", "fuel", "utilities", "retail", "online_services",
             "entertainment", "healthcare", "electronics", "travel")
_NAME_A = ("North", "Blue", "Silver", "Urban", "Prime", "Metro", "Green", "Crown", "Summit", "Harbor",
           "Golden", "Bright", "Atlas", "Nova", "Vertex")
_NAME_B = {"grocery": "Market", "restaurants": "Kitchen", "fuel": "Fuel", "utilities": "Energy",
           "retail": "Store", "electronics": "Electronics", "travel": "Travel", "entertainment": "Cinema",
           "healthcare": "Pharmacy", "online_services": "Digital", "gift_cards": "Gift Cards",
           "crypto_exchange": "Crypto", "gambling": "Casino", "money_transfer": "Remit"}


@dataclass
class SimulationConfig:
    mode: SimulationMode = "mixed"
    tps: float = 10.0
    duration_seconds: float | None = 60.0
    max_events: int | None = None
    customer_count: int = 500
    suspicious_pct: float = 5.0
    avg_amount: float = 60.0
    scenarios: list[str] | None = None
    seed: int | None = 42
    currency: str = "USD"
    # Customer/merchant identity. With an int, each customer's profile (home city, devices,
    # spending level, merchants) depends only on (population_seed, customer index): the same
    # customer id is the same synthetic person across runs, seeds and customer counts.
    # None = LEGACY mode (population derived from `seed` + customer_count); used only to
    # reproduce dataset synthetic-fraudstream v1.0 byte-for-byte.
    population_seed: int | None = 1000

    def __post_init__(self) -> None:
        if self.tps <= 0 or self.tps > 5000:
            raise ValueError("tps must be in (0, 5000]")
        if not 1 <= self.customer_count <= 1_000_000:
            raise ValueError("customer_count must be in [1, 1_000_000]")
        if not 0 <= self.suspicious_pct <= 100:
            raise ValueError("suspicious_pct must be in [0, 100]")
        if self.avg_amount <= 0:
            raise ValueError("avg_amount must be > 0")
        if self.scenarios:
            unknown = set(self.scenarios) - set(SCENARIOS)
            if unknown:
                raise ValueError(f"unknown scenarios: {sorted(unknown)}")

    @property
    def effective_suspicious_pct(self) -> float:
        if self.mode == "normal":
            return 0.0
        if self.mode == "fraud":
            return max(40.0, self.suspicious_pct)
        return self.suspicious_pct

    @property
    def active_scenarios(self) -> tuple[str, ...]:
        if self.mode == "custom" and self.scenarios:
            return tuple(self.scenarios)
        return SCENARIOS


@dataclass
class Merchant:
    merchant_id: str
    name: str
    category: str


@dataclass
class CustomerProfile:
    customer_id: str
    account_id: str
    home_city: tuple[str, str, float, float]
    typical_amount: float
    devices: list[str]
    ips: list[str]
    favourite_merchants: list[Merchant]
    channels: list[str]
    channel_weights: list[float]
    activity_weight: float
    last_city: tuple[str, str, float, float] | None = None


@dataclass(order=True)
class _Pending:
    when: datetime
    seq: int
    event: TransactionEvent = field(compare=False)


class TransactionGenerator:
    """Produces an event-time-ordered stream of synthetic transactions."""

    def __init__(self, config: SimulationConfig, start_time: datetime | None = None) -> None:
        self.config = config
        self.start_time = (start_time or datetime.now(timezone.utc)).astimezone(timezone.utc)
        seed = config.seed if config.seed is not None else random.SystemRandom().randrange(2**31)
        self.seed = seed
        self._legacy_population = config.population_seed is None
        # LEGACY: one RNG for the whole population, coupled to the traffic seed (dataset v1.0)
        self._world = random.Random(f"world:{seed}:{config.customer_count}")
        self._rng = random.Random(f"stream:{seed}:{self.start_time.isoformat()}")
        ns_hash = hashlib.sha256(f"{seed}|{self.start_time.isoformat()}".encode()).hexdigest()
        self._namespace = uuid.UUID(ns_hash[:32])
        self._counter = itertools.count()
        self._seq = itertools.count()
        self.merchants = self._build_merchants()
        self.customers = self._build_customers()
        self._weights = list(itertools.accumulate(c.activity_weight for c in self.customers))
        self._pending: list[_Pending] = []
        self.stats = {"events": 0, "suspicious_events": 0, "scenarios_started": 0}

    # ------------------------------------------------------------------ population
    def _build_merchants(self) -> dict[str, list[Merchant]]:
        out: dict[str, list[Merchant]] = {}
        rng = self._world if self._legacy_population else random.Random(f"merchants:{self.config.population_seed}")
        for ci, cat in enumerate(MERCHANT_CATEGORIES):
            n = 25 if cat in _EVERYDAY else 8
            out[cat] = [
                Merchant(f"M{ci:02d}{i:04d}", f"{rng.choice(_NAME_A)} {_NAME_B[cat]} #{i}", cat)
                for i in range(n)
            ]
        return out

    def _build_customers(self) -> list[CustomerProfile]:
        customers = []
        for i in range(self.config.customer_count):
            # stable identity: per-customer RNG (independent of seed and customer_count)
            w = self._world if self._legacy_population else random.Random(f"customer:{self.config.population_seed}:{i}")
            city = w.choice(CITIES)
            n_dev = 1 if w.random() < 0.7 else 2
            cats = w.sample(_EVERYDAY, k=4)
            favs = [w.choice(self.merchants[c]) for c in cats for _ in range(2)]
            channels = ["CARD_PRESENT", "ONLINE", "MOBILE_APP", "ATM", "BANK_TRANSFER"]
            cw = [w.uniform(0.5, 4), w.uniform(0.5, 3), w.uniform(0.2, 3), 0.25, 0.3]
            customers.append(CustomerProfile(
                customer_id=f"C{i:06d}",
                account_id=f"A{i:06d}{w.randrange(10)}",
                home_city=city,
                typical_amount=self.config.avg_amount * math.exp(w.gauss(0, 0.45)),
                devices=[f"dev-{i:06d}-{k}" for k in range(n_dev)],
                ips=[f"198.51.100.{w.randrange(1, 255)}" for _ in range(n_dev)],
                favourite_merchants=favs,
                channels=channels,
                channel_weights=cw,
                activity_weight=w.paretovariate(2.0),
            ))
        return customers

    # ------------------------------------------------------------------ helpers
    def _new_ids(self) -> tuple[str, str]:
        n = next(self._counter)
        ev = uuid.uuid5(self._namespace, f"event:{n}")
        tx = uuid.uuid5(self._namespace, f"tx:{n}")
        return str(ev), "TX" + tx.hex[:20].upper()

    def _pick_customer(self) -> CustomerProfile:
        r = self._rng.random() * self._weights[-1]
        lo, hi = 0, len(self._weights) - 1
        while lo < hi:
            mid = (lo + hi) // 2
            if self._weights[mid] < r:
                lo = mid + 1
            else:
                hi = mid
        return self.customers[lo]

    def _amount(self, base: float, sigma: float = 0.55) -> float:
        return round(max(1.0, base * math.exp(self._rng.gauss(0, sigma))), 2)

    def _event(self, c: CustomerProfile, when: datetime, *, amount: float, merchant: Merchant,
               channel: str, device: str, ip: str, city: tuple[str, str, float, float],
               status: str = "APPROVED", tx_type: str | None = None,
               suspicious: bool, scenario: str) -> TransactionEvent:
        event_id, tx_id = self._new_ids()
        if tx_type is None:
            tx_type = {"ATM": "WITHDRAWAL", "BANK_TRANSFER": "TRANSFER"}.get(channel, "PURCHASE")
            if merchant.category == "money_transfer":
                tx_type = "TRANSFER"
            elif merchant.category == "utilities":
                tx_type = "PAYMENT"
        return TransactionEvent(
            event_id=event_id, transaction_id=tx_id, customer_id=c.customer_id,
            account_id=c.account_id, merchant_id=merchant.merchant_id, merchant_name=merchant.name,
            amount=round(amount, 2), currency=self.config.currency, event_time=when,
            transaction_type=tx_type, merchant_category=merchant.category, channel=channel,
            device_id=device, ip_address=ip,
            location=GeoLocation(city=city[0], country=city[1], lat=city[2], lon=city[3], simulated=True),
            status=status, schema_version=SCHEMA_VERSION, synthetic=True, source="simulator",
            ground_truth=GroundTruth(is_suspicious=suspicious, scenario=scenario),
        )

    def _schedule(self, ev: TransactionEvent) -> None:
        heapq.heappush(self._pending, _Pending(ev.event_time, next(self._seq), ev))

    # ------------------------------------------------------------------ behaviour
    def _normal(self, c: CustomerProfile, when: datetime) -> None:
        r = self._rng
        channel = r.choices(c.channels, weights=c.channel_weights)[0]
        merchant = r.choice(c.favourite_merchants) if r.random() < 0.8 else \
            r.choice(self.merchants[r.choice(_EVERYDAY)])
        amount = self._amount(c.typical_amount)
        scenario = "normal"
        if r.random() < 0.015:                      # legitimate occasional big purchase
            amount = self._amount(c.typical_amount * r.uniform(3, 9), 0.2)
            scenario = "normal_large_purchase"
        device, ip, city = c.devices[0], c.ips[0], c.home_city
        if len(c.devices) > 1 and r.random() < 0.3:
            device, ip = c.devices[1], c.ips[1]
        if r.random() < 0.01:                       # legitimate travel (slow, plausible)
            city = r.choice(CITIES)
            scenario = "normal_travel"
        if r.random() < 0.004:                      # legitimate device change (new phone)
            device = f"dev-{c.customer_id[1:]}-{len(c.devices)}"
            ip = f"198.51.100.{r.randrange(1, 255)}"
            c.devices.insert(0, device)
            c.ips.insert(0, ip)
            scenario = "normal_new_device"
        status = "APPROVED"
        if r.random() < 0.02:
            status = r.choice(["DECLINED", "FAILED"])
        self._schedule(self._event(c, when, amount=amount, merchant=merchant, channel=channel,
                                   device=device, ip=ip, city=city, status=status,
                                   suspicious=False, scenario=scenario))
        if r.random() < 0.012:                      # legitimate rapid follow-ups (split basket, retries)
            for k in range(r.randint(1, 4)):
                self._schedule(self._event(c, when + timedelta(seconds=r.uniform(5, 240)),
                                           amount=self._amount(c.typical_amount * 0.6), merchant=merchant,
                                           channel=channel, device=device, ip=ip, city=city,
                                           status="DECLINED" if r.random() < 0.15 else "APPROVED",
                                           suspicious=False, scenario="normal_multi_purchase"))

    def _fraud_device(self) -> tuple[str, str]:
        r = self._rng
        return f"dev-x{r.getrandbits(32):08x}", f"203.0.113.{r.randrange(1, 255)}"

    def _far_city(self, home: tuple[str, str, float, float]) -> tuple[str, str, float, float]:
        far = [ct for ct in CITIES if haversine_km(home[2], home[3], ct[2], ct[3]) > 3000]
        return self._rng.choice(far)

    def _scenario(self, name: str, c: CustomerProfile, t0: datetime) -> None:
        r = self._rng
        s = lambda sec: t0 + timedelta(seconds=sec)  # noqa: E731
        hr = lambda: r.choice(self.merchants[r.choice(sorted(HIGH_RISK_MERCHANT_CATEGORIES))])  # noqa: E731
        if name == "odd_hour" and not (0 <= t0.hour <= 5):
            name = "large_amount"
        self.stats["scenarios_started"] += 1
        if name == "large_amount":
            dev, ip = self._fraud_device() if r.random() < 0.6 else (c.devices[0], c.ips[0])
            m = r.choice(self.merchants[r.choice(["electronics", "travel", "money_transfer", "retail"])])
            self._schedule(self._event(c, t0, amount=self._amount(c.typical_amount * r.uniform(5, 40), 0.3),
                                       merchant=m, channel="ONLINE", device=dev, ip=ip, city=c.home_city,
                                       suspicious=True, scenario=name))
        elif name == "velocity_burst":
            dev, ip = self._fraud_device()
            n = r.randint(6, 14)
            offs = sorted(r.uniform(0, r.uniform(30, 150)) for _ in range(n))
            for o in offs:
                m = r.choice(self.merchants[r.choice(["online_services", "gift_cards", "retail"])])
                st = "DECLINED" if r.random() < 0.3 else "APPROVED"
                self._schedule(self._event(c, s(o), amount=round(r.uniform(1, 25), 2), merchant=m,
                                           channel="ONLINE", device=dev, ip=ip, city=c.home_city,
                                           status=st, suspicious=True, scenario=name))
        elif name == "failed_attempts":
            dev, ip = self._fraud_device()
            n = r.randint(4, 7)
            m = r.choice(self.merchants[r.choice(["electronics", "gift_cards", "online_services"])])
            for k in range(n):
                self._schedule(self._event(c, s(k * r.uniform(8, 35)), amount=self._amount(c.typical_amount * 4, 0.3),
                                           merchant=m, channel="ONLINE", device=dev, ip=ip, city=c.home_city,
                                           status=r.choice(["FAILED", "DECLINED"]), suspicious=True, scenario=name))
            self._schedule(self._event(c, s(n * 40), amount=self._amount(c.typical_amount * 5, 0.3), merchant=m,
                                       channel="ONLINE", device=dev, ip=ip, city=c.home_city,
                                       suspicious=True, scenario=name))
        elif name == "account_takeover":
            dev, ip = self._fraud_device()
            city = self._far_city(c.home_city)
            for k in range(r.randint(2, 4)):
                m = r.choice(self.merchants[r.choice(["electronics", "money_transfer", "gift_cards"])])
                self._schedule(self._event(c, s(k * r.uniform(30, 180)),
                                           amount=self._amount(c.typical_amount * r.uniform(5, 15), 0.25),
                                           merchant=m, channel=r.choice(["ONLINE", "MOBILE_APP"]), device=dev,
                                           ip=ip, city=city, suspicious=True, scenario=name))
        elif name == "impossible_travel":
            m = r.choice(c.favourite_merchants)
            self._schedule(self._event(c, t0, amount=self._amount(c.typical_amount), merchant=m,
                                       channel="CARD_PRESENT", device=c.devices[0], ip=c.ips[0], city=c.home_city,
                                       suspicious=False, scenario="impossible_travel_anchor"))
            city = self._far_city(c.home_city)
            m2 = r.choice(self.merchants[r.choice(["retail", "electronics", "travel"])])
            self._schedule(self._event(c, s(r.uniform(120, 1200)),
                                       amount=self._amount(c.typical_amount * r.uniform(2, 8), 0.3), merchant=m2,
                                       channel="CARD_PRESENT", device=c.devices[0], ip=c.ips[0], city=city,
                                       suspicious=True, scenario=name))
        elif name == "odd_hour":
            dev, ip = self._fraud_device() if r.random() < 0.5 else (c.devices[0], c.ips[0])
            m = r.choice(self.merchants[r.choice(["electronics", "money_transfer", "gambling"])])
            self._schedule(self._event(c, t0, amount=self._amount(c.typical_amount * r.uniform(5, 15), 0.25),
                                       merchant=m, channel="ONLINE", device=dev, ip=ip, city=c.home_city,
                                       suspicious=True, scenario=name))
        elif name == "suspicious_merchant":
            for k in range(r.randint(2, 3)):
                self._schedule(self._event(c, s(k * r.uniform(20, 240)),
                                           amount=self._amount(c.typical_amount * r.uniform(1, 10), 0.4),
                                           merchant=hr(), channel=r.choice(["ONLINE", "MOBILE_APP"]),
                                           device=c.devices[0], ip=c.ips[0], city=c.home_city,
                                           suspicious=True, scenario=name))
        else:  # pragma: no cover - guarded by config validation
            raise ValueError(name)

    # ------------------------------------------------------------------ public API
    def inject(self, scenario: str, customer_id: str | None = None, when: datetime | None = None,
               align: Literal["end", "start"] = "end") -> list[TransactionEvent]:
        """Generate one scenario immediately (Simulation Lab 'inject').

        align="end" (default): the scenario is placed so its LAST event happens at ``when``
        (default now) - no future-dated events. Emitting future timestamps would advance
        event-time watermarks in the streaming layer and cause genuine real-time events to
        be dropped as late. align="start": the first event happens at ``when`` (used by tests
        that append a scenario after a generated stream)."""
        if scenario not in SCENARIOS:
            raise ValueError(f"unknown scenario '{scenario}'")
        c = next((x for x in self.customers if x.customer_id == customer_id), None) if customer_id else None
        c = c or self._pick_customer()
        anchor = (when or datetime.now(timezone.utc)).astimezone(timezone.utc)
        self._scenario(scenario, c, anchor)
        out = []
        while self._pending:
            out.append(heapq.heappop(self._pending).event)
        out.sort(key=lambda e: e.event_time)
        if align == "end" and out:
            shift = out[-1].event_time - anchor
            if shift > timedelta(0):
                out = [e.model_copy(update={"event_time": e.event_time - shift}) for e in out]
        self.stats["events"] += len(out)
        self.stats["suspicious_events"] += sum(1 for e in out if e.ground_truth and e.ground_truth.is_suspicious)
        return out

    def events(self) -> Iterator[TransactionEvent]:
        """Infinite (or bounded by duration/max_events) event-time-ordered stream."""
        cfg = self.config
        clock = self.start_time
        end = self.start_time + timedelta(seconds=cfg.duration_seconds) if cfg.duration_seconds else None
        p_susp = cfg.effective_suspicious_pct / 100.0
        scenarios = cfg.active_scenarios
        emitted = 0
        while True:
            clock = clock + timedelta(seconds=self._rng.expovariate(cfg.tps))
            if end is not None and clock > end:
                break
            c = self._pick_customer()
            if p_susp > 0 and self._rng.random() < p_susp:
                self._scenario(self._rng.choice(scenarios), c, clock)
            else:
                self._normal(c, clock)
            while self._pending and self._pending[0].when <= clock:
                ev = heapq.heappop(self._pending).event
                yield from self._emit(ev)
                emitted += 1
                if cfg.max_events is not None and emitted >= cfg.max_events:
                    return
        while self._pending:  # flush scheduled scenario tails
            if end is not None and self._pending[0].when > end:
                break
            yield from self._emit(heapq.heappop(self._pending).event)
            emitted += 1
            if cfg.max_events is not None and emitted >= cfg.max_events:
                return

    def _emit(self, ev: TransactionEvent) -> Iterator[TransactionEvent]:
        self.stats["events"] += 1
        if ev.ground_truth and ev.ground_truth.is_suspicious:
            self.stats["suspicious_events"] += 1
        yield ev
