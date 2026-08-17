"""
simulator.py
LogiHive Q-Commerce :: Stress Test Simulator
-----------------------------------------------
Continuously floods the running FastAPI service with synthetic
RiderTelemetry and DisruptionAlert payloads to validate system
stability, backpressure handling, and Redis/LangGraph throughput
under sustained concurrent load.

Usage:
    python simulator.py --host http://localhost:8000 --concurrency 50 --duration 120
    python simulator.py --duration 0   # run until Ctrl+C
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import random
import signal
import time
from dataclasses import dataclass, field
from typing import Any

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("logihive.simulator")

DARK_STORES = ["DS_BANDRA", "DS_POWAI", "DS_LOWER_PAREL", "DS_MULUND", "DS_VASHI"]
RIDER_IDS = [f"RIDER_{i:04d}" for i in range(1, 501)]
DISRUPTION_CATEGORIES = [
    "flood", "traffic_jam", "vehicle_breakdown", "protest",
    "power_outage", "rider_shortage", "stockout",
]
ALERT_TEXT_TEMPLATES = [
    "Heavy waterlogging reported near {area}, riders unable to cross junction.",
    "Sudden protest march blocking main arterial road near {area}.",
    "Dark store {area} reporting cold-chain power outage, perishables at risk.",
    "Multiple rider breakdowns clustered around {area} in last 15 minutes.",
    "Unusually high order surge detected at {area}, inventory depleting fast.",
]


@dataclass
class SimulatorStats:
    total_requests: int = 0
    total_success: int = 0
    total_errors: int = 0
    status_codes: dict[int, int] = field(default_factory=dict)
    latencies_ms: list[float] = field(default_factory=list)
    start_time: float = field(default_factory=time.monotonic)

    def record(self, status_code: int, latency_ms: float, success: bool) -> None:
        self.total_requests += 1
        self.status_codes[status_code] = self.status_codes.get(status_code, 0) + 1
        self.latencies_ms.append(latency_ms)
        if success:
            self.total_success += 1
        else:
            self.total_errors += 1

    def summary(self) -> str:
        elapsed = max(time.monotonic() - self.start_time, 1e-6)
        rps = self.total_requests / elapsed
        p50 = p95 = p99 = 0.0
        if self.latencies_ms:
            sorted_lat = sorted(self.latencies_ms)
            n = len(sorted_lat)
            p50 = sorted_lat[max(int(n * 0.50) - 1, 0)]
            p95 = sorted_lat[max(int(n * 0.95) - 1, 0)]
            p99 = sorted_lat[max(int(n * 0.99) - 1, 0)]
        return (
            "\n--- LogiHive Stress Test Summary ---\n"
            f"Elapsed:        {elapsed:.1f}s\n"
            f"Total requests: {self.total_requests}\n"
            f"Success:        {self.total_success}\n"
            f"Errors:         {self.total_errors}\n"
            f"Throughput:     {rps:.2f} req/s\n"
            f"Latency p50/p95/p99 (ms): {p50:.1f} / {p95:.1f} / {p99:.1f}\n"
            f"Status codes:   {self.status_codes}\n"
        )


def _random_telemetry_payload() -> dict[str, Any]:
    return {
        "rider_id": random.choice(RIDER_IDS),
        "dark_store_id": random.choice(DARK_STORES),
        "lat": round(19.0 + random.random() * 0.3, 6),
        "lon": round(72.8 + random.random() * 0.3, 6),
        "speed_kmph": round(random.uniform(0, 45), 1),
        "battery_pct": round(random.uniform(5, 100), 1),
        "active_order_id": f"ORD_{random.randint(100000, 999999)}",
        "timestamp": time.time(),
    }


def _random_alert_payload() -> dict[str, Any]:
    area = random.choice(DARK_STORES)
    template = random.choice(ALERT_TEXT_TEMPLATES)
    return {
        "alert_id": f"ALERT_{random.randint(100000, 999999)}",
        "category": random.choice(DISRUPTION_CATEGORIES),
        "affected_area": area,
        "raw_text": template.format(area=area),
        "reported_at": time.time(),
        "severity_hint": random.choice(["low", "medium", "high", "critical"]),
    }


class LoadSimulator:
    def __init__(
        self,
        host: str,
        concurrency: int,
        telemetry_ratio: float = 0.85,
        request_timeout_s: float = 10.0,
    ) -> None:
        self.host = host.rstrip("/")
        self.concurrency = concurrency
        self.telemetry_ratio = telemetry_ratio
        self.request_timeout_s = request_timeout_s
        self.stats = SimulatorStats()
        self._stop_event = asyncio.Event()

    async def _fire_request(self, client: httpx.AsyncClient) -> None:
        is_telemetry = random.random() < self.telemetry_ratio
        endpoint = "/api/telemetry" if is_telemetry else "/api/alert"
        payload = _random_telemetry_payload() if is_telemetry else _random_alert_payload()

        started = time.monotonic()
        try:
            resp = await client.post(f"{self.host}{endpoint}", json=payload)
            latency_ms = (time.monotonic() - started) * 1000
            success = 200 <= resp.status_code < 300
            self.stats.record(resp.status_code, latency_ms, success)
            if not success:
                logger.warning("%s -> HTTP %s: %.100s", endpoint, resp.status_code, resp.text)
        except httpx.TimeoutException:
            latency_ms = (time.monotonic() - started) * 1000
            self.stats.record(0, latency_ms, success=False)
            logger.error("%s -> TIMEOUT after %.0fms", endpoint, latency_ms)
        except httpx.HTTPError as exc:
            latency_ms = (time.monotonic() - started) * 1000
            self.stats.record(0, latency_ms, success=False)
            logger.error("%s -> connection error: %s", endpoint, exc)

    async def _worker(self, worker_id: int, client: httpx.AsyncClient) -> None:
        while not self._stop_event.is_set():
            await self._fire_request(client)
            # small jitter so workers don't march in lockstep and create
            # artificial thundering-herd spikes
            await asyncio.sleep(random.uniform(0.01, 0.15))
        logger.debug("Worker %d shutting down", worker_id)

    async def _periodic_reporter(self, interval_s: float = 10.0) -> None:
        try:
            while not self._stop_event.is_set():
                await asyncio.sleep(interval_s)
                logger.info(
                    "Progress: %d req sent | %d success | %d errors",
                    self.stats.total_requests,
                    self.stats.total_success,
                    self.stats.total_errors,
                )
        except asyncio.CancelledError:
            pass

    async def run(self, duration_s: float | None) -> None:
        limits = httpx.Limits(
            max_connections=self.concurrency * 2,
            max_keepalive_connections=self.concurrency,
        )
        timeout = httpx.Timeout(self.request_timeout_s)

        async with httpx.AsyncClient(limits=limits, timeout=timeout) as client:
            workers = [
                asyncio.create_task(self._worker(i, client))
                for i in range(self.concurrency)
            ]
            reporter = asyncio.create_task(self._periodic_reporter())

            if duration_s is not None:
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=duration_s)
                except asyncio.TimeoutError:
                    pass
            else:
                await self._stop_event.wait()

            self._stop_event.set()
            reporter.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

    def stop(self) -> None:
        self._stop_event.set()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="LogiHive Q-Commerce stress simulator")
    parser.add_argument("--host", default="http://localhost:8000", help="Target FastAPI base URL")
    parser.add_argument("--concurrency", type=int, default=25, help="Concurrent worker coroutines")
    parser.add_argument("--duration", type=float, default=60.0, help="Run duration in seconds (0 = run until Ctrl+C)")
    parser.add_argument("--telemetry-ratio", type=float, default=0.85, help="Fraction of requests hitting /api/telemetry vs /api/alert")
    return parser.parse_args()


async def _main() -> None:
    args = _parse_args()
    sim = LoadSimulator(
        host=args.host,
        concurrency=args.concurrency,
        telemetry_ratio=args.telemetry_ratio,
    )

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, sim.stop)
        except NotImplementedError:
            # Windows event loops don't support add_signal_handler for these signals
            pass

    duration = None if args.duration <= 0 else args.duration
    logger.info(
        "Starting stress test: host=%s concurrency=%d duration=%s",
        args.host, args.concurrency, duration or "until interrupted",
    )

    await sim.run(duration_s=duration)
    logger.info(sim.stats.summary())


if __name__ == "__main__":
    asyncio.run(_main())