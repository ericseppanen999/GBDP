from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import requests

from gbdp.bronze.cache import ResponseCache
from gbdp.bronze.writer import BronzeWriter, RawPayload
from gbdp.bronze.request_log import write_request_log
from gbdp.utils.io import request_hash, sha256_bytes
from gbdp.utils.logging import get_logger
from gbdp.utils.time import utc_now

logger = get_logger("gbdp.connectors")


@dataclass(frozen=True)
class Partition:
    dt: str
    entity: str
    keys: Dict[str, Any]


class BaseConnector:
    source: str
    # Per-entity list of field names that MUST be present as keys on a real
    # (non-empty, non-error) parsed record. Populated for entities where a
    # wrong-endpoint/wrong-report bug has actually happened in production
    # (see: mlb_statcast silently returning a leaderboard report instead of
    # pitch-by-pitch data) -- this is exactly what would have caught that bug
    # at ingestion time instead of several layers downstream, silently.
    EXPECTED_FIELDS: Dict[str, List[str]] = {}

    def __init__(self, writer: BronzeWriter, cache: ResponseCache) -> None:
        self.writer = writer
        self.cache = cache
        self.user_agent = os.getenv("GBDP_USER_AGENT", "gbdp-ingest/0.1")
        self.timeout = int(os.getenv("GBDP_HTTP_TIMEOUT", "30"))
        self.retries = int(os.getenv("GBDP_HTTP_RETRIES", "3"))
        self.backoff = float(os.getenv("GBDP_HTTP_BACKOFF", "1.5"))
        self.min_interval = float(os.getenv("GBDP_HTTP_MIN_INTERVAL", "0.25"))
        self._last_request_ts = 0.0

    def list_partitions(self, start_date: str, end_date: str, entity: str) -> List[Partition]:
        raise NotImplementedError

    def fetch_partition(self, partition: Partition) -> RawPayload:
        raise NotImplementedError

    def parse_payload(self, payload: RawPayload) -> Iterable[Dict[str, Any]]:
        raise NotImplementedError

    def write_bronze(self, payload: RawPayload, records: Iterable[Dict[str, Any]], force: bool = False) -> None:
        records = list(records)
        self._check_contract(payload.entity, records)
        self.writer.write_raw(payload, force=force)
        self.writer.write_parsed(payload, records, force=force)

    def _check_contract(self, entity: str, records: List[Dict[str, Any]]) -> None:
        expected = self.EXPECTED_FIELDS.get(entity)
        if not expected:
            return
        real_record = next(
            (r for r in records if isinstance(r, dict) and "raw_text" not in r and not r.get("empty")),
            None,
        )
        if real_record is None:
            return  # genuinely no data for this partition; nothing to validate
        missing = [f for f in expected if f not in real_record]
        if missing:
            raise RuntimeError(
                f"{self.source}:{entity} contract violation: response is missing expected "
                f"field(s) {missing} -- this usually means the wrong endpoint/report was "
                f"fetched (got keys: {sorted(real_record.keys())[:15]})"
            )

    def emit_watermark(self, partition: Partition, status: str) -> None:
        logger.info("watermark source=%s entity=%s dt=%s status=%s", self.source, partition.entity, partition.dt, status)

    def http_get(self, url: str, params: Optional[Dict[str, Any]] = None) -> RawPayload:
        params = params or {}
        key = request_hash(url, params)
        cached = None
        if os.getenv("GBDP_DISABLE_CACHE", "false").lower() not in ("1", "true", "yes"):
            cached = self.cache.get(key)
        if cached is not None:
            if os.getenv("GBDP_DISABLE_REQUEST_LOG", "false").lower() not in ("1", "true", "yes"):
                write_request_log(
                    {
                        "source": self.source,
                        "url": cached["url"],
                        "params": cached["params"],
                        "status_code": cached["status_code"],
                        "cached": True,
                        "latency_ms": 0,
                        "retries": 0,
                        "fetched_at_utc": cached["fetched_at_utc"],
                        "dt": cached["fetched_at_utc"][:10],
                    }
                )
            return RawPayload(
                source=self.source,
                entity="unknown",
                dt="unknown",
                url=cached["url"],
                params=cached["params"],
                status_code=cached["status_code"],
                fetched_at_utc=cached["fetched_at_utc"],
                checksum=cached["checksum"],
                content_type=cached.get("content_type", "application/json"),
                body_text=cached["body_text"],
            )

        headers = {"User-Agent": self.user_agent}
        last_exc: Optional[Exception] = None
        for attempt in range(1, self.retries + 1):
            try:
                self._throttle()
                start = time.time()
                resp = requests.get(url, params=params, headers=headers, timeout=self.timeout)
                latency_ms = int((time.time() - start) * 1000)
                body_text = resp.text
                checksum = sha256_bytes(body_text.encode("utf-8"))
                fetched_at = utc_now().isoformat()
                payload = {
                    "url": url,
                    "params": params,
                    "status_code": resp.status_code,
                    "fetched_at_utc": fetched_at,
                    "checksum": checksum,
                    "content_type": resp.headers.get("Content-Type", ""),
                    "body_text": body_text,
                }
                if os.getenv("GBDP_DISABLE_CACHE", "false").lower() not in ("1", "true", "yes"):
                    self.cache.set(key, payload)
                if os.getenv("GBDP_DISABLE_REQUEST_LOG", "false").lower() not in ("1", "true", "yes"):
                    write_request_log(
                        {
                            "source": self.source,
                            "url": url,
                            "params": params,
                            "status_code": resp.status_code,
                            "cached": False,
                            "latency_ms": latency_ms,
                            "retries": attempt - 1,
                            "fetched_at_utc": fetched_at,
                            "dt": fetched_at[:10],
                        }
                    )
                return RawPayload(
                    source=self.source,
                    entity="unknown",
                    dt="unknown",
                    url=url,
                    params=params,
                    status_code=resp.status_code,
                    fetched_at_utc=fetched_at,
                    checksum=checksum,
                    content_type=resp.headers.get("Content-Type", ""),
                    body_text=body_text,
                )
            except Exception as exc:
                last_exc = exc
                sleep_s = self.backoff ** attempt
                time.sleep(sleep_s)
        raise RuntimeError(f"HTTP GET failed after {self.retries} attempts: {last_exc}")

    def _throttle(self) -> None:
        now = time.time()
        wait = self.min_interval - (now - self._last_request_ts)
        if wait > 0:
            time.sleep(wait)
        self._last_request_ts = time.time()

    def json_records(self, payload: RawPayload) -> Iterable[Dict[str, Any]]:
        try:
            data = json.loads(payload.body_text)
        except json.JSONDecodeError:
            return [{"raw_text": payload.body_text}]
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            if "data" in data and isinstance(data["data"], list):
                return data["data"]
            return [data]
        return [{"raw_text": payload.body_text}]
