from __future__ import annotations

import json
from datetime import date
from typing import Any, Dict, Iterable, List

from gbdp.bronze.writer import RawPayload
from gbdp.connectors.base import BaseConnector, Partition
from gbdp.utils.time import daterange, parse_date


class MlbStatsApiConnector(BaseConnector):
    source = "mlb_statsapi"
    EXPECTED_FIELDS = {
        "schedule": ["dates"],
        "rosters": ["teams"],
        "transactions": ["transactions"],
    }

    def __init__(self, writer, cache, base_url: str) -> None:
        super().__init__(writer, cache)
        self.base_url = base_url.rstrip("/")

    def list_partitions(self, start_date: str, end_date: str, entity: str) -> List[Partition]:
        start = parse_date(start_date)
        end = parse_date(end_date)
        partitions: List[Partition] = []
        for d in daterange(start, end):
            partitions.append(Partition(dt=d.isoformat(), entity=entity, keys={}))
        return partitions

    def fetch_partition(self, partition: Partition) -> RawPayload:
        if partition.entity == "schedule":
            params = {"sportId": 1, "startDate": partition.dt, "endDate": partition.dt}
            url = f"{self.base_url}/schedule"
            raw = self.http_get(url, params)
            return raw.__class__(
                source=self.source,
                entity="schedule",
                dt=partition.dt,
                url=raw.url,
                params=params,
                status_code=raw.status_code,
                fetched_at_utc=raw.fetched_at_utc,
                checksum=raw.checksum,
                content_type=raw.content_type,
                body_text=raw.body_text,
            )
        if partition.entity == "rosters":
            season = date.fromisoformat(partition.dt).year
            teams = self._get_team_ids()
            all_records: Dict[str, Any] = {"teams": []}
            for team_id in teams:
                params = {"season": season, "rosterType": "active"}
                url = f"{self.base_url}/teams/{team_id}/roster"
                raw = self.http_get(url, params)
                all_records["teams"].append(
                    {
                        "team_id": team_id,
                        "status_code": raw.status_code,
                        "body_text": raw.body_text,
                    }
                )
            body_text = json.dumps(all_records, ensure_ascii=True)
            checksum = raw.checksum if teams else self._checksum_empty(body_text)
            return RawPayload(
                source=self.source,
                entity="rosters",
                dt=partition.dt,
                url=f"{self.base_url}/teams/{{teamId}}/roster",
                params={"season": season, "rosterType": "active"},
                status_code=200,
                fetched_at_utc=raw.fetched_at_utc if teams else partition.dt + "T00:00:00Z",
                checksum=checksum,
                content_type="application/json",
                body_text=body_text,
            )
        if partition.entity == "transactions":
            params = {"sportId": 1, "startDate": partition.dt, "endDate": partition.dt}
            url = f"{self.base_url}/transactions"
            raw = self.http_get(url, params)
            return raw.__class__(
                source=self.source,
                entity="transactions",
                dt=partition.dt,
                url=raw.url,
                params=params,
                status_code=raw.status_code,
                fetched_at_utc=raw.fetched_at_utc,
                checksum=raw.checksum,
                content_type=raw.content_type,
                body_text=raw.body_text,
            )
        raise ValueError(f"Unsupported MLB StatsAPI entity: {partition.entity}")

    def parse_payload(self, payload: RawPayload) -> Iterable[Dict[str, Any]]:
        return self.json_records(payload)

    def _get_team_ids(self) -> List[int]:
        url = f"{self.base_url}/teams"
        params = {"sportId": 1}
        raw = self.http_get(url, params)
        data = json.loads(raw.body_text)
        teams = data.get("teams", [])
        return [int(t["id"]) for t in teams if "id" in t]

    def _checksum_empty(self, body_text: str) -> str:
        from gbdp.utils.io import sha256_bytes

        return sha256_bytes(body_text.encode("utf-8"))
