from __future__ import annotations

from typing import Any, Dict, Iterable, List

from gbdp.bronze.writer import RawPayload
from gbdp.connectors.base import BaseConnector, Partition
from gbdp.utils.time import daterange, parse_date


class MlbStatcastConnector(BaseConnector):
    source = "mlb_statcast"

    def __init__(self, writer, cache, base_url: str) -> None:
        super().__init__(writer, cache)
        self.base_url = base_url

    def list_partitions(self, start_date: str, end_date: str, entity: str) -> List[Partition]:
        start = parse_date(start_date)
        end = parse_date(end_date)
        return [Partition(dt=d.isoformat(), entity=entity, keys={}) for d in daterange(start, end)]

    def fetch_partition(self, partition: Partition) -> RawPayload:
        if partition.entity != "pitches":
            raise ValueError(f"Unsupported Statcast entity: {partition.entity}")
        params = {
            "all": "true",
            "type": "details",
            "game_date_gt": partition.dt,
            "game_date_lt": partition.dt,
        }
        raw = self.http_get(self.base_url, params)
        return RawPayload(
            source=self.source,
            entity="pitches",
            dt=partition.dt,
            url=raw.url,
            params=params,
            status_code=raw.status_code,
            fetched_at_utc=raw.fetched_at_utc,
            checksum=raw.checksum,
            content_type=raw.content_type,
            body_text=raw.body_text,
        )

    def parse_payload(self, payload: RawPayload) -> Iterable[Dict[str, Any]]:
        import csv
        from io import StringIO

        buffer = StringIO(payload.body_text)
        reader = csv.DictReader(buffer)
        return list(reader)
