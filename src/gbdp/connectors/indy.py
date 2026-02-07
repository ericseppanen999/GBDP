from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterable, List

from gbdp.bronze.writer import RawPayload
from gbdp.connectors.base import BaseConnector, Partition
from gbdp.utils.io import list_dir, manual_root, path_exists, read_text, sha256_bytes
from gbdp.utils.time import daterange, parse_date, utc_now


class IndyLocalConnector(BaseConnector):
    """
    Local-file connector for independent leagues.
    Expects JSON files in data/manual/indy/<entity>/dt=YYYY-MM-DD/*.json
    """

    source = "indy_local"

    def list_partitions(self, start_date: str, end_date: str, entity: str) -> List[Partition]:
        start = parse_date(start_date)
        end = parse_date(end_date)
        return [Partition(dt=d.isoformat(), entity=entity, keys={}) for d in daterange(start, end)]

    def fetch_partition(self, partition: Partition) -> RawPayload:
        base = manual_root() / "indy" / partition.entity / f"dt={partition.dt}"
        records = []
        if path_exists(base):
            for f in list_dir(base):
                if f.suffix.lower() != ".json":
                    continue
                try:
                    records.append(json.loads(read_text(f, encoding="utf-8")))
                except Exception:
                    continue
        body_text = json.dumps(records, ensure_ascii=True)
        checksum = sha256_bytes(body_text.encode("utf-8"))
        return RawPayload(
            source=self.source,
            entity=partition.entity,
            dt=partition.dt,
            url=str(base),
            params={},
            status_code=200,
            fetched_at_utc=utc_now().isoformat(),
            checksum=checksum,
            content_type="application/json",
            body_text=body_text,
        )

    def parse_payload(self, payload: RawPayload) -> Iterable[Dict[str, Any]]:
        try:
            data = json.loads(payload.body_text)
            if isinstance(data, list):
                return data
            if isinstance(data, dict):
                return [data]
        except json.JSONDecodeError:
            return []
        return []
