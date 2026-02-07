from __future__ import annotations

import gzip
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pyarrow as pa

import os

from gbdp.utils.io import (
    bronze_root,
    dbfs_fuse_available,
    is_dbfs_path,
    path_exists,
    stable_json_dumps,
    write_bytes,
    write_parquet_table,
    write_text,
)
from gbdp.utils.time import utc_now


@dataclass
class RawPayload:
    source: str
    entity: str
    dt: str
    url: str
    params: Dict[str, Any]
    status_code: int
    fetched_at_utc: str
    checksum: str
    content_type: str
    body_text: str


class BronzeWriter:
    def __init__(self, root: Path | None = None) -> None:
        self.root = root or bronze_root()

    def _raw_path(self, payload: RawPayload) -> Path:
        return (
            self.root
            / "raw"
            / payload.source
            / payload.entity
            / f"dt={payload.dt}"
        )

    def _parsed_path(self, payload: RawPayload) -> Path:
        return (
            self.root
            / "parsed"
            / payload.source
            / payload.entity
            / f"dt={payload.dt}"
        )

    def write_raw(self, payload: RawPayload, force: bool = False) -> Path:
        path = self._raw_path(payload)
        record = {
            "source": payload.source,
            "entity": payload.entity,
            "dt": payload.dt,
            "url": payload.url,
            "params": payload.params,
            "status_code": payload.status_code,
            "fetched_at_utc": payload.fetched_at_utc,
            "checksum": payload.checksum,
            "content_type": payload.content_type,
            "body_text": payload.body_text,
        }
        payload_text = json.dumps(record, ensure_ascii=True)
        if is_dbfs_path(path) and not dbfs_fuse_available():
            filename = f"{payload.checksum}.json"
            full_path = path / filename
            if path_exists(full_path) and not force:
                return full_path
            return write_text(full_path, payload_text, force=force)
        filename = f"{payload.checksum}.json.gz"
        full_path = path / filename
        payload_bytes = payload_text.encode("utf-8")
        compressed = gzip.compress(payload_bytes)
        if path_exists(full_path) and not force:
            return full_path
        return write_bytes(full_path, compressed, force=force)

    def write_parsed(self, payload: RawPayload, records: Iterable[Dict[str, Any]], force: bool = False) -> Path:
        path = self._parsed_path(payload)
        table = self._to_table(payload, records)
        filename = f"{payload.checksum}.parquet"
        full_path = path / filename
        return write_parquet_table(table, full_path, force=force)

    def _to_table(self, payload: RawPayload, records: Iterable[Dict[str, Any]]) -> pa.Table:
        include_partition_cols = os.getenv("GBDP_INCLUDE_PARTITION_COLS", "false").lower() in (
            "1",
            "true",
            "yes",
        )
        normalized: List[Dict[str, Any]] = []
        for record in records:
            row = {}
            for k, v in record.items():
                if isinstance(v, (dict, list)):
                    row[k] = stable_json_dumps(v)
                else:
                    row[k] = v
            if include_partition_cols:
                row["dt"] = payload.dt
                row["source"] = payload.source
                row["entity"] = payload.entity
            row["ingested_at_utc"] = utc_now().isoformat()
            row["request_url"] = payload.url
            row["request_params"] = stable_json_dumps(payload.params)
            row["response_status"] = payload.status_code
            row["response_checksum"] = payload.checksum
            normalized.append(row)
        if not normalized:
            empty_row = {
                "ingested_at_utc": utc_now().isoformat(),
                "request_url": payload.url,
                "request_params": stable_json_dumps(payload.params),
                "response_status": payload.status_code,
                "response_checksum": payload.checksum,
                "empty": True,
            }
            if include_partition_cols:
                empty_row["dt"] = payload.dt
                empty_row["source"] = payload.source
                empty_row["entity"] = payload.entity
            normalized.append(empty_row)
        return pa.Table.from_pylist(normalized)
