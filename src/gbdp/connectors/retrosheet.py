from __future__ import annotations

import csv
import json
import zipfile
from io import TextIOWrapper
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from gbdp.bronze.writer import RawPayload
from gbdp.connectors.base import BaseConnector, Partition
from gbdp.utils.io import manual_root, sha256_bytes
from gbdp.utils.time import daterange, parse_date, utc_now


class RetrosheetLocalConnector(BaseConnector):
    """
    Reads Retrosheet CSVs from:
    - data/manual/csvdownloads.zip (preferred)
    - data/manual/retrosheet/*.csv (fallback)
    """

    source = "retrosheet_local"

    _date_cols = {
        "gameinfo": "date",
        "teamstats": "date",
        "batting": "date",
        "pitching": "date",
        "fielding": "date",
        "plays": "date",
    }

    def list_partitions(self, start_date: str, end_date: str, entity: str) -> List[Partition]:
        start = parse_date(start_date)
        end = parse_date(end_date)
        return [Partition(dt=d.isoformat(), entity=entity, keys={}) for d in daterange(start, end)]

    def fetch_partition(self, partition: Partition) -> RawPayload:
        rows = self._read_rows(partition.entity, partition.dt)
        body_text = json.dumps(rows, ensure_ascii=True)
        checksum = sha256_bytes(body_text.encode("utf-8"))
        return RawPayload(
            source=self.source,
            entity=partition.entity,
            dt=partition.dt,
            url=self._source_location(),
            params={},
            status_code=200,
            fetched_at_utc=utc_now().isoformat(),
            checksum=checksum,
            content_type="text/csv",
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

    def _source_location(self) -> str:
        zip_path = manual_root() / "csvdownloads.zip"
        if zip_path.exists():
            return str(zip_path)
        return str(manual_root() / "retrosheet")

    def _read_rows(self, entity: str, dt: str) -> List[Dict[str, Any]]:
        zip_path = manual_root() / "csvdownloads.zip"
        if zip_path.exists():
            return self._read_from_zip(zip_path, entity, dt)
        return self._read_from_dir(manual_root() / "retrosheet", entity, dt)

    def _read_from_dir(self, base: Path, entity: str, dt: str) -> List[Dict[str, Any]]:
        path = base / f"{entity}.csv"
        if not path.exists():
            return []
        with path.open("r", encoding="utf-8") as f:
            return self._filter_rows(csv.DictReader(f), entity, dt)

    def _read_from_zip(self, zip_path: Path, entity: str, dt: str) -> List[Dict[str, Any]]:
        name = self._find_zip_member(zip_path, f"{entity}.csv")
        if not name:
            return []
        with zipfile.ZipFile(zip_path, "r") as zf:
            with zf.open(name, "r") as bf:
                wrapper = TextIOWrapper(bf, encoding="utf-8")
                return self._filter_rows(csv.DictReader(wrapper), entity, dt)

    def _find_zip_member(self, zip_path: Path, filename: str) -> Optional[str]:
        with zipfile.ZipFile(zip_path, "r") as zf:
            for name in zf.namelist():
                if name.lower().endswith(filename.lower()):
                    return name
        return None

    def _filter_rows(self, reader: csv.DictReader, entity: str, dt: str) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        date_col = self._date_cols.get(entity)
        year = dt.split("-")[0]
        yyyymmdd = dt.replace("-", "")
        for row in reader:
            if entity == "allplayers":
                if row.get("season") != year:
                    continue
                rows.append(row)
                continue
            if date_col:
                raw = row.get(date_col) or ""
                if raw == dt or raw == yyyymmdd or raw.startswith(yyyymmdd):
                    rows.append(row)
                continue
            rows.append(row)
        return rows
