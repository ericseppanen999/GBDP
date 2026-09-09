from __future__ import annotations

import json
from datetime import date
from typing import Any, Dict, Iterable, List

from gbdp.bronze.writer import RawPayload
from gbdp.connectors.base import BaseConnector, Partition
from gbdp.utils.time import daterange, parse_date

# koreabaseball.com has no published/documented public API. These are internal
# ASP.NET .asmx JSON web services that the site's own frontend JS calls --
# reverse-engineered from the page source of Schedule.aspx and
# GameCenter/ReviewNew.aspx. They are unofficial: KBO could change or block
# them without notice. Confirmed working with real requests (no session
# cookie required -- a stateless POST is sufficient).
_BASE = "https://www.koreabaseball.com"
_GAME_LIST_URL = f"{_BASE}/ws/Main.asmx/GetKboGameList"
_BOXSCORE_URL = f"{_BASE}/ws/Schedule.asmx/GetBoxScoreScroll"

# All-series ID list used by the site's own "all games" view (regular season,
# postseason, exhibition, etc). Passed as-is to GetKboGameList.
_ALL_SERIES_IDS = "0,1,3,4,5,6,7,8,9"


class KboApiConnector(BaseConnector):
    source = "kbo_api"
    EXPECTED_FIELDS = {
        "games": ["G_ID", "AWAY_ID", "HOME_ID"],
        "boxscore": ["games"],
    }

    def list_partitions(self, start_date: str, end_date: str, entity: str) -> List[Partition]:
        start = parse_date(start_date)
        end = parse_date(end_date)
        return [Partition(dt=d.isoformat(), entity=entity, keys={}) for d in daterange(start, end)]

    def fetch_partition(self, partition: Partition) -> RawPayload:
        if partition.entity == "games":
            return self._fetch_games(partition.dt)
        if partition.entity == "boxscore":
            return self._fetch_boxscore(partition.dt)
        raise ValueError(f"Unsupported KBO API entity: {partition.entity}")

    def _fetch_games(self, dt_str: str) -> RawPayload:
        yyyymmdd = dt_str.replace("-", "")
        form = {"leId": "1", "srId": _ALL_SERIES_IDS, "date": yyyymmdd}
        raw = self.http_post(_GAME_LIST_URL, form, referer=f"{_BASE}/")
        return RawPayload(
            source=self.source,
            entity="games",
            dt=dt_str,
            url=_GAME_LIST_URL,
            params=form,
            status_code=raw.status_code,
            fetched_at_utc=raw.fetched_at_utc,
            checksum=raw.checksum,
            content_type=raw.content_type,
            body_text=raw.body_text,
        )

    def _games_for_date(self, dt_str: str) -> List[Dict[str, Any]]:
        raw = self._fetch_games(dt_str)
        try:
            data = json.loads(raw.body_text)
        except json.JSONDecodeError:
            return []
        return data.get("game") or []

    def _fetch_boxscore(self, dt_str: str) -> RawPayload:
        games = self._games_for_date(dt_str)
        season_id = date.fromisoformat(dt_str).year
        all_records: Dict[str, Any] = {"games": []}
        for g in games:
            game_id = g.get("G_ID")
            if not game_id:
                continue
            sr_id = g.get("SR_ID", 0)
            form = {"leId": "1", "srId": str(sr_id), "seasonId": str(season_id), "gameId": game_id}
            raw = self.http_post(_BOXSCORE_URL, form, referer=f"{_BASE}/")
            all_records["games"].append(
                {
                    "game_id": game_id,
                    # arrHitter/arrPitcher in the boxscore response carry no
                    # team identifier at all -- confirmed empirically (via
                    # win/loss pitcher cross-reference against the game list)
                    # that index 0 is always the away team and index 1 the
                    # home team. Carry the real team codes through from the
                    # game list here so silver doesn't have to guess or
                    # re-join against a separate fetch.
                    "away_team_id": g.get("AWAY_ID"),
                    "home_team_id": g.get("HOME_ID"),
                    "status_code": raw.status_code,
                    "body_text": raw.body_text,
                }
            )
        body_text = json.dumps(all_records, ensure_ascii=True)
        from gbdp.utils.io import sha256_bytes
        from gbdp.utils.time import utc_now

        return RawPayload(
            source=self.source,
            entity="boxscore",
            dt=dt_str,
            url=_BOXSCORE_URL,
            params={"seasonId": season_id},
            status_code=200,
            fetched_at_utc=utc_now().isoformat(),
            checksum=sha256_bytes(body_text.encode("utf-8")),
            content_type="application/json",
            body_text=body_text,
        )

    def parse_payload(self, payload: RawPayload) -> Iterable[Dict[str, Any]]:
        try:
            data = json.loads(payload.body_text)
        except json.JSONDecodeError:
            return [{"raw_text": payload.body_text}]
        if payload.entity == "games":
            return data.get("game") or []
        if payload.entity == "boxscore":
            return [data]
        return [data] if isinstance(data, dict) else data
