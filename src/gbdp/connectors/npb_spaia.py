from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any, Dict, Iterable, List

from gbdp.bronze.writer import RawPayload
from gbdp.connectors.base import BaseConnector, Partition
from gbdp.utils.time import daterange, parse_date, yyyymmdd


class NpbSpaiaConnector(BaseConnector):
    source = "npb_spaia"

    def __init__(self, writer, cache, base_url: str) -> None:
        super().__init__(writer, cache)
        self.base_url = base_url.rstrip("/")

    def list_partitions(self, start_date: str, end_date: str, entity: str) -> List[Partition]:
        start = parse_date(start_date)
        end = parse_date(end_date)
        if entity in {
            "schedules",
            "standings",
            "player_batting_saber",
            "player_pitching_saber",
            "player_stats_by_year",
            "player_stats_by_month",
            "player_stats_by_game",
            "player_hitting_career",
            "player_info",
            "related_players",
            "same_draft_year_players",
        }:
            years = {d.year for d in daterange(start, end)}
            return [Partition(dt=f"{year}-01-01", entity=entity, keys={"year": year}) for year in sorted(years)]
        if entity == "monthly_schedule":
            months = {(d.year, d.month) for d in daterange(start, end)}
            return [
                Partition(dt=f"{y}-{m:02d}-01", entity=entity, keys={"year": y, "month": m})
                for y, m in sorted(months)
            ]
        return [Partition(dt=d.isoformat(), entity=entity, keys={}) for d in daterange(start, end)]

    def fetch_partition(self, partition: Partition) -> RawPayload:
        if partition.entity == "schedules":
            year = partition.keys.get("year") or date.fromisoformat(partition.dt).year
            params = {"Year": year}
            url = f"{self.base_url}/schedules"
            raw = self.http_get(url, params)
            return self._wrap(raw, partition, "schedules", params)

        if partition.entity == "standings":
            year = partition.keys.get("year") or date.fromisoformat(partition.dt).year
            params = {"GameAssortment": 1, "Year": year}
            url = f"{self.base_url}/official_stats_history"
            raw = self.http_get(url, params)
            return self._wrap(raw, partition, "standings", params)

        if partition.entity == "game_schedule":
            url = f"{self.base_url}/game_schedule"
            raw = self.http_get(url, {})
            return self._wrap(raw, partition, "game_schedule", {})

        if partition.entity == "weekly_schedule":
            start_dt = date.fromisoformat(partition.dt)
            end_dt = start_dt + timedelta(days=6)
            params = {"from": yyyymmdd(start_dt), "to": yyyymmdd(end_dt)}
            url = f"{self.base_url}/weekly_schedule"
            raw = self.http_get(url, params)
            return self._wrap(raw, partition, "weekly_schedule", params)

        if partition.entity == "monthly_schedule":
            year = partition.keys.get("year") or date.fromisoformat(partition.dt).year
            month = partition.keys.get("month") or date.fromisoformat(partition.dt).month
            params = {"year": year, "month": f"{month:02d}"}
            url = f"{self.base_url}/game_calendar"
            raw = self.http_get(url, params)
            return self._wrap(raw, partition, "monthly_schedule", params)

        if partition.entity == "directory":
            url = f"{self.base_url}/directory"
            raw = self.http_get(url, {})
            return self._wrap(raw, partition, "directory", {})

        if partition.entity == "rosters":
            year = date.fromisoformat(partition.dt).year
            teams = self._get_team_ids()
            all_records: Dict[str, Any] = {"teams": []}
            for team_id in teams:
                params = {"team_id": team_id, "year": year}
                url = f"{self.base_url}/players_by_team"
                raw = self.http_get(url, params)
                all_records["teams"].append(
                    {"team_id": team_id, "status_code": raw.status_code, "body_text": raw.body_text}
                )
            body_text = json.dumps(all_records, ensure_ascii=True)
            checksum = self._checksum(body_text)
            return RawPayload(
                source=self.source,
                entity="rosters",
                dt=partition.dt,
                url=f"{self.base_url}/players_by_team",
                params={"year": year},
                status_code=200,
                fetched_at_utc=partition.dt + "T00:00:00Z",
                checksum=checksum,
                content_type="application/json",
                body_text=body_text,
            )

        if partition.entity == "batter_list":
            return self._batch_team_payload(partition, "batter_list", f"{self.base_url}/batter_list", "team")

        if partition.entity == "pitcher_list":
            return self._batch_team_payload(partition, "pitcher_list", f"{self.base_url}/pitcher_list", "team")

        if partition.entity == "staff_list":
            return self._batch_team_payload(partition, "staff_list", f"{self.base_url}/staff_list", "team")

        if partition.entity == "games":
            params = {"gameDate": yyyymmdd(date.fromisoformat(partition.dt))}
            url = f"{self.base_url}/games_info_by_date"
            raw = self.http_get(url, params)
            return self._wrap(raw, partition, "games", params)

        if partition.entity == "game_pbp":
            game_ids = self._game_ids_for_date(partition.dt)
            return self._batch_game_payload(
                partition, "game_pbp", game_ids, f"{self.base_url}/game_text_pbp", "GameID"
            )

        if partition.entity == "game_pitches":
            game_ids = self._game_ids_for_date(partition.dt)
            return self._batch_game_payload(
                partition, "game_pitches", game_ids, f"{self.base_url}/flash_atbat_history", "gameId"
            )

        if partition.entity == "current_score":
            game_ids = self._game_ids_for_date(partition.dt)
            return self._batch_game_payload(
                partition, "current_score", game_ids, f"{self.base_url}/current_score", "game_id"
            )

        if partition.entity == "starting_members":
            game_ids = self._game_ids_for_date(partition.dt)
            return self._batch_game_payload(
                partition,
                "starting_members",
                game_ids,
                f"{self.base_url}/starting_members_for_flash",
                "gameId",
            )

        if partition.entity == "game_over_view":
            game_ids = self._game_ids_for_date(partition.dt)
            return self._batch_game_payload(
                partition, "game_over_view", game_ids, f"{self.base_url}/game_over_view", "gameId"
            )

        if partition.entity == "prediction_game":
            game_ids = self._game_ids_for_date(partition.dt)
            return self._batch_game_payload(
                partition, "prediction_game", game_ids, f"{self.base_url}/prediction_game", "GameID"
            )

        if partition.entity == "game_batter_stats":
            game_ids = self._game_ids_for_date(partition.dt)
            return self._batch_game_payload(
                partition,
                "game_batter_stats",
                game_ids,
                f"{self.base_url}/both_batter_stats",
                "gameId",
                extra_params={"matchday": yyyymmdd(date.fromisoformat(partition.dt))},
            )

        if partition.entity == "game_pitcher_stats":
            game_ids = self._game_ids_for_date(partition.dt)
            return self._batch_game_payload(
                partition,
                "game_pitcher_stats",
                game_ids,
                f"{self.base_url}/both_pitcher_game_stats",
                "gameId",
                extra_params={"matchday": yyyymmdd(date.fromisoformat(partition.dt))},
            )

        if partition.entity == "player_batting_saber":
            year = partition.keys.get("year") or date.fromisoformat(partition.dt).year
            params = {"year": year, "order_by": "BattingAverage", "is_desc": 1, "teams": self._teams_param()}
            url = f"{self.base_url}/player_batting_detail_saber"
            raw = self.http_get(url, params)
            return self._wrap(raw, partition, "player_batting_saber", params)

        if partition.entity == "player_pitching_saber":
            year = partition.keys.get("year") or date.fromisoformat(partition.dt).year
            params = {"year": year, "order_by": "EarnedRunAverage", "is_desc": 1, "teams": self._teams_param()}
            url = f"{self.base_url}/player_pitching_detail_saber"
            raw = self.http_get(url, params)
            return self._wrap(raw, partition, "player_pitching_saber", params)

        if partition.entity == "player_stats_by_year":
            year = date.fromisoformat(partition.dt).year
            return self._batch_player_payload(
                partition, "player_stats_by_year", f"{self.base_url}/hitting_stats_by_year", "player_id", {"year": year}
            )

        if partition.entity == "player_stats_by_month":
            year = date.fromisoformat(partition.dt).year
            return self._batch_player_payload(
                partition,
                "player_stats_by_month",
                f"{self.base_url}/hitting_stats_by_month",
                "player_id",
                {"year": year},
            )

        if partition.entity == "player_stats_by_game":
            year = date.fromisoformat(partition.dt).year
            return self._batch_player_payload(
                partition,
                "player_stats_by_game",
                f"{self.base_url}/hitting_stats_by_game",
                "player_id",
                {"year": year},
            )

        if partition.entity == "player_hitting_career":
            return self._batch_player_payload(
                partition,
                "player_hitting_career",
                f"{self.base_url}/hitting_stats_career",
                "playerId",
            )

        if partition.entity == "player_info":
            return self._batch_player_payload(
                partition, "player_info", f"{self.base_url}/player_by_team", "person_info_id"
            )

        if partition.entity == "related_players":
            return self._batch_player_payload(
                partition, "related_players", f"{self.base_url}/related_players", "player_id"
            )

        if partition.entity == "same_draft_year_players":
            return self._batch_player_payload(
                partition, "same_draft_year_players", f"{self.base_url}/same_draft_year_players", "player_id"
            )

        raise ValueError(f"Unsupported NPB SPAIA entity: {partition.entity}")

    def parse_payload(self, payload: RawPayload) -> Iterable[Dict[str, Any]]:
        return self.json_records(payload)

    def _wrap(self, raw: RawPayload, partition: Partition, entity: str, params: Dict[str, Any]) -> RawPayload:
        return RawPayload(
            source=self.source,
            entity=entity,
            dt=partition.dt,
            url=raw.url,
            params=params,
            status_code=raw.status_code,
            fetched_at_utc=raw.fetched_at_utc,
            checksum=raw.checksum,
            content_type=raw.content_type,
            body_text=raw.body_text,
        )

    def _checksum(self, body_text: str) -> str:
        from gbdp.utils.io import sha256_bytes

        return sha256_bytes(body_text.encode("utf-8"))

    def _game_ids_for_date(self, dt_str: str) -> List[str]:
        params = {"gameDate": yyyymmdd(date.fromisoformat(dt_str))}
        url = f"{self.base_url}/games_info_by_date"
        raw = self.http_get(url, params)
        try:
            data = json.loads(raw.body_text)
        except json.JSONDecodeError:
            return []
        if isinstance(data, list):
            return [str(x.get("GameID") or x.get("gameId") or x.get("game_id")) for x in data if isinstance(x, dict)]
        if isinstance(data, dict):
            games = data.get("games") or data.get("data") or []
            return [str(x.get("GameID") or x.get("gameId") or x.get("game_id")) for x in games if isinstance(x, dict)]
        return []

    def _batch_game_payload(
        self,
        partition: Partition,
        entity: str,
        game_ids: List[str],
        url: str,
        id_param: str,
        extra_params: Dict[str, Any] | None = None,
    ) -> RawPayload:
        all_records: Dict[str, Any] = {"games": []}
        for game_id in game_ids:
            params = {id_param: game_id}
            if extra_params:
                params.update(extra_params)
            raw = self.http_get(url, params)
            all_records["games"].append(
                {"game_id": game_id, "status_code": raw.status_code, "body_text": raw.body_text}
            )
        body_text = json.dumps(all_records, ensure_ascii=True)
        checksum = self._checksum(body_text)
        return RawPayload(
            source=self.source,
            entity=entity,
            dt=partition.dt,
            url=url,
            params=extra_params or {},
            status_code=200,
            fetched_at_utc=partition.dt + "T00:00:00Z",
            checksum=checksum,
            content_type="application/json",
            body_text=body_text,
        )

    def _get_team_ids(self) -> List[int]:
        url = f"{self.base_url}/directory"
        raw = self.http_get(url, {})
        try:
            data = json.loads(raw.body_text)
        except json.JSONDecodeError:
            return []
        teams = data.get("teams") if isinstance(data, dict) else []
        if not teams:
            teams = data if isinstance(data, list) else []
        ids = []
        for t in teams:
            if isinstance(t, dict):
                tid = t.get("TeamID") or t.get("team_id") or t.get("id")
                if tid is not None:
                    ids.append(int(tid))
        return ids

    def _batch_team_payload(
        self,
        partition: Partition,
        entity: str,
        url: str,
        team_param: str,
    ) -> RawPayload:
        year = date.fromisoformat(partition.dt).year
        teams = self._get_team_ids()
        all_records: Dict[str, Any] = {"teams": []}
        for team_id in teams:
            params = {team_param: team_id, "year": year}
            raw = self.http_get(url, params)
            all_records["teams"].append(
                {"team_id": team_id, "status_code": raw.status_code, "body_text": raw.body_text}
            )
        body_text = json.dumps(all_records, ensure_ascii=True)
        checksum = self._checksum(body_text)
        return RawPayload(
            source=self.source,
            entity=entity,
            dt=partition.dt,
            url=url,
            params={"year": year},
            status_code=200,
            fetched_at_utc=partition.dt + "T00:00:00Z",
            checksum=checksum,
            content_type="application/json",
            body_text=body_text,
        )

    def _batch_player_payload(
        self,
        partition: Partition,
        entity: str,
        url: str,
        player_param: str,
        extra_params: Dict[str, Any] | None = None,
    ) -> RawPayload:
        year = date.fromisoformat(partition.dt).year
        player_ids = self._player_ids_for_year(year)
        all_records: Dict[str, Any] = {"players": []}
        for player_id in player_ids:
            params = {player_param: player_id}
            if extra_params:
                params.update(extra_params)
            raw = self.http_get(url, params)
            all_records["players"].append(
                {"player_id": player_id, "status_code": raw.status_code, "body_text": raw.body_text}
            )
        body_text = json.dumps(all_records, ensure_ascii=True)
        checksum = self._checksum(body_text)
        return RawPayload(
            source=self.source,
            entity=entity,
            dt=partition.dt,
            url=url,
            params=extra_params or {},
            status_code=200,
            fetched_at_utc=partition.dt + "T00:00:00Z",
            checksum=checksum,
            content_type="application/json",
            body_text=body_text,
        )

    def _player_ids_for_year(self, year: int) -> List[str]:
        teams = self._get_team_ids()
        ids: List[str] = []
        for team_id in teams:
            params = {"team_id": team_id, "year": year}
            url = f"{self.base_url}/players_by_team"
            raw = self.http_get(url, params)
            try:
                data = json.loads(raw.body_text)
            except json.JSONDecodeError:
                continue
            candidates = []
            if isinstance(data, list):
                candidates = data
            elif isinstance(data, dict):
                candidates = data.get("players") or data.get("data") or []
            for p in candidates:
                if not isinstance(p, dict):
                    continue
                pid = (
                    p.get("player_id")
                    or p.get("playerId")
                    or p.get("PersonInfoId")
                    or p.get("person_info_id")
                    or p.get("id")
                )
                if pid is not None:
                    ids.append(str(pid))
        return sorted(set(ids))

    def _teams_param(self) -> str:
        team_ids = self._get_team_ids()
        if not team_ids:
            return "1,2,3,4,5,6,7,8,9,11,12,376"
        return ",".join(str(t) for t in team_ids)
