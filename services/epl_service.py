# services/epl_service.py
import requests
import time
import os
import json
import pandas as pd

# ========= CONFIG =========
API_KEY = os.getenv("FOOTBALL_DATA_API_KEY")
HEADERS = {"X-Auth-Token": API_KEY}
SEASONS = range(2023, 2026)


# ========= GENERIC FETCH =========
def fetch_multi_season_api(
    base_url: str,
    seasons: range,
    extractor_func,
    sleep_sec: int = 6,
):
    all_rows = []

    for season in seasons:
        r = requests.get(base_url, headers=HEADERS, params={"season": season})
        data = r.json()
        rows = extractor_func(data, season)
        all_rows.extend(rows)
        time.sleep(sleep_sec)

    return all_rows


# ========= EXTRACTORS =========
def extract_standings(data, season):
    rows = []
    for standing in data.get("standings", []):
        for row in standing.get("table", []):
            row["season"] = season
            row["standing_type"] = standing.get("type")
            rows.append(row)
    return rows


def extract_matches(data, season):
    return [{**m, "season": season} for m in data.get("matches", [])]


def extract_scorers(data, season):
    return [{**s, "season": season} for s in data.get("scorers", [])]


def extract_teams(data, season):
    return [{**t, "season": season} for t in data.get("teams", [])]


def extract_squad(data, team_id):
    return [{**p, "team_id": team_id} for p in data.get("squad", [])]


def fetch_all_team_squads(team_ids, sleep_sec=6):
    all_rows = []
    for team_id in team_ids:
        url = f"https://api.football-data.org/v4/teams/{team_id}"
        r = requests.get(url, headers=HEADERS)
        data = r.json()
        all_rows.extend(extract_squad(data, team_id))
        time.sleep(sleep_sec)
    return all_rows


# ========= PIPELINE (AZURE CALL) =========
def run_epl_pipeline():
    standings = fetch_multi_season_api(
        "https://api.football-data.org/v4/competitions/PL/standings",
        SEASONS,
        extract_standings
    )

    matches = fetch_multi_season_api(
        "https://api.football-data.org/v4/competitions/PL/matches",
        SEASONS,
        extract_matches
    )

    scorers = fetch_multi_season_api(
        "https://api.football-data.org/v4/competitions/PL/scorers",
        SEASONS,
        extract_scorers
    )

    teams = fetch_multi_season_api(
        "https://api.football-data.org/v4/competitions/PL/teams",
        SEASONS,
        extract_teams
    )

    team_ids = [t["id"] for t in teams]
    squad = fetch_all_team_squads(team_ids)

    return {
        "standings_rows": len(standings),
        "matches_rows": len(matches),
        "scorers_rows": len(scorers),
        "teams_rows": len(teams),
        "squad_rows": len(squad),
    }
# ========= END PIPELINE =========