# services/epl_service.py
import requests
import time
import os
import json


# ===== Fetch helper =====
def fetch_multi_season_api(
    base_url,
    seasons,
    headers,
    extractor_func,
    sleep_sec=6,
    save_raw=False,
    raw_dir=None
):
    all_rows = []

    for season in seasons:
        r = requests.get(base_url, headers=headers, params={"season": season})
        data = r.json()

        if save_raw and raw_dir:
            os.makedirs(raw_dir, exist_ok=True)
            with open(
                os.path.join(raw_dir, f"raw_{season}.json"),
                "w",
                encoding="utf-8"
            ) as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

        rows = extractor_func(data, season)
        all_rows.extend(rows)
        time.sleep(sleep_sec)

    return all_rows


# ===== Extractors =====
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


def fetch_all_team_squads(team_ids, headers, sleep_sec=6):
    rows = []
    for team_id in team_ids:
        r = requests.get(
            f"https://api.football-data.org/v4/teams/{team_id}",
            headers=headers
        )
        rows.extend(extract_squad(r.json(), team_id))
        time.sleep(sleep_sec)
    return rows


# ===== MAIN PIPELINE (AZURE GỌI CÁI NÀY) =====
def run_epl_pipeline():
    API_KEY = os.getenv("FOOTBALL_DATA_API_KEY")
    if not API_KEY:
        raise ValueError("FOOTBALL_DATA_API_KEY not set")

    headers = {"X-Auth-Token": API_KEY}
    BASE_DIR = "/tmp/eplraw"   # OK cho Azure
    SEASONS = range(2023, 2026)

    standings = fetch_multi_season_api(
        "https://api.football-data.org/v4/competitions/PL/standings",
        SEASONS, headers, extract_standings,
        save_raw=True,
        raw_dir=os.path.join(BASE_DIR, "standings")
    )

    matches = fetch_multi_season_api(
        "https://api.football-data.org/v4/competitions/PL/matches",
        SEASONS, headers, extract_matches
    )

    scorers = fetch_multi_season_api(
        "https://api.football-data.org/v4/competitions/PL/scorers",
        SEASONS, headers, extract_scorers
    )

    teams = fetch_multi_season_api(
        "https://api.football-data.org/v4/competitions/PL/teams",
        SEASONS, headers, extract_teams
    )

    team_ids = [t["id"] for t in teams]
    squads = fetch_all_team_squads(team_ids, headers)

    return {
        "standings": len(standings),
        "matches": len(matches),
        "scorers": len(scorers),
        "teams": len(teams),
        "squads": len(squads)
    }
