import requests
import time
import os
import json
import pandas as pd
# import azure.functions as func

def fetch_multi_season_api(
    base_url: str,
    seasons: range,
    headers: dict,
    extractor_func,
    sleep_sec: int = 6,
    save_raw: bool = False,
    raw_dir: str = None
):
    all_rows = []

    for season in seasons:
        print(f"Fetching season {season}...")

        r = requests.get(base_url, headers=headers, params={"season": season})
        data = r.json()

        # Save raw JSON if needed
        if save_raw and raw_dir:
            os.makedirs(raw_dir, exist_ok=True)
            with open(os.path.join(raw_dir, f"raw_{season}.json"), "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)

        rows = extractor_func(data, season)
        all_rows.extend(rows)

        time.sleep(sleep_sec)

    return all_rows


# Extract standings data
def extract_standings(data, season):
    rows = []

    for standing in data.get("standings", []):
        standing_type = standing.get("type")

        for row in standing.get("table", []):
            row["season"] = season
            row["standing_type"] = standing_type
            rows.append(row)

    return rows


# Extract scorers data
def extract_scorers(data, season):
    rows = []

    for scorer in data.get("scorers", []):
        scorer["season"] = season
        rows.append(scorer)

    return rows


# Extract matches data
def extract_matches(data, season):
    rows = []

    for match in data.get("matches", []):
        match["season"] = season
        rows.append(match)

    return rows


# Extract teams data
def extract_teams(data, season):
    rows = []

    for team in data.get("teams", []):
        team["season"] = season
        rows.append(team)

    return rows


# Extract squad data
def extract_squad(data, team_id):
    rows = []
    squad = data.get("squad", [])

    for p in squad:
        p["team_id"] = team_id
        rows.append(p)

    return rows


# ===== Squad =====
def fetch_all_team_squads(team_ids, headers, sleep_sec=6):
    all_rows = []

    for team_id in team_ids:
        print(f"Fetching squad for team {team_id}")

        url = f"https://api.football-data.org/v4/teams/{team_id}"
        r = requests.get(url, headers=headers)
        data = r.json()

        rows = extract_squad(data, team_id)
        all_rows.extend(rows)

        time.sleep(sleep_sec)

    return all_rows



# Get API key from environment variable from 2023-2026
API_KEY = "4435eae26a3f4f9190d022d5baf6be97"
headers = {"X-Auth-Token": API_KEY}

BASE_DIR = r"C:\Users\ADMIN\Desktop\EPLraw"
os.makedirs(BASE_DIR, exist_ok=True)

SEASONS = range(2023, 2026)

# ===== Standings =====
standings_rows = fetch_multi_season_api(
    base_url="https://api.football-data.org/v4/competitions/PL/standings",
    seasons=SEASONS,
    headers=headers,
    extractor_func=extract_standings,
    save_raw=True,
    raw_dir=os.path.join(BASE_DIR, "raw_standings")
)

df_standings = pd.json_normalize(standings_rows)
print("Standings:", df_standings.shape)

# ===== Matches =====
matches_rows = fetch_multi_season_api(
    base_url="https://api.football-data.org/v4/competitions/PL/matches",
    seasons=SEASONS,
    headers=headers,
    extractor_func=extract_matches
)

df_matches = pd.json_normalize(matches_rows)
print("Matches:", df_matches.shape)

# ===== Scorers =====
scorers_rows = fetch_multi_season_api(
    base_url="https://api.football-data.org/v4/competitions/PL/scorers",
    seasons=SEASONS,
    headers=headers,
    extractor_func=extract_scorers
)

df_scorers = pd.json_normalize(scorers_rows)
print("Scorers:", df_scorers.shape)

# ===== Teams =====
teams_rows = fetch_multi_season_api(
    base_url="https://api.football-data.org/v4/competitions/PL/teams",
    seasons=SEASONS,
    headers=headers,
    extractor_func=extract_teams
)

df_teams = pd.json_normalize(teams_rows)
print("Teams:", df_teams.shape)


# ===== Squad =====
team_ids = df_teams["id"].unique().tolist()

squad_rows = fetch_all_team_squads(team_ids, headers)
df_squad = pd.json_normalize(squad_rows)

print("Squad:", df_squad.shape)