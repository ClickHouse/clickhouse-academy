# Step 1:
import requests
import pandas as pd
import chdb


response = requests.get(
  "https://raw.githubusercontent.com/statsbomb/open-data/master/data/matches/223/282.json"
)
matches_df = pd.json_normalize(response.json(), sep='_')


# Step 2:
import requests
import pandas as pd
import chdb


response = requests.get(
  "https://raw.githubusercontent.com/statsbomb/open-data/master/data/matches/223/282.json"
)
matches_df = pd.json_normalize(response.json(), sep='_')

query = chdb.query("""
SELECT team_name, COUNT(*) AS total_matches
FROM (
    SELECT home_team_home_team_name AS team_name
    FROM Python(matches_df)

    UNION ALL

    SELECT away_team_away_team_name AS team_name
    FROM Python(matches_df)
) AS all_teams
GROUP BY team_name
ORDER BY total_matches DESC;
""", "DataFrame")

print(query)
Answer: Argentina


# Step 2 (using DataStore API)
import requests
from chdb import datastore as pd

response = requests.get(
    "https://raw.githubusercontent.com/statsbomb/open-data/master/data/matches/223/282.json"
)
matches = response.json()

ds = pd.DataFrame({
    "home_team_name": [m["home_team"]["home_team_name"] for m in matches],
    "away_team_name": [m["away_team"]["away_team_name"] for m in matches],
})

home_teams = ds[["home_team_name"]].rename(columns={"home_team_name": "team_name"})
away_teams = ds[["away_team_name"]].rename(columns={"away_team_name": "team_name"})
all_teams = pd.concat([home_teams, away_teams])

result = (
    all_teams
    .groupby("team_name")
    .agg(total_matches=("team_name", "count"))
    .reset_index()
    .sort_values("total_matches", ascending=False)
)

print(result)
Answer: Argentina

# Step 3
import requests
from chdb import datastore as pd

response = requests.get(
    "https://raw.githubusercontent.com/statsbomb/open-data/master/data/matches/223/282.json"
)
matches = response.json()

ds = pd.DataFrame({
    "home_team_name": [m["home_team"]["home_team_name"] for m in matches],
    "away_team_name": [m["away_team"]["away_team_name"] for m in matches],
})

home_teams = ds[["home_team_name"]].rename(columns={"home_team_name": "team_name"})
away_teams = ds[["away_team_name"]].rename(columns={"away_team_name": "team_name"})
all_teams = pd.concat([home_teams, away_teams])

result = (
    all_teams
)

print(type(result))

# Step 4
import requests
from chdb import datastore as pd

response = requests.get(
    "https://raw.githubusercontent.com/statsbomb/open-data/master/data/matches/223/282.json"
)
matches = response.json()

ds = pd.DataFrame({
    "home_team_name": [m["home_team"]["home_team_name"] for m in matches],
    "away_team_name": [m["away_team"]["away_team_name"] for m in matches],
})

home_teams = ds[["home_team_name"]].rename(columns={"home_team_name": "team_name"})
away_teams = ds[["away_team_name"]].rename(columns={"away_team_name": "team_name"})
all_teams = pd.concat([home_teams, away_teams])

result = (
    all_teams
)

print(result)
