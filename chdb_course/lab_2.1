-- Step 1:
import requests
import pandas as pd
import chdb


response = requests.get(
  "https://raw.githubusercontent.com/statsbomb/open-data/master/data/matches/223/282.json"
)
matches_df = pd.json_normalize(response.json(), sep='_')


-- Step 2:
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
