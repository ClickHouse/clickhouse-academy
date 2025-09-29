-- Step 1:
pip install jupysql

-- Step 2:
pip install jupyterlab
pip install notebook
pip install ipython
  
--Step 3:
from urllib.request import urlretrieve
files = ['00s', '10s', '20s', '70s', '80s', '90s', 'current']
base = "https://raw.githubusercontent.com/JeffSackmann/tennis_atp/master"
for file in files:
  _ = urlretrieve(
    f"{base}/atp_rankings_{file}.csv",
    f"atp_rankings_{file}.csv",
  )


-- Step 4:
from chdb import dbapi
conn = dbapi.connect(path="atp.chdb")

-- Step 5: 
%load_ext sql
%sql conn --alias chdb
%config SqlMagic.displaylimit = None

-- Step 6:
%%sql
DESCRIBE file('atp_rankings*.csv')
SETTINGS describe_compact_output=1,
         schema_inference_make_columns_nullable=0


%sql SELECT * FROM file('atp_rankings*.csv') LIMIT 5

-- Step 7:
%%sql
SELECT * REPLACE (
  toDate(parseDateTime32BestEffort(toString(ranking_date))) AS ranking_date
)
FROM file('atp_rankings*.csv')
LIMIT 10
SETTINGS schema_inference_make_columns_nullable=0

-- Step 8:
%sql CREATE DATABASE atp

-- Step 9:
%%sql
CREATE TABLE atp.rankings
ENGINE=MergeTree
ORDER BY ranking_date AS
SELECT * REPLACE (
  toDate(parseDateTime32BestEffort(toString(ranking_date))) AS ranking_date
)
FROM file('atp_rankings*.csv')
SETTINGS schema_inference_make_columns_nullable=0

-- Step 10:
_ = urlretrieve(
    f"{base}/atp_players.csv",
    "atp_players.csv",
)

-- Step 11:
%%sql
CREATE TABLE atp.players
Engine=MergeTree
ORDER BY player_id AS
SELECT * REPLACE (
  makeDate32(
    toInt32OrNull(substring(toString(dob), 1, 4)),
    toInt32OrNull(substring(toString(dob), 5, 2)),
    toInt32OrNull(substring(toString(dob), 7, 2))
  )::Nullable(Date32) AS dob
)
FROM file('atp_players.csv')
SETTINGS schema_inference_make_columns_nullable=0

-- Step 12:
%%sql
SELECT name_first, name_last,
       max(points) as maxPoints,
       argMax(rank, points) as rank,
       argMax(ranking_date, points) as date
FROM atp.players
JOIN atp.rankings ON rankings.player = players.player_id
GROUP BY ALL
ORDER BY maxPoints DESC
LIMIT 10

-- Step 13:
%%sql --save best_points --no-execute
SELECT name_first, name_last,
       max(points) as maxPoints,
       argMax(rank, points) as rank,
       argMax(ranking_date, points) as date
FROM atp.players
JOIN atp.rankings ON rankings.player = players.player_id
GROUP BY ALL
ORDER BY maxPoints DESC

-- Step 14:
%sql select * FROM best_points WHERE rank=1

-- Step 15:
rank = 10

%%sql
SELECT name_first, name_last,
       MIN(ranking_date) AS earliest_date,
       MAX(ranking_date) AS most_recent_date,
       most_recent_date - earliest_date AS days,
       1 + (days/7) AS weeks
FROM atp.rankings
JOIN atp.players ON players.player_id = rankings.player
WHERE rank <= {{rank}}
GROUP BY ALL
ORDER BY days
LIMIT 10

