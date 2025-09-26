-- Step 1: 
import chdb

query = """
SELECT
    toStartOfDay(date-1)::Date32 AS day,
    sum(count) AS num_of_downloads
FROM remoteSecure(
  'sql-clickhouse.clickhouse.com',
  'pypi.pypi_downloads_per_day',
  'play'
)
WHERE project = 'scikit-learn'
GROUP BY day
ORDER BY day ASC
"""

openai_df = chdb.query(query, "DataFrame")
openai_df.sort_values(by=["day"], ascending=False).head(n=1)

-- Step 2:
import chdb

query2 = """
SELECT
    toStartOfDay(date-1)::Date32 AS day,
    sumIf(count, project = 'openai') AS openai_downloads,
    sumIf(count, project = 'scikit-learn') AS sklearn_downloads,
    round(
        sumIf(count, project = 'openai') / nullIf(sumIf(count, project = 'scikit-learn'), 0),
        4
    ) AS openai_to_sklearn_ratio
FROM remoteSecure(
    'sql-clickhouse.clickhouse.com',
    'pypi.pypi_downloads_per_day',
    'play'
)
GROUP BY day
ORDER BY day DESC
LIMIT 1
"""

df = chdb.query(query2, "DataFrame")
