-- Step 1: 
import chdb

query = """
SELECT
    toStartOfDay(date)::Date32 AS day,
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
openai_df.sort_values(by=["day"], ascending=False).head(n=10)

-- Step 2:
import chdb

query = """
SELECT
    toStartOfDay(date)::Date32 AS day,
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
WHERE toStartOfDay(date) = [insert today's date]
GROUP BY day

"""

df = chdb.query(query, "DataFrame")
print(df)
