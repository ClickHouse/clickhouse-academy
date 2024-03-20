--Step 2:

/*
 * The query returns the last 20 queries executed ordered by time descending.
 */

--Step 3:

/*
 * The query is executed only on the node that is handling the request.
 * As there are two nodes in the cluster, the query returns two different sets
 * of results, one for each node.
 */

--Step 4:
SELECT
    event_time,
    query
FROM clusterAllReplicas(default, system.query_log)
ORDER BY  event_time DESC
LIMIT 20;

--Step 5:
SELECT
    query
FROM clusterAllReplicas(default, system.query_log)
WHERE has(tables, 'default.uk_price_paid');

--Step 6:
SELECT count()
FROM clusterAllReplicas(default, system.query_log)
WHERE positionCaseInsensitive(query, 'insert') > 0;

--Step 8:
SELECT count()
FROM clusterAllReplicas(default, system.parts);
