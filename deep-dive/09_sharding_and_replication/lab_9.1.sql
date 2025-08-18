--Step 1:
SELECT
    cluster,
    shard_num,
    replica_num,
    database_shard_name,
    database_replica_name
FROM system.clusters;

--Step 2:
SELECT event_time, query
FROM system.query_log
ORDER BY event_time DESC
LIMIT 20;

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
WHERE has(tables, 'default.uk_prices_3');

--Step 6:
SELECT count()
FROM clusterAllReplicas(default, system.query_log)
WHERE positionCaseInsensitive(query, 'insert') > 0;

--Step 7:
SELECT count()
FROM system.parts;

--Step 8:
SELECT count()
FROM clusterAllReplicas(default, system.parts);

--Step 9:
SELECT
    instance,
    * EXCEPT instance APPLY formatReadableSize
FROM (
    SELECT
        hostname() AS instance,
        sum(primary_key_size),
        sum(primary_key_bytes_in_memory),
        sum(primary_key_bytes_in_memory_allocated)
    FROM clusterAllReplicas(default, system.parts)
    GROUP BY instance
);