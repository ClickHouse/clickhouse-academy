/**
** ClickHouse Architecture
**/

-- view metadata about pypi table
SELECT *
FROM system.tables
WHERE (database = currentDatabase()) AND (name = 'pypi')
FORMAT VERTICAL

-- view metadata about pypi columns
SELECT
    table,
    name,
    type,
    position,
    is_in_primary_key
FROM system.columns
WHERE (database = currentDatabase()) AND (table = 'pypi')

-- view metadata about pypi parts
SELECT
    table,
    name,
    rows,
    formatReadableSize(data_compressed_bytes) AS compressed_size,
    formatReadableSize(data_uncompressed_bytes) AS uncompressed_size
FROM system.parts
WHERE (active = 1) AND (database = currentDatabase()) AND (table = 'pypi')

-- view info about individual column files in part folders
SELECT
    column,
    name,
    rows,
    formatReadableSize(data_compressed_bytes) AS compressed_size,
    formatReadableSize(data_uncompressed_bytes) AS uncompressed_size
FROM system.parts_columns
WHERE (active = 1) AND (database = currentDatabase()) AND (table = 'pypi')

-- show the size of the data in all the parts
SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    count() AS num_of_active_parts
FROM system.parts
WHERE (active = 1) AND (table = 'pypi') AND (database = currentDatabase())
GROUP BY table;


