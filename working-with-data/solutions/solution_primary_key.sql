/** 
 ** Modeling Data -- Choosing a good primary key
 **/

-- returns the top 100 downloaded projects.
SELECT
    PROJECT,
    count() AS c
FROM pypi
GROUP BY PROJECT
ORDER BY c DESC
LIMIT 100;


-- same as above but filter the results that occurred in April of 2023. 
SELECT
    PROJECT,
    count() AS c
FROM pypi
WHERE TIMESTAMP >= toDate('2023-04-01') 
      AND TIMESTAMP < toDate('2023-05-01')
GROUP BY PROJECT
ORDER BY c DESC
LIMIT 100;

-- Count downloads of projects that start with the string "boto". 
SELECT
    PROJECT,
    count() AS c
FROM pypi
WHERE PROJECT LIKE 'boto%'
GROUP BY PROJECT
ORDER BY c DESC;

/** Try different primary key combinations. Which performs best? **/

-- Create and populate a new PyPi table with primary key (TIMESTAMP,PROJECT). 
CREATE TABLE pypi_key_time_project (
    TIMESTAMP DateTime,
    COUNTRY_CODE String,
    URL String,
    PROJECT String
)
ENGINE = MergeTree
PRIMARY KEY (TIMESTAMP, PROJECT);

INSERT INTO pypi_key_time_project
    SELECT *
    FROM pypi;

-- Repeat test query on new table.
SELECT
    PROJECT,
    count() AS c
FROM pypi_key_time_project
WHERE PROJECT LIKE 'boto%'
GROUP BY PROJECT
ORDER BY c DESC;

-- Create and populate a another new PyPi table with primary key (PROJECT,TIMESTAMP). 

CREATE TABLE pypi_key_project_time (
    TIMESTAMP DateTime,
    COUNTRY_CODE String,
    URL String,
    PROJECT String
)
ENGINE = MergeTree
PRIMARY KEY (PROJECT, TIMESTAMP);

INSERT INTO pypi_key_project_time
    SELECT *
    FROM pypi;

-- Repeat test query on new table.
SELECT
    PROJECT,
    count() AS c
FROM pypi_key_project_time
WHERE PROJECT LIKE 'boto%'
GROUP BY PROJECT
ORDER BY c DESC;

/** Conclusion: for this particular query, the (PROJECT,TIMESTAMP) is best **/

-- Compare compressed and uncompressed data sizes in the different key variations
SELECT
    table,
    formatReadableSize(sum(data_compressed_bytes)) AS compressed_size,
    formatReadableSize(sum(data_uncompressed_bytes)) AS uncompressed_size,
    count() AS num_of_active_parts
FROM system.parts
WHERE (active = 1) AND (table LIKE 'pypi%') AND (database = currentDatabase())
GROUP BY table;
-- Conclusion:  the compressed size with (PROJECT,TIME) is significantly smaller
