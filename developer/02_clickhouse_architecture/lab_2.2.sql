--Step 4:
CREATE TABLE test_pypi (
    TIMESTAMP DateTime,
    COUNTRY_CODE String,
    URL String,
    PROJECT String
)
ENGINE = MergeTree
PRIMARY KEY (PROJECT, COUNTRY_CODE, TIMESTAMP);

INSERT INTO test_pypi
    SELECT * FROM pypi2;