-- Materialized view demo

-- create example source table
CREATE OR REPLACE TABLE temperatures (
    temp_C Int8,
    record_date Date,
    city LowCardinality(String)
)
PRIMARY KEY (record_date, city);

-- insert sample data
INSERT INTO temperatures VALUES
  (9, '2024-12-02', 'London'),
  (3, '2024-12-02', 'Boston'),
  (27, '2024-12-03', 'Hong Kong'),
  (5, '2024-12-03', 'Boston') ;

-- create destination table
CREATE OR REPLACE TABLE boston_temps_dest (
    temp_F Int8,
    record_date Date
)
PRIMARY KEY record_date;


-- copy and transform sample data into destination table
INSERT INTO boston_temps_dest
   SELECT (temp_C * 9/5) + 32  AS temp_F, record_date
   FROM temperatures
   WHERE city='Boston';

-- create materialized view (insert trigger)
-- DROP TABLE boston_temps_mv;
CREATE MATERIALIZED VIEW boston_temps_mv 
TO boston_temps_dest
AS
   SELECT (temp_C * 9/5) + 32  AS temp_F, record_date
   FROM temperatures
   WHERE city='Boston';

-- test materialized view by inserting new rows
INSERT INTO temperatures VALUES
  (5, '2024-12-05', 'Boston'),
  (12, '2024-12-05', 'Madrid');

-- did it work?
SELECT * FROM temperatures;
SELECT * FROM boston_temps_dest;