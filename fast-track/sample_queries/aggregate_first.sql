CREATE OR REPLACE TABLE drivers (
    id Int16,
    vehicle_id LowCardinality(String)
)
PRIMARY KEY tuple();

INSERT INTO drivers
VALUES (100, 'HC1'),
(101,'F1'),
(102,'F1'),
(103,'AA3'),
(104,'HC1'),
(105,'FF');

CREATE OR REPLACE TABLE vehicles ( 
    id LowCardinality(String),
    make LowCardinality(String),
    model LowCardinality(String)
)
PRIMARY KEY (make,model);

INSERT INTO vehicles
VALUES ('HC1','Honda','Civic'),
       ('FF','Ford','Fiesta'),
       ('F1','Ford','F-150'),
       ('AA3','Audi','A3');

SELECT 
   vehicle_id, 
   count() AS vehicle_count
FROM drivers 
GROUP BY vehicle_id;

-- no pre-aggregation
SELECT make, count()
FROM drivers
JOIN vehicles
ON vehicle_id = vehicles.id
GROUP BY make;

SELECT make, model, vehicle_count
FROM (
    SELECT 
      vehicle_id, 
      count() AS vehicle_count
    FROM drivers 
    GROUP BY vehicle_id
) AS vehicle_counts
JOIN vehicles
ON vehicle_counts.vehicle_id = vehicles.id
ORDER BY vehicle_count DESC;