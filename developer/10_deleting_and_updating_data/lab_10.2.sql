--Step 1:
CREATE TABLE messages (
    id UInt32,
    day Date,
    message String,
    sign Int8
)
ENGINE = CollapsingMergeTree(sign)
PRIMARY KEY id;

--Step 2:
INSERT INTO messages VALUES
   (1, '2024-07-04', 'Hello', 1),
   (2, '2024-07-04', 'Hi', 1),
   (3, '2024-07-04', 'Bonjour', 1);

--Step 3:
SELECT * FROM messages;

--Step 4:
INSERT INTO messages VALUES
   (2, '2024-07-04', 'Hi', -1),
   (2, '2024-07-05', 'Goodbye', 1);

--Step 5:
INSERT INTO messages (id,sign) VALUES
    (3,-1);

--Step 6:
SELECT * FROM messages;

--Step 7:
SELECT * FROM messages FINAL;