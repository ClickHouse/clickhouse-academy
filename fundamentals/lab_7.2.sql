--Step 1:
DROP TABLE messages;

--Step 2:
CREATE TABLE messages (
    id UInt32,
    timestamp DateTime,
    message String,
    sign Int8
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY id;

--Step 3:
INSERT INTO messages VALUES 
   (1, now(), 'Message #1', 1),
   (2, now(), 'Message #2', 1),
   (3, now(), 'Message #3', 1);

--Step 4:
INSERT INTO messages VALUES 
   (2, null, null, -1),
   (2, now(), 'New message #2', 1);

--Step 5:
SELECT * FROM messages;


--Step 6:
SELECT * FROM messages FINAL;

--Step 7:
INSERT INTO messages VALUES (3, now(), 'New message #3', 1);

SELECT * FROM messages FINAL;

--Step 8:
INSERT INTO messages VALUES (2, null, null, -1);