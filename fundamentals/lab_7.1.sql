--Step 1:
CREATE TABLE messages (
    id UInt32,
    timestamp DateTime,
    message String
)
ENGINE = ReplacingMergeTree
ORDER BY id;

--Step 2:
INSERT INTO messages VALUES 
   (1, now(), 'Message #1'),
   (2, now(), 'Message #2'),
   (3, now(), 'Message #3');

--Step 3:
INSERT INTO messages VALUES 
   (1, now() + 10, 'New message #1');

--Step 4:
SELECT * FROM messages;

--Step 5:
SELECT * FROM messages FINAL;