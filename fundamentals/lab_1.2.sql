--Step 1:
SELECT *
FROM s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet')
LIMIT 100;

--Step 2:
SELECT count()
FROM s3(
   'https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet');

--Step 3:
SELECT
    avg(volume)
FROM s3(
   'https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet')
WHERE
    crypto_name = 'Bitcoin';

--Step 4:
SELECT
   crypto_name,
   count() AS count
FROM s3('https://learnclickhouse.s3.us-east-2.amazonaws.com/datasets/crypto_prices.parquet')
GROUP BY crypto_name
ORDER BY crypto_name;
