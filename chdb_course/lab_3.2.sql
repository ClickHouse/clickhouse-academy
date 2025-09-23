-- Step 1:
docker pull ghcr.io/chdb-io/chdb-lambda:0.13.1

-- Step 2:
export AWS_ACCOUNT_ID = <account_id>

aws configure

-- Step 3:
./deploy.sh

-- Step 6:
curl -XPOST "http://{lambda_url}/query" \
  --header 'Content-Type: application/json'
  --data '{"query": "SELECT version()", "default_format": "CSV"}'
