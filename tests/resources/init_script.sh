
# initalize mysql db
python tests/resources/init_mysql.py

# validate server start
until curl -s -f "http://localhost:8585/api/v1/system/version"; do
  printf 'Waiting on Server to start...\n'
  curl -v "http://localhost:8585/api/v1/system/version"
  sleep 5
done

# ingest mysql to OMD
metadata ingest -c tests/resources/mysql.yaml