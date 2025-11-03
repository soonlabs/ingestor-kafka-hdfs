#!/bin/bash

# Timeout settings
MAX_WAIT_TIME=60  # Maximum wait time in seconds
WAIT_INTERVAL=2   # Time to wait between attempts
TOTAL_WAIT=0      # Total time spent waiting

echo "Waiting for HBase to initialize..."
while ! echo "status" | /opt/hbase/bin/hbase shell &>/dev/null; do
  sleep $WAIT_INTERVAL
  TOTAL_WAIT=$((TOTAL_WAIT + WAIT_INTERVAL))
  if [ $TOTAL_WAIT -ge $MAX_WAIT_TIME ]; then
    echo "ERROR: HBase did not initialize within $MAX_WAIT_TIME seconds."
    exit 1
  fi
done
echo "HBase is ready."

# Define tables and column families
echo "Creating HBase tables..."
TABLE_PREFIX="${TABLE_PREFIX:-}"
TABLES=("blocks" "tx" "tx-by-addr" "tx_full")

for table in "${TABLES[@]}"; do
  if [ -n "$TABLE_PREFIX" ]; then
    full_table="${TABLE_PREFIX}.${table}"
  else
    full_table="$table"
  fi

  echo "create '${full_table}', 'x'" | /opt/hbase/bin/hbase shell || \
    echo "INFO: Table '${full_table}' already exists, skipping."
done

echo "HBase table creation completed."
exit 0
