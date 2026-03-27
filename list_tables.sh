#!/bin/bash

set -u

# =====================================
# MySQL connection parameters (Hive Metastore)
# =====================================
MYSQL_HOST="mysql_host"
MYSQL_PORT="3306"
MYSQL_DB="metastore"
MYSQL_USER="hive"
MYSQL_PASSWORD="hivesecretpassword"

# =====================================
# Input SQL file / output file
# =====================================
SQL_FILE="list_tables.sql"
OUTPUT_FILE="hive_storage_locations.tsv"

# =====================================
# Checks
# =====================================
if [ ! -f "$SQL_FILE" ]; then
  echo "ERROR: SQL file not found: $SQL_FILE"
  exit 1
fi

echo "Starting Hive Metastore export..."
echo "SQL file: $SQL_FILE"

if [ -z "$MYSQL_PASSWORD" ]; then
  mysql -h "$MYSQL_HOST" \
        -P "$MYSQL_PORT" \
        -u "$MYSQL_USER" \
        -p \
        "$MYSQL_DB" \
        -N < "$SQL_FILE" > "$OUTPUT_FILE"
else
  mysql -h "$MYSQL_HOST" \
        -P "$MYSQL_PORT" \
        -u "$MYSQL_USER" \
        -p"$MYSQL_PASSWORD" \
        "$MYSQL_DB" \
        -N < "$SQL_FILE" > "$OUTPUT_FILE"
fi

RC=$?

if [ "$RC" -ne 0 ]; then
  echo "Export failed."
  exit "$RC"
fi

echo "Export completed."
echo "Output file: $OUTPUT_FILE"