#!/bin/bash
# Migration PostgreSQL (pg-bench) -> Cassandra (cass-bench)
# 1. schema cible  2. export CSV  3. transfert  4. import  5. verification
set -e
cd "$(dirname "$0")"
mkdir -p csv

# 1. Creation du keyspace et des tables cibles
docker cp schema_cible.cql cass-bench:/tmp/schema_cible.cql
docker exec cass-bench cqlsh -f /tmp/schema_cible.cql

# 2. Export depuis PostgreSQL
docker cp export.sql pg-bench:/tmp/export.sql
docker exec pg-bench psql -U postgres -d iot -f /tmp/export.sql

# 3. Transfert des fichiers d'un conteneur a l'autre
for f in readings alerts daily_stats last_reading; do
  docker cp pg-bench:/tmp/$f.csv csv/$f.csv
  docker cp csv/$f.csv cass-bench:/tmp/$f.csv
done

# 4. Import dans Cassandra
docker cp import.cql cass-bench:/tmp/import.cql
docker exec cass-bench cqlsh -f /tmp/import.cql

# 5. Verification
docker cp verification.sql pg-bench:/tmp/verification.sql
docker exec pg-bench psql -U postgres -d iot -f /tmp/verification.sql
docker cp verification.cql cass-bench:/tmp/verification.cql
docker exec cass-bench cqlsh --request-timeout=120 -f /tmp/verification.cql
