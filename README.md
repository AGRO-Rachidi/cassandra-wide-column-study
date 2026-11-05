# Wide-column databases in practice: Apache Cassandra vs PostgreSQL

*Étude du modèle NoSQL orienté colonnes : cluster Cassandra, tolérance aux pannes et comparaison chiffrée avec PostgreSQL.*

Group project for the **Information Systems** course, engineering programme in Mathematical Engineering & Modeling, **ENSGMM / UNSTIM** (Abomey, Benin), academic year 2026–2027.

**Group 4:** AGRO Rachidi · ANATO Krisland · HOUNSOUNOU Marius. **Supervisor:** Dr (MC) KONNON Abel.

## What this project does

1. **Explains the model.** It covers the Bigtable data model, Cassandra vocabulary (keyspace, partition key, clustering key), the comparison with columnar storage and relational databases, partitioning (Murmur3), replication, tunable consistency, and the write and read paths.
2. **Builds a case study: IoT sensors.** The schema is designed *from the queries* (one table per query), with day bucketing so partitions stay bounded (about 8,640 rows per sensor per day). It runs on a **3-node Cassandra 5.0 cluster in Docker**.
   - Replication with RF = 3, and partitioning made visible with a second keyspace at RF = 1 (`nodetool getendpoints`).
   - Fault tolerance: `cass3` was stopped, and query Q1 still succeeded at **QUORUM**.
   - A query that was not planned for (`ALLOW FILTERING`) shows the model's limit.
3. **Benchmarks ten years of history.** Data was loaded year by year, about 17.5 million rows in total, with 50 sensors and one reading every 15 minutes. Cassandra and PostgreSQL were measured on the same machine:

| Criterion | Cassandra | PostgreSQL |
|---|---|---|
| Key lookup, warm | 3–5 ms, flat from 1 to 10 years | 0.3–1.6 ms, slowly rising |
| Cold read | 15–55 ms | 26–172 ms |
| Planned aggregate (pre-computed table) | 7 ms | 22 ms |
| Unplanned query over 10 years | fails at 12 s timeout; 20.7 s when raised | 16–24 s |
| Single-row writes | 2,504 / s | 305 / s |
| Disk space, 10 years | 349 MB | 1,755 MB |

4. **Migration.** PostgreSQL → Cassandra (export, import, verification scripts).

**Honest limits** (also stated in the report):

- All three nodes run on one machine. The measurements say nothing about Cassandra's main strength, which is adding machines.
- Timings include the Python drivers and the Docker network.
- The reading frequency was lowered to 15 minutes, from 10 seconds in the assignment.

## Repository layout

```
report/       rapport-modele-oriente-colonnes.pdf (full report, FR), presentation.pdf, LaTeX sources
case-study/   cql/setup.cql (schema + data), outputs/ (real cqlsh / nodetool outputs: quorum, failure, endpoints)
benchmark/    bench.py (load / measure / writes), graphiques.py, schemas, resultats.json (raw results)
migration/    export.sql, import.cql, migrer.sh, verification scripts
figures/      screenshots and charts used in the report
```

## Reproduce

Requirements: Docker, Python 3 with `cassandra-driver`, `psycopg2` and `numpy`.

```bash
# 3-node cluster (see report, section "Environnement")
docker network create cass-net
docker run -d --name cass1 --network cass-net -e MAX_HEAP_SIZE=512M -e HEAP_NEWSIZE=128M cassandra:5.0
# ... cass2 and cass3 with -e CASSANDRA_SEEDS=cass1, then:
docker exec -i cass1 cqlsh < case-study/cql/setup.cql

# Benchmark (PostgreSQL in a local container; the DSN is read from an environment variable)
export PG_DSN="host=127.0.0.1 port=5433 dbname=iot user=postgres password=<your-local-password>"
python benchmark/bench.py init
python benchmark/bench.py load 1     # repeat for K = 1..10
python benchmark/bench.py measure 10
python benchmark/bench.py writes
```

## Licence

Report and figures © the authors (Group 4), shared for reading. Code: MIT.
