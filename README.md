# Cassandra vs PostgreSQL: a wide-column study

Group project for our Information Systems course at ENSGMM (Benin), 2026–2027. Group 4: AGRO Rachidi, ANATO Krisland, HOUNSOUNOU Marius. Supervised by Dr (MC) KONNON Abel.

The full report (in French) is in `report/rapport-modele-oriente-colonnes.pdf`.

## What we did

We started with the theory (Bigtable data model, partition and clustering keys, Murmur3 partitioning, replication, consistency levels), then built a case study around IoT sensors.

The Cassandra schema was designed from the queries: six queries ended up as four tables. Readings are bucketed by day, so a partition holds about 8,640 rows per sensor and doesn't grow forever.

We ran it on a 3-node Cassandra 5.0 cluster in Docker with RF = 3. To check fault tolerance, we stopped `cass3` and ran query Q1 again at `QUORUM`: it still answered. The real `cqlsh` and `nodetool` outputs are in `case-study/outputs/`.

Then we loaded ten years of readings, one year at a time (about 17.5 million rows in total), into both Cassandra and PostgreSQL, and timed the same queries on the same machine:

| | Cassandra | PostgreSQL |
|---|---|---|
| Key lookup (warm) | 3–5 ms, same from 1 to 10 years | 0.3–1.6 ms, slowly rising |
| Cold read | 15–55 ms | 26–172 ms |
| Planned aggregate | 7 ms | 22 ms |
| Unplanned query over 10 years | timed out at 12 s (20.7 s with a longer timeout) | 16–24 s |
| Single-row writes | 2,504 / s | 305 / s |
| Disk used after 10 years | 349 MB | 1,755 MB |

Things to keep in mind when reading these numbers: all three nodes ran on one laptop, so they say nothing about adding machines, which is Cassandra's main strength. The timings also include the Python drivers and the Docker network. And we lowered the reading interval from 10 seconds to 15 minutes so the data would fit on the machine.

There is also a small PostgreSQL → Cassandra migration in `migration/`.

## Running it

You need Docker and Python 3 with `cassandra-driver`, `psycopg2` and `numpy`.

```bash
docker network create cass-net
docker run -d --name cass1 --network cass-net -e MAX_HEAP_SIZE=512M -e HEAP_NEWSIZE=128M cassandra:5.0
# start cass2 and cass3 the same way, with -e CASSANDRA_SEEDS=cass1
docker exec -i cass1 cqlsh < case-study/cql/setup.cql

export PG_DSN="host=127.0.0.1 port=5433 dbname=iot user=postgres password=<local password>"
python benchmark/bench.py init
python benchmark/bench.py load 1      # then 2, 3 … 10
python benchmark/bench.py measure 10
python benchmark/bench.py writes
```

The report and figures belong to the three of us. The code is under the MIT license.
