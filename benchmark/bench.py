"""
Banc d'essai Cassandra / PostgreSQL sur des mesures de capteurs IoT.

  python bench.py init              cree les schemas
  python bench.py load K            charge l'annee K en remontant le temps (K = 1 : l'annee la plus recente)
  python bench.py measure V         mesure les temps de requete pour un historique de V annees
  python bench.py writes            debit d'ecriture unitaire (une mesure = une requete)

50 capteurs, 5 zones, une mesure toutes les 15 minutes (96 par jour et par capteur),
trois valeurs par mesure : temperature, humidite, batterie.
"""
import io
import os
import json
import random
import statistics
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from multiprocessing import Pool
from pathlib import Path

import numpy as np
import psycopg2
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.policies import RoundRobinPolicy

ICI = Path(__file__).resolve().parent
RESULTATS = ICI / "resultats.json"

CASS_PORT = 9043
# Connexion PostgreSQL : definir la variable d'environnement PG_DSN, par exemple
#   export PG_DSN="host=127.0.0.1 port=5433 dbname=iot user=postgres password=..."
PG_DSN = os.environ.get("PG_DSN", "host=127.0.0.1 port=5433 dbname=iot user=postgres")

NB_CAPTEURS = 50
ZONES = ["Zone-Nord", "Zone-Sud", "Zone-Est", "Zone-Ouest", "Zone-Centre"]
CAPTEURS = [f"S{i:03d}" for i in range(1, NB_CAPTEURS + 1)]
ZONE_DE = {c: ZONES[i % len(ZONES)] for i, c in enumerate(CAPTEURS)}
PAS_MIN = 15
MESURES_JOUR = 24 * 60 // PAS_MIN
FIN = date(2026, 10, 15)          # exclu : la derniere journee chargee est le 14/10/2026
JOUR_RECENT = date(2026, 10, 14)
CAPTEUR_TEST = "S017"
ZONE_TEST = ZONE_DE[CAPTEUR_TEST]
EPOCH = date(1970, 1, 1)


# ---------------------------------------------------------------- donnees
def bornes_annee(k):
    """Annee k en remontant : [FIN - k ans, FIN - (k-1) ans)."""
    return date(FIN.year - k, FIN.month, FIN.day), date(FIN.year - k + 1, FIN.month, FIN.day)


def debut_historique(v):
    return date(FIN.year - v, FIN.month, FIN.day)


def generer(capteur, k):
    """Mesures d'un capteur sur l'annee k : (jours, ts_ms, temp, hum, bat), tableaux numpy."""
    d0, d1 = bornes_annee(k)
    n_jours = (d1 - d0).days
    idx = int(capteur[1:])
    rng = np.random.default_rng(1000 * k + idx)
    n = n_jours * MESURES_JOUR
    jour = np.repeat(np.arange(n_jours), MESURES_JOUR)
    minute = np.tile(np.arange(MESURES_JOUR) * PAS_MIN, n_jours)
    jours_epoch = (d0 - EPOCH).days + jour
    ts_ms = (jours_epoch.astype(np.int64) * 86400 + minute.astype(np.int64) * 60) * 1000
    doy = (jours_epoch + 4) % 365
    heure = minute / 60.0
    base = 26.0 + (idx % 5) * 0.6
    temp = (base
            + 2.0 * np.sin(2 * np.pi * (doy - 60) / 365.0)
            + 3.5 * np.sin(2 * np.pi * (heure - 9) / 24.0)
            + rng.normal(0, 0.8, n))
    pic = rng.random(n) < 0.002
    temp = temp + pic * rng.uniform(4.0, 7.0, n)
    temp = np.round(temp, 1)
    hum = np.round(np.clip(65 - 1.3 * (temp - 27) + rng.normal(0, 2.5, n), 20, 100), 1)
    bat = (100 - (jours_epoch % 90)).astype(np.int64)
    return d0, n_jours, jours_epoch, ts_ms, temp, hum, bat


def stats_jour(n_jours, temp):
    t = temp.reshape(n_jours, MESURES_JOUR)
    return t.min(axis=1), t.max(axis=1), np.round(t.mean(axis=1), 2)


# ---------------------------------------------------------------- connexions
def session_cassandra(timeout=600):
    profil = ExecutionProfile(load_balancing_policy=RoundRobinPolicy(), request_timeout=timeout)
    cluster = Cluster(["127.0.0.1"], port=CASS_PORT, execution_profiles={EXEC_PROFILE_DEFAULT: profil},
                      protocol_version=5)
    return cluster, cluster.connect()


def connexion_pg():
    cx = psycopg2.connect(PG_DSN)
    cx.autocommit = True
    with cx.cursor() as cur:
        cur.execute("SET TIME ZONE 'UTC'")
    return cx


# ---------------------------------------------------------------- init
def init():
    cluster, s = session_cassandra()
    for instr in (ICI / "schema_cassandra.cql").read_text().split(";"):
        lignes = [l for l in instr.splitlines() if not l.strip().startswith("//")]
        texte = "\n".join(lignes).strip()
        if texte:
            s.execute(texte)
    cluster.shutdown()
    cx = connexion_pg()
    with cx.cursor() as cur:
        cur.execute((ICI / "schema_postgres.sql").read_text())
        cur.executemany("INSERT INTO sensors VALUES (%s, %s) ON CONFLICT DO NOTHING",
                        [(c, ZONE_DE[c]) for c in CAPTEURS])
    cx.close()
    print("schemas crees")


# ---------------------------------------------------------------- chargement
_s = None
_ps = {}


def _init_worker():
    global _s, _ps
    _, _s = session_cassandra()
    _s.set_keyspace("iot_bench")
    _ps["r"] = _s.prepare("INSERT INTO readings_by_sensor_day (sensor_id, day, ts, temperature, humidity, battery_pct) "
                          "VALUES (?, ?, ?, ?, ?, ?)")
    _ps["a"] = _s.prepare("INSERT INTO alerts_by_zone_day (zone, day, ts, sensor_id, temperature) VALUES (?, ?, ?, ?, ?)")
    _ps["d"] = _s.prepare("INSERT INTO daily_stats_by_sensor (sensor_id, day, nb_mesures, temp_min, temp_max, temp_moy) "
                          "VALUES (?, ?, ?, ?, ?, ?)")
    _ps["l"] = _s.prepare("INSERT INTO last_reading_by_sensor (sensor_id, ts, temperature, humidity) VALUES (?, ?, ?, ?)")


def _charger_capteur_cassandra(arg):
    capteur, k = arg
    d0, n_jours, jours_epoch, ts_ms, temp, hum, bat = generer(capteur, k)
    jours = [d0 + timedelta(days=i) for i in range(n_jours)]
    jour_de = np.repeat(np.arange(n_jours), MESURES_JOUR)
    lignes = [(capteur, jours[j], int(t), float(a), float(b), int(c))
              for j, t, a, b, c in zip(jour_de, ts_ms, temp, hum, bat)]
    execute_concurrent_with_args(_s, _ps["r"], lignes, concurrency=200, raise_on_first_error=True)
    alertes = [(ZONE_DE[capteur], jours[jour_de[i]], int(ts_ms[i]), capteur, float(temp[i]))
               for i in np.nonzero(temp > 35)[0]]
    if alertes:
        execute_concurrent_with_args(_s, _ps["a"], alertes, concurrency=100, raise_on_first_error=True)
    tmin, tmax, tmoy = stats_jour(n_jours, temp)
    execute_concurrent_with_args(_s, _ps["d"], [(capteur, jours[i], MESURES_JOUR, float(tmin[i]), float(tmax[i]),
                                                  float(tmoy[i])) for i in range(n_jours)],
                                 concurrency=100, raise_on_first_error=True)
    if k == 1:
        _s.execute(_ps["l"], (capteur, int(ts_ms[-1]), float(temp[-1]), float(hum[-1])))
    return len(lignes)


def charger_postgres(k):
    cx = connexion_pg()
    n = 0
    with cx.cursor() as cur:
        for capteur in CAPTEURS:
            _, _, _, ts_ms, temp, hum, bat = generer(capteur, k)
            buf = io.StringIO()
            for t, a, b, c in zip(ts_ms, temp, hum, bat):
                buf.write(f"{capteur},{datetime.fromtimestamp(t / 1000, timezone.utc).isoformat()},{a},{b},{c}\n")
            buf.seek(0)
            cur.copy_expert("COPY readings (sensor_id, ts, temperature, humidity, battery_pct) FROM STDIN WITH (FORMAT csv)", buf)
            n += len(ts_ms)
    cx.close()
    return n


def load(k):
    res = charger_resultats()
    t0 = time.perf_counter()
    with Pool(3, initializer=_init_worker) as pool:
        n_c = sum(pool.map(_charger_capteur_cassandra, [(c, k) for c in CAPTEURS]))
    t_c = time.perf_counter() - t0
    t0 = time.perf_counter()
    n_p = charger_postgres(k)
    t_p = time.perf_counter() - t0
    res.setdefault("chargement", {})[str(k)] = {"lignes": n_c, "cassandra_s": round(t_c, 1), "postgres_s": round(t_p, 1)}
    sauver_resultats(res)
    print(f"annee {k} : {n_c} lignes | Cassandra {t_c:.1f} s ({n_c / t_c:.0f} l/s) | PostgreSQL {t_p:.1f} s ({n_p / t_p:.0f} l/s)")


# ---------------------------------------------------------------- mesure
def chrono(f, *a):
    t0 = time.perf_counter()
    f(*a)
    return (time.perf_counter() - t0) * 1000


def resume(echantillon):
    e = sorted(echantillon)
    return {"med": round(statistics.median(e), 2), "p95": round(e[int(0.95 * (len(e) - 1))], 2),
            "min": round(e[0], 2), "n": len(e)}


class Requetes:
    """Les memes questions metier, posees a chaque base dans son propre langage."""

    def __init__(self):
        self.cluster, self.s = session_cassandra()
        self.s.set_keyspace("iot_bench")
        p = self.s.prepare
        self.c = {
            "q1": p("SELECT ts, temperature, humidity FROM readings_by_sensor_day WHERE sensor_id = ? AND day = ? LIMIT 10"),
            "q2": p("SELECT ts, temperature FROM readings_by_sensor_day WHERE sensor_id = ? AND day = ? AND ts >= ? AND ts < ?"),
            "q3": p("SELECT * FROM last_reading_by_sensor"),
            "q4": p("SELECT * FROM alerts_by_zone_day WHERE zone = ? AND day = ?"),
            "q5": p("SELECT COUNT(*), MIN(temperature), MAX(temperature), AVG(temperature) FROM readings_by_sensor_day "
                    "WHERE sensor_id = ? AND day = ?"),
            "q6": p("SELECT day, nb_mesures, temp_min, temp_max, temp_moy FROM daily_stats_by_sensor "
                    "WHERE sensor_id = ? AND day >= ? AND day <= ?"),
            "q7": p("SELECT COUNT(*) FROM readings_by_sensor_day WHERE temperature > 35 ALLOW FILTERING"),
        }
        self.pg = connexion_pg()
        self.cur = self.pg.cursor()

    @staticmethod
    def _ts(jour, h):
        return datetime(jour.year, jour.month, jour.day, h, tzinfo=timezone.utc)

    # Cassandra
    def c_q1(self, cap, jour):
        return list(self.s.execute(self.c["q1"], (cap, jour)))

    def c_q2(self, cap, jour):
        return list(self.s.execute(self.c["q2"], (cap, jour, self._ts(jour, 8), self._ts(jour, 12))))

    def c_q3(self, *_):
        return list(self.s.execute(self.c["q3"]))

    def c_q4(self, zone, jour):
        return list(self.s.execute(self.c["q4"], (zone, jour)))

    def c_q5(self, cap, jour):
        return list(self.s.execute(self.c["q5"], (cap, jour)))

    def c_q6(self, cap, jour):
        return list(self.s.execute(self.c["q6"], (cap, jour - timedelta(days=364), jour)))

    def c_q7(self, *_):
        return list(self.s.execute(self.c["q7"], timeout=1800))

    # PostgreSQL
    def _pg(self, sql, args=()):
        self.cur.execute(sql, args)
        return self.cur.fetchall()

    def p_q1(self, cap, jour):
        return self._pg("SELECT ts, temperature, humidity FROM readings WHERE sensor_id = %s AND ts >= %s AND ts < %s "
                        "ORDER BY ts DESC LIMIT 10", (cap, jour, jour + timedelta(days=1)))

    def p_q2(self, cap, jour):
        return self._pg("SELECT ts, temperature FROM readings WHERE sensor_id = %s AND ts >= %s AND ts < %s "
                        "ORDER BY ts DESC", (cap, self._ts(jour, 8), self._ts(jour, 12)))

    def p_q3(self, *_):
        return self._pg("SELECT s.sensor_id, r.ts, r.temperature, r.humidity FROM sensors s CROSS JOIN LATERAL "
                        "(SELECT ts, temperature, humidity FROM readings WHERE sensor_id = s.sensor_id "
                        "ORDER BY ts DESC LIMIT 1) r")

    def p_q4(self, zone, jour):
        return self._pg("SELECT r.ts, r.sensor_id, r.temperature FROM readings r JOIN sensors s ON s.sensor_id = r.sensor_id "
                        "WHERE s.zone = %s AND r.ts >= %s AND r.ts < %s AND r.temperature > 35 ORDER BY r.ts DESC",
                        (zone, jour, jour + timedelta(days=1)))

    def p_q5(self, cap, jour):
        return self._pg("SELECT count(*), min(temperature), max(temperature), avg(temperature) FROM readings "
                        "WHERE sensor_id = %s AND ts >= %s AND ts < %s", (cap, jour, jour + timedelta(days=1)))

    def p_q6(self, cap, jour):
        return self._pg("SELECT ts::date AS day, count(*), min(temperature), max(temperature), round(avg(temperature)::numeric, 2) "
                        "FROM readings WHERE sensor_id = %s AND ts >= %s AND ts < %s GROUP BY 1 ORDER BY 1 DESC",
                        (cap, jour - timedelta(days=364), jour + timedelta(days=1)))

    def p_q7(self, *_):
        return self._pg("SELECT count(*) FROM readings WHERE temperature > 35")


def vider_caches():
    """Redemarre les deux serveurs et vide le cache disque du systeme : la lecture suivante part du disque."""
    subprocess.run(["docker", "exec", "cass-bench", "nodetool", "flush", "iot_bench"], check=True)
    subprocess.run(["docker", "exec", "pg-bench", "psql", "-U", "postgres", "-d", "iot", "-c", "CHECKPOINT"],
                   check=True, capture_output=True)
    subprocess.run(["docker", "restart", "cass-bench", "pg-bench"], check=True, capture_output=True)
    subprocess.run(["docker", "run", "--rm", "--privileged", "postgres:15-alpine", "sh", "-c",
                    "sync; echo 3 > /proc/sys/vm/drop_caches"], check=True)
    for _ in range(120):
        try:
            cl, _s2 = session_cassandra()
            cl.shutdown()
            break
        except Exception:
            time.sleep(2)
    for _ in range(60):
        try:
            connexion_pg().close()
            break
        except Exception:
            time.sleep(1)


def tailles():
    out = subprocess.run(["docker", "exec", "cass-bench", "nodetool", "tablestats", "iot_bench.readings_by_sensor_day"],
                         capture_output=True, text=True).stdout
    cass = next(l.split(":")[1].strip() for l in out.splitlines() if "Space used (live)" in l)
    cx = connexion_pg()
    with cx.cursor() as cur:
        cur.execute("SELECT pg_relation_size('readings'), pg_indexes_size('readings'), pg_total_relation_size('readings')")
        table, index, total = cur.fetchone()
    cx.close()
    return {"cassandra_octets": int(cass), "pg_table_octets": table, "pg_index_octets": index, "pg_total_octets": total}


def measure(v):
    res = charger_resultats()
    debut = debut_historique(v)
    vieux = debut                      # la journee la plus ancienne de l'historique
    print(f"--- historique de {v} an(s) : du {debut} au {JOUR_RECENT}")

    # Maintenance identique des deux cotes avant mesure
    subprocess.run(["docker", "exec", "pg-bench", "psql", "-U", "postgres", "-d", "iot", "-c", "VACUUM ANALYZE"],
                   check=True, capture_output=True)
    vider_caches()
    r = Requetes()
    rnd = random.Random(v)

    # Mise en route (JVM, plans) sur une journee du milieu, sans toucher aux cles mesurees ensuite
    milieu = debut + (JOUR_RECENT - debut) / 2
    for i in range(30):
        r.c_q1("S050", milieu + timedelta(days=i))
        r.p_q1("S050", milieu + timedelta(days=i))

    # 1. Lectures a froid : 20 cles jamais lues, recentes (30 derniers jours) puis anciennes (premiere annee)
    froid = {}
    for nom, plage in (("recent", (JOUR_RECENT - timedelta(days=29), JOUR_RECENT)),
                       ("ancien", (debut, debut + timedelta(days=364)))):
        cles = [(rnd.choice(CAPTEURS[:49]), plage[0] + timedelta(days=rnd.randrange((plage[1] - plage[0]).days + 1)))
                for _ in range(20)]
        froid[nom] = {"cassandra": resume([chrono(r.c_q1, c, j) for c, j in cles]),
                      "postgres": resume([chrono(r.p_q1, c, j) for c, j in cles])}

    # 2. Lectures a chaud : la meme requete repetee (5 essais ignores, puis 50 mesures)
    chaud = {}
    plan = {"q1_recent": ("q1", CAPTEUR_TEST, JOUR_RECENT), "q1_ancien": ("q1", CAPTEUR_TEST, vieux),
            "q2_ancien": ("q2", CAPTEUR_TEST, vieux), "q3": ("q3", None, None),
            "q4": ("q4", ZONE_TEST, JOUR_RECENT), "q5_ancien": ("q5", CAPTEUR_TEST, vieux),
            "q6_1an": ("q6", CAPTEUR_TEST, JOUR_RECENT)}
    for cle, (q, a, b) in plan.items():
        chaud[cle] = {}
        for base, f in (("cassandra", getattr(r, "c_" + q)), ("postgres", getattr(r, "p_" + q))):
            for _ in range(5):
                f(a, b)
            chaud[cle][base] = resume([chrono(f, a, b) for _ in range(50)])

    # 3. Requete non prevue : parcours complet (3 executions reussies, 6 tentatives au plus ;
    #    un depassement du delai serveur est note comme echec avec le temps ecoule)
    q7 = {}
    for base, f in (("cassandra", r.c_q7), ("postgres", r.p_q7)):
        t, echecs, val = [], [], None
        for _ in range(6):
            t0 = time.perf_counter()
            try:
                val = f()
                t.append((time.perf_counter() - t0) * 1000)
            except Exception as e:
                echecs.append({"ms": round((time.perf_counter() - t0) * 1000), "erreur": type(e).__name__})
            if len(t) == 3:
                break
        q7[base] = {**(resume(t) if t else {}), "resultat": int(val[0][0]) if val else None, "echecs": echecs}

    res.setdefault("mesures", {})[str(v)] = {"froid": froid, "chaud": chaud, "q7": q7, "tailles": tailles(),
                                             "jour_ancien": str(vieux)}
    sauver_resultats(res)
    print(json.dumps(res["mesures"][str(v)], indent=1))
    r.cluster.shutdown()


def q7_delai_releve(v):
    """Requete non prevue apres relevement de native_transport_timeout (600 s) : 3 executions."""
    vider_caches()
    r = Requetes()
    t, val = [], None
    for _ in range(3):
        t0 = time.perf_counter()
        val = r.c_q7()
        t.append((time.perf_counter() - t0) * 1000)
    res = charger_resultats()
    res["mesures"][str(v)]["q7_cassandra_delai_releve"] = {**resume(t), "resultat": int(val[0][0])}
    sauver_resultats(res)
    print(res["mesures"][str(v)]["q7_cassandra_delai_releve"])
    r.cluster.shutdown()


# ---------------------------------------------------------------- ecritures unitaires
def writes(n=50000):
    """Ecritures une par une, comme des capteurs qui envoient chacun leur mesure (capteur fictif T001)."""
    base = int(datetime(2026, 10, 15, tzinfo=timezone.utc).timestamp() * 1000)
    lignes = [("T001", date(2026, 10, 15) + timedelta(days=i // 8640), base + i * 10000, 27.0, 60.0, 90) for i in range(n)]
    cluster, s = session_cassandra()
    s.set_keyspace("iot_bench")
    ps = s.prepare("INSERT INTO readings_by_sensor_day (sensor_id, day, ts, temperature, humidity, battery_pct) VALUES (?, ?, ?, ?, ?, ?)")
    t0 = time.perf_counter()
    execute_concurrent_with_args(s, ps, lignes, concurrency=64, raise_on_first_error=True)
    t_c = time.perf_counter() - t0
    cluster.shutdown()

    cx = connexion_pg()
    with cx.cursor() as cur:
        cur.execute("INSERT INTO sensors VALUES ('T001', 'Zone-Test') ON CONFLICT DO NOTHING")
    cx.close()

    def ecrire(tranche):
        c = connexion_pg()
        with c.cursor() as cur:
            for l in tranche:
                cur.execute("INSERT INTO readings VALUES (%s, %s, %s, %s, %s)",
                            (l[0], datetime.fromtimestamp(l[2] / 1000, timezone.utc), l[3], l[4], l[5]))
        c.close()

    t0 = time.perf_counter()
    with ThreadPoolExecutor(8) as ex:
        list(ex.map(ecrire, [lignes[i::8] for i in range(8)]))
    t_p = time.perf_counter() - t0

    res = charger_resultats()
    res["ecritures_unitaires"] = {"n": n, "cassandra_s": round(t_c, 2), "postgres_s": round(t_p, 2),
                                  "cassandra_par_s": round(n / t_c), "postgres_par_s": round(n / t_p)}
    sauver_resultats(res)
    print(res["ecritures_unitaires"])

    # nettoyage du capteur fictif
    cluster, s = session_cassandra()
    s.set_keyspace("iot_bench")
    for d in sorted({l[1] for l in lignes}):
        s.execute("DELETE FROM readings_by_sensor_day WHERE sensor_id = 'T001' AND day = %s", (d,))
    cluster.shutdown()
    cx = connexion_pg()
    with cx.cursor() as cur:
        cur.execute("DELETE FROM readings WHERE sensor_id = 'T001'")
        cur.execute("DELETE FROM sensors WHERE sensor_id = 'T001'")
    cx.close()


# ---------------------------------------------------------------- utilitaires
def charger_resultats():
    return json.loads(RESULTATS.read_text()) if RESULTATS.exists() else {}


def sauver_resultats(res):
    RESULTATS.write_text(json.dumps(res, indent=1, ensure_ascii=False))


if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "init":
        init()
    elif cmd == "load":
        load(int(sys.argv[2]))
    elif cmd == "measure":
        measure(int(sys.argv[2]))
    elif cmd == "writes":
        writes()
    elif cmd == "q7":
        q7_delai_releve(int(sys.argv[2]))
