"""Graphiques du banc d'essai (niveaux de gris, pour l'impression) a partir de resultats.json."""
import json
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ICI = Path(__file__).resolve().parent
SORTIE = Path(sys.argv[1]) if len(sys.argv) > 1 else ICI.parent
res = json.loads((ICI / "resultats.json").read_text())
ans = sorted(int(v) for v in res["mesures"])

plt.rcParams.update({"font.family": "serif", "font.serif": ["Nimbus Roman", "Times New Roman", "DejaVu Serif"],
                     "font.size": 9, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.edgecolor": "#555555", "axes.linewidth": 0.6, "grid.color": "#dddddd",
                     "grid.linewidth": 0.5, "xtick.color": "#333333", "ytick.color": "#333333"})
STYLE = {"cassandra": dict(color="black", ls="-", marker="o", ms=5, lw=1.6, label="Cassandra"),
         "postgres": dict(color="#888888", ls="--", marker="s", ms=5, lw=1.6, label="PostgreSQL")}


def serie(chemin, base):
    out = []
    for v in ans:
        d = res["mesures"][str(v)]
        for k in chemin:
            d = d[k]
        out.append(d[base]["med"])
    return out


def panneau(ax, chemin, titre, ylabel="ms (médiane)"):
    for base in ("cassandra", "postgres"):
        ax.plot(ans, serie(chemin, base), **STYLE[base])
    ax.set_title(titre, fontsize=9.5)
    ax.set_xticks(ans)
    ax.set_xlabel("historique (années)")
    ax.set_ylabel(ylabel)
    ax.set_ylim(bottom=0)
    ax.grid(axis="y")


# Figure 1 : lectures par cle, a chaud
fig, axes = plt.subplots(1, 3, figsize=(7.2, 2.5))
panneau(axes[0], ("chaud", "q1_recent"), "Q1, journée récente")
panneau(axes[1], ("chaud", "q1_ancien"), "Q1, journée la plus ancienne")
panneau(axes[2], ("chaud", "q6_1an"), "Q6, indicateurs sur 365 jours")
axes[0].legend(frameon=False, loc="center right")
fig.tight_layout()
fig.savefig(SORTIE / "fig11_latence_volume.png", dpi=220)

# Figure 2 : lecture a froid et requete non prevue
fig, axes = plt.subplots(1, 2, figsize=(7.2, 2.6))
panneau(axes[0], ("froid", "ancien"), "Q1 à froid, journées anciennes")
axes[1].plot(ans, [res["mesures"][str(v)]["q7"]["postgres"]["med"] / 1000 for v in ans], **STYLE["postgres"])
# Cassandra : a 10 ans la requete echoue avec le delai par defaut ; le point creux est mesure apres relevement du delai
ok = [v for v in ans if res["mesures"][str(v)]["q7"]["cassandra"].get("med")]
axes[1].plot(ok, [res["mesures"][str(v)]["q7"]["cassandra"]["med"] / 1000 for v in ok], **STYLE["cassandra"])
for v in ans:
    m = res["mesures"][str(v)]
    if v not in ok and "q7_cassandra_delai_releve" in m:
        y = m["q7_cassandra_delai_releve"]["med"] / 1000
        axes[1].plot([ok[-1], v], [res["mesures"][str(ok[-1])]["q7"]["cassandra"]["med"] / 1000, y],
                     color="black", ls=":", lw=1.2)
        axes[1].plot([v], [y], marker="o", ms=6, mfc="white", mec="black", ls="none")
        axes[1].plot([v], [12], marker="x", ms=7, color="black", ls="none")
        axes[1].annotate("échec à 12 s\n(délai par défaut)", (v, 12), xytext=(-8, -24), textcoords="offset points",
                         ha="right", fontsize=7.5)
        axes[1].annotate("délai relevé", (v, y), xytext=(-8, 4), textcoords="offset points", ha="right", fontsize=7.5)
axes[1].set_title("Requête non prévue (parcours complet)", fontsize=9.5)
axes[1].set_xticks(ans)
axes[1].set_xlabel("historique (années)")
axes[1].set_ylabel("secondes (médiane)")
axes[1].set_ylim(bottom=0)
axes[1].grid(axis="y")
axes[1].legend(frameon=False, loc="upper left")
fig.tight_layout()
fig.savefig(SORTIE / "fig12_froid_parcours.png", dpi=220)
print("graphiques ecrits dans", SORTIE)
