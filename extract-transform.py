import pandas as pd
import os

# ============================================================
# CONFIGURATION — Changez le code de votre département ici
# ============================================================
DEPT_CODE = "67"  # Ex: "67" pour Bas-Rhin, "75" pour Paris...
# ============================================================

DATA_DIR = "data"
CLEAN_DIR = "clean"
os.makedirs(CLEAN_DIR, exist_ok=True)

print(f"Département cible : {DEPT_CODE}")

# ============================================================
# EXTRACTION
# ============================================================

print("\n--- Chargement des fichiers ---")

# candidats : séparateur ; , encodage utf-8
df_candidats = pd.read_csv(
    f"{DATA_DIR}/candidats-2026.csv",
    sep=";",
    encoding="utf-8",
    dtype=str,
)

# communes : séparateur , , code numérique → string
df_communes = pd.read_csv(
    f"{DATA_DIR}/communes.csv",
    sep=",",
    encoding="utf-8",
    dtype=str,
)

# insee_communes : séparateur ; , BOM utf-8, décimales avec virgule
df_insee = pd.read_csv(
    f"{DATA_DIR}/insee_communes.csv",
    sep=";",
    encoding="utf-8-sig",
    decimal=",",
    dtype=str,
)

# departements : séparateur ,
df_depts = pd.read_csv(
    f"{DATA_DIR}/departements-france.csv",
    sep=",",
    encoding="utf-8",
    dtype=str,
)

print(f"  candidats-2026  : {len(df_candidats):>7} lignes")
print(f"  communes        : {len(df_communes):>7} lignes")
print(f"  insee_communes  : {len(df_insee):>7} lignes")
print(f"  departements    : {len(df_depts):>7} lignes")

# ============================================================
# TRANSFORMATION — Unification des codes
# ============================================================

print("\n--- Transformation des codes ---")

# --- candidats-2026 ---
# Code département : déjà "01" mais nettoyage des espaces/guillemets résiduels
df_candidats["Code département"] = (
    df_candidats["Code département"].str.strip().str.zfill(2)
)
# Code circonscription (= code commune) : déjà "01001"
df_candidats["Code circonscription"] = (
    df_candidats["Code circonscription"].str.strip().str.zfill(5)
)

# --- communes ---
# code_commune_INSEE : "1001" → "01001" (5 chars)
df_communes["code_commune_INSEE"] = (
    df_communes["code_commune_INSEE"].str.strip().str.zfill(5)
)
# code_departement : "1" → "01" (2 chars)
df_communes["code_departement"] = (
    df_communes["code_departement"].str.strip().str.zfill(2)
)

# --- insee_communes ---
# CODGEO : "1001" → "01001"
df_insee["CODGEO"] = df_insee["CODGEO"].str.strip().str.zfill(5)
# DEP : "1" → "01"
df_insee["DEP"] = df_insee["DEP"].str.strip().str.zfill(2)

# --- departements ---
# code_departement : "1" → "01"
df_depts["code_departement"] = (
    df_depts["code_departement"].str.strip().str.zfill(2)
)

print("  Codes unifiés : département sur 2 chars, commune sur 5 chars")

# ============================================================
# TRANSFORMATION — Filtrage têtes de liste
# ============================================================

print("\n--- Filtrage des têtes de liste ---")

df_tetes = df_candidats[df_candidats["Tête de liste"].str.strip() == "OUI"].copy()
print(f"  Têtes de liste : {len(df_tetes)} (sur {len(df_candidats)} candidats)")

# ============================================================
# TRANSFORMATION — Filtrage par département
# ============================================================

print(f"\n--- Filtrage département {DEPT_CODE} ---")

dept_2 = DEPT_CODE.zfill(2)

df_candidats_dept = df_candidats[df_candidats["Code département"] == dept_2].copy()
df_tetes_dept     = df_tetes[df_tetes["Code département"] == dept_2].copy()
df_communes_dept  = df_communes[df_communes["code_departement"] == dept_2].copy()
df_insee_dept     = df_insee[df_insee["DEP"] == dept_2].copy()

print(f"  candidats (tous)  dept {dept_2} : {len(df_candidats_dept)}")
print(f"  têtes de liste    dept {dept_2} : {len(df_tetes_dept)}")
print(f"  communes          dept {dept_2} : {len(df_communes_dept)}")
print(f"  insee_communes    dept {dept_2} : {len(df_insee_dept)}")

if len(df_candidats_dept) == 0:
    print(f"\n  ATTENTION : aucune donnée pour le département '{dept_2}'.")
    print("  Vérifiez la valeur de DEPT_CODE en tête de script.")

# ============================================================
# SAUVEGARDE — Écriture des fichiers nettoyés dans clean/
# ============================================================

print(f"\n--- Sauvegarde dans {CLEAN_DIR}/ ---")

df_tetes_dept.to_csv(
    f"{CLEAN_DIR}/candidats-tetes-{dept_2}.csv", index=False, encoding="utf-8"
)
df_communes_dept.to_csv(
    f"{CLEAN_DIR}/communes-{dept_2}.csv", index=False, encoding="utf-8"
)
df_insee_dept.to_csv(
    f"{CLEAN_DIR}/insee-communes-{dept_2}.csv", index=False, encoding="utf-8"
)
df_depts.to_csv(
    f"{CLEAN_DIR}/departements.csv", index=False, encoding="utf-8"
)

print(f"  clean/candidats-tetes-{dept_2}.csv  ({len(df_tetes_dept)} lignes)")
print(f"  clean/communes-{dept_2}.csv          ({len(df_communes_dept)} lignes)")
print(f"  clean/insee-communes-{dept_2}.csv    ({len(df_insee_dept)} lignes)")
print(f"  clean/departements.csv               ({len(df_depts)} lignes)")
print("\nETL terminé.")
