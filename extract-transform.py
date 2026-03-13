import pandas as pd
from csv import QUOTE_NONNUMERIC

# Département cible — à modifier selon votre département
dept = '01'

# ============================================================
# EXTRACTION + TRANSFORMATION — Régions
# ============================================================

df_regions = pd.read_csv('data/regions-france.csv', sep=",", dtype=str)
df_regions.to_csv(f'clean/{dept}-regions.csv', sep=",", index=False, quoting=QUOTE_NONNUMERIC)

# ============================================================
# EXTRACTION + TRANSFORMATION — Départements
# ============================================================

df_deps = pd.read_csv('data/departements-france.csv', sep=",", dtype=str)
# Normalisation du code département sur 2 caractères
df_deps['code_departement'] = df_deps['code_departement'].str.zfill(2)
df_deps.to_csv(f'clean/{dept}-departements.csv', sep=",", index=False, quoting=QUOTE_NONNUMERIC)

# ============================================================
# EXTRACTION + TRANSFORMATION — Communes
# ============================================================

df_communes = pd.read_csv(
    'data/communes.csv', sep=",",
    # Forcer ces colonnes en str pour préserver les zéros initiaux
    dtype={'code_commune_INSEE': str, 'code_departement': str, 'code_region': str}
)
# Normalisation des codes : commune sur 5 chars, département et région sur 2 chars
df_communes['code_commune_INSEE'] = df_communes['code_commune_INSEE'].str.zfill(5)
df_communes['code_departement']   = df_communes['code_departement'].str.zfill(2)
df_communes['code_region']        = df_communes['code_region'].str.zfill(2)
# Sélection des colonnes utiles
df_communes = df_communes[['code_commune_INSEE', 'nom_commune', 'latitude', 'longitude', 'code_departement', 'code_region']]
# Filtrage sur le département cible
df_communes = df_communes[df_communes['code_departement'] == dept]
# Suppression des doublons sur le code INSEE
df_communes = df_communes.drop_duplicates(subset='code_commune_INSEE')
df_communes.to_csv(f'clean/{dept}-communes.csv', sep=",", index=False, quoting=QUOTE_NONNUMERIC)

# ============================================================
# EXTRACTION + TRANSFORMATION — Candidats (têtes de liste)
# ============================================================

df_candidats = pd.read_csv(
    'data/candidats-2026.csv', sep=";",
    dtype={'Code département': str, 'Code circonscription': str, 'CC': str}
)
# Normalisation des codes
df_candidats['Code département']    = df_candidats['Code département'].str.zfill(2)
df_candidats['Code circonscription'] = df_candidats['Code circonscription'].str.zfill(5)
# Filtrage : département cible + têtes de liste uniquement
df_candidats = df_candidats[df_candidats['Code département'] == dept]
df_candidats = df_candidats[df_candidats['Tête de liste'] == 'OUI']
df_candidats.to_csv(f'clean/{dept}-candidats.csv', sep=",", index=False, quoting=QUOTE_NONNUMERIC)

# ============================================================
# EXTRACTION + TRANSFORMATION — Données INSEE communes
# ============================================================

df_insee = pd.read_csv(
    'data/insee_communes.csv', sep=";",
    encoding="utf-8-sig", decimal=",",
    dtype={'CODGEO': str, 'REG': str, 'DEP': str}
)
# Normalisation des codes
df_insee['DEP']    = df_insee['DEP'].str.zfill(2)
df_insee['REG']    = df_insee['REG'].str.zfill(2)
df_insee['CODGEO'] = df_insee['CODGEO'].str.zfill(5)
# Filtrage sur le département cible
df_insee = df_insee[df_insee['DEP'] == dept]
df_insee.to_csv(f'clean/{dept}-insee.csv', sep=",", index=False, quoting=QUOTE_NONNUMERIC)

print(f"ETL terminé pour le département {dept}.")
print(f"  clean/{dept}-regions.csv")
print(f"  clean/{dept}-departements.csv")
print(f"  clean/{dept}-communes.csv     ({len(df_communes)} lignes)")
print(f"  clean/{dept}-candidats.csv    ({len(df_candidats)} lignes)")
print(f"  clean/{dept}-insee.csv        ({len(df_insee)} lignes)")
