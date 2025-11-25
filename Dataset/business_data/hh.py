import pandas as pd


# --- Chargement des fichiers avec gestion automatique de l'encodage ---
def load_csv_with_encoding(path):
    encodings_to_try = ["utf-8", "ISO-8859-1", "latin1", "cp1252"]

    for enc in encodings_to_try:
        try:
            print(f"Lecture de {path} avec encodage : {enc}")
            return pd.read_csv(path, encoding=enc)
        except Exception as e:
            print(f"Échec avec {enc} : {e}")

    raise ValueError(f"Impossible de lire le fichier {path} avec les encodages connus.")


# Charger les deux fichiers
bug_reports = load_csv_with_encoding("project_bug_reports.csv")
tunis = load_csv_with_encoding("projects_france.csv")

# Vérifier que la colonne project_id existe
if "project_id" not in bug_reports.columns:
    raise KeyError("La colonne 'project_id' est absente de project_bug_reports.csv")

if "project_id" not in tunis.columns:
    raise KeyError("La colonne 'project_id' est absente de projet_tunis.csv")

# --- Filtrage des lignes avec project_id correspondant ---
tunis_ids = set(tunis["project_id"])

filtered = bug_reports[bug_reports["project_id"].isin(tunis_ids)]

# --- Sauvegarde du résultat ---
filtered.to_csv("project_bug_reports_france.csv", index=False, encoding="utf-8")

print("\n🎉 Fichier 'project_bug_reports_tunis.csv' généré avec succès !")
print(f"Lignes filtrées : {len(filtered)}")
