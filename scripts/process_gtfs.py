from pathlib import Path
import pandas as pd

RAW_DIR = Path("data/raw/gtfs/IDFM")
PROCESSED_DIR = Path("data/processed/gtfs")

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def process_file(file_path):
    print(f"Traitement: {file_path.name}")

    try:
        df = pd.read_csv(file_path)

        # Nettoyage simple (important pour la suite)
        df.columns = df.columns.str.strip()
        
        # Supprimer lignes complètement vides
        df = df.dropna(how="all")

        # Sauvegarde
        output_path = PROCESSED_DIR / f"{file_path.stem}.csv"
        df.to_csv(output_path, index=False)

        print(f"✅ Sauvé: {output_path.name} ({df.shape})")

    except Exception as e:
        print(f"❌ Erreur {file_path.name}: {e}")


def main():
    files = list(RAW_DIR.glob("*.txt"))

    if not files:
        print("⚠️ Aucun fichier trouvé")
        return

    for file in files:
        process_file(file)


if __name__ == "__main__":
    main()