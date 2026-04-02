from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw" / "gtfs" / "IDFM-gtfs"
PROCESSED_DIR = BASE_DIR / "data" / "processed" / "gtfs"

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def process_file(file_path):
    print(f"Traitement: {file_path.name}")
    try:
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip()
        df = df.dropna(how="all")

        output_path = PROCESSED_DIR / f"{file_path.stem}.csv"
        df.to_csv(output_path, index=False)
        print(f"✅ Sauvé: {output_path} ({df.shape})")
    except Exception as e:
        print(f"❌ Erreur {file_path.name}: {e}")

def main():
    print("RAW_DIR =", RAW_DIR)
    print("Existe ?", RAW_DIR.exists())

    files = list(RAW_DIR.glob("*.txt"))
    if not files:
        print("⚠️ Aucun fichier .txt trouvé")
        return

    for file in files:
        process_file(file)

if __name__ == "__main__":
    main()