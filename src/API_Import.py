import os
import sys
import requests
from datetime import datetime

# Récupération du token depuis la variable d'environnement.
# Remplacez par votre token directement ici si vous souhaitez le coder en dur.
TOKEN = os.getenv("IDFM_API_TOKEN")
if not TOKEN:
    # Pour développement local / debug, ajoutez le token ici (non recommandé en production).
    TOKEN = "votre_token_ici"

if not TOKEN or TOKEN == "votre_token_ici":
    raise RuntimeError(
        "IDFM_API_TOKEN introuvable. Définissez la variable d'environnement IDFM_API_TOKEN "
        "ou mettez votre token directement dans le script."
    )

# URL PRIM unitaire
url = "https://api.idfmobilites.fr/coverage/sncf:OCE/vehicle_positions"  # Exemple: remplacer par votre endpoint
params = {
    # Exemple de paramètre reporté depuis PRIM
    # "filter[stop]": "stop_id",
    # "datetime": "YYYY-MM-DDTHH:MM:SSZ",
}

headers = {
    "apikey": TOKEN,
    # "Authorization": f"Bearer {TOKEN}"  # si l'API attend un Bearer token
    "Accept": "application/xml",
}

try:
    response = requests.get(url, params=params, headers=headers, timeout=30)
    response.raise_for_status()
except requests.RequestException as exc:
    print("Erreur lors de l'appel API :", exc, file=sys.stderr)
    sys.exit(1)

raw_dir = os.path.join("data", "raw", "realtime")
os.makedirs(raw_dir, exist_ok=True)
filename = os.path.join(
    raw_dir,
    f"stop_snapshot_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.xml",
)

with open(filename, "w", encoding="utf-8") as f:
    f.write(response.text)

print("OK", filename)
