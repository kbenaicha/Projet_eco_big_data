import os
import json
from pathlib import Path
from datetime import datetime

import requests
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("IDFM_API_TOKEN")
if not TOKEN:
    raise RuntimeError("IDFM_API_TOKEN introuvable dans le fichier .env")

BASE_URL = "https://prim.iledefrance-mobilites.fr/marketplace/stop-monitoring"

# Quelques MonitoringRef plausibles à tester
MONITORING_REFS = [
    "STIF:StopPoint:Q:463158:",   # exemple PRIM
    "STIF:StopArea:SP:71517:",    # exemple public
    "STIF:StopPoint:Q:41442:",    # exemple public
    "STIF:StopArea:SP:43032:",    # exemple public
    "STIF:StopPoint:Q:36384:",    # exemple public
]

headers = {
    "apiKey": TOKEN,
    "Accept": "application/json",
}

raw_dir = Path("data") / "raw" / "realtime"
raw_dir.mkdir(parents=True, exist_ok=True)

results = []

for monitoring_ref in MONITORING_REFS:
    try:
        response = requests.get(
            BASE_URL,
            params={"MonitoringRef": monitoring_ref},
            headers=headers,
            timeout=30,
        )

        status_code = response.status_code

        try:
            payload = response.json()
        except Exception:
            payload = None

        visits = []
        if payload:
            visits = (
                payload.get("Siri", {})
                .get("ServiceDelivery", {})
                .get("StopMonitoringDelivery", [{}])[0]
                .get("MonitoredStopVisit", [])
            )

        result = {
            "monitoring_ref": monitoring_ref,
            "status_code": status_code,
            "nb_passages": len(visits),
            "ok": status_code == 200 and len(visits) > 0,
        }
        results.append(result)

        print(
            f"{monitoring_ref} -> status={status_code}, passages={len(visits)}"
        )

        # Sauvegarde seulement les réponses utiles
        if status_code == 200 and payload is not None:
            filename = raw_dir / (
                f"snapshot_{monitoring_ref.replace(':', '_')}_"
                f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.json"
            )
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)

    except requests.RequestException as exc:
        print(f"{monitoring_ref} -> erreur : {exc}")
        results.append(
            {
                "monitoring_ref": monitoring_ref,
                "status_code": None,
                "nb_passages": 0,
                "ok": False,
                "error": str(exc),
            }
        )

print("\nRésumé :")
for r in results:
    print(r)