import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import requests
from dotenv import load_dotenv

from config import local_data_root, monitoring_refs

load_dotenv()

BASE_URL = "https://prim.iledefrance-mobilites.fr/marketplace/stop-monitoring"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def output_directory() -> Path:
    output_dir = local_data_root() / "raw" / "realtime"
    output_dir.mkdir(parents=True, exist_ok=True)
    return output_dir


def api_headers() -> Dict[str, str]:
    token = os.getenv("IDFM_API_TOKEN")
    if not token:
        raise RuntimeError("IDFM_API_TOKEN introuvable dans le fichier .env")
    return {
        "apiKey": token,
        "Accept": "application/json",
    }


def extract_visits(payload: Dict[str, Any]) -> List[Any]:
    return (
        payload.get("Siri", {})
        .get("ServiceDelivery", {})
        .get("StopMonitoringDelivery", [{}])[0]
        .get("MonitoredStopVisit", [])
    )


def fetch_snapshot(monitoring_ref: str, headers: Dict[str, str]) -> Dict[str, Any]:
    response = requests.get(
        BASE_URL,
        params={"MonitoringRef": monitoring_ref},
        headers=headers,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def save_payload(
    raw_dir: Path,
    monitoring_ref: str,
    payload: Dict[str, Any],
    collected_at: datetime,
) -> Path:
    filename = raw_dir / (
        f"snapshot_{monitoring_ref.replace(':', '_')}_"
        f"{collected_at.strftime('%Y%m%d_%H%M%S')}.json"
    )
    with open(filename, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    return filename


def save_report(raw_dir: Path, results: List[Dict[str, Any]], collected_at: datetime) -> None:
    report_path = raw_dir / f"collection_report_{collected_at.strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_path, "w", encoding="utf-8") as handle:
        json.dump(results, handle, ensure_ascii=False, indent=2)


def main() -> None:
    headers = api_headers()
    raw_dir = output_directory()
    results = []

    for monitoring_ref in monitoring_refs():
        collected_at = utc_now()
        try:
            payload = fetch_snapshot(monitoring_ref, headers)
            visits = extract_visits(payload)
            snapshot_path = save_payload(raw_dir, monitoring_ref, payload, collected_at)
            result = {
                "monitoring_ref": monitoring_ref,
                "status_code": 200,
                "nb_passages": len(visits),
                "ok": True,
                "snapshot_file": snapshot_path.name,
                "collected_at": collected_at.isoformat(),
            }
            print(
                "{0} -> status=200, passages={1}, fichier={2}".format(
                    monitoring_ref,
                    len(visits),
                    snapshot_path.name,
                )
            )
        except requests.RequestException as exc:
            result = {
                "monitoring_ref": monitoring_ref,
                "status_code": None,
                "nb_passages": 0,
                "ok": False,
                "error": str(exc),
                "collected_at": collected_at.isoformat(),
            }
            print("{0} -> erreur: {1}".format(monitoring_ref, exc))

        results.append(result)

    save_report(raw_dir, results, utc_now())

    print("\nResume de collecte:")
    for result in results:
        print(result)


if __name__ == "__main__":
    main()
