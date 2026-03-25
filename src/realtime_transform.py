import json
from pathlib import Path
import pandas as pd

RAW_DIR = Path("data") / "raw" / "realtime"
PROCESSED_DIR = Path("data") / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

records = []

json_files = sorted(RAW_DIR.glob("*.json"))

if not json_files:
    raise RuntimeError("Aucun fichier JSON trouvé dans data/raw/realtime")

for file_path in json_files:
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    deliveries = (
        data.get("Siri", {})
        .get("ServiceDelivery", {})
        .get("StopMonitoringDelivery", [])
    )

    for delivery in deliveries:
        visits = delivery.get("MonitoredStopVisit", [])

        for visit in visits:
            journey = visit.get("MonitoredVehicleJourney", {})
            monitored_call = journey.get("MonitoredCall", {})

            record = {
                "source_file": file_path.name,
                "recorded_at_time": visit.get("RecordedAtTime"),
                "monitoring_ref": visit.get("MonitoringRef", {}).get("value"),
                "line_ref": journey.get("LineRef", {}).get("value"),
                "operator_ref": journey.get("OperatorRef", {}).get("value"),
                "dated_vehicle_journey_ref": journey.get("FramedVehicleJourneyRef", {}).get("DatedVehicleJourneyRef"),
                "direction_name": (journey.get("DirectionName") or [{}])[0].get("value"),
                "destination_ref": journey.get("DestinationRef", {}).get("value"),
                "destination_name": (journey.get("DestinationName") or [{}])[0].get("value"),
                "stop_point_name": (monitored_call.get("StopPointName") or [{}])[0].get("value"),
                "vehicle_at_stop": monitored_call.get("VehicleAtStop"),
                "destination_display": (monitored_call.get("DestinationDisplay") or [{}])[0].get("value"),
                "expected_departure_time": monitored_call.get("ExpectedDepartureTime"),
                "departure_status": monitored_call.get("DepartureStatus"),
                "arrival_status": monitored_call.get("ArrivalStatus"),
                "direction_ref": journey.get("DirectionRef", {}).get("value"),
                "destination_short_name": (journey.get("DestinationShortName") or [{}])[0].get("value"),
            }

            records.append(record)

df = pd.DataFrame(records)

if df.empty:
    print("Aucun passage exploitable trouvé.")
else:
    output_csv = PROCESSED_DIR / "realtime/realtime_passages.csv"
    output_parquet = PROCESSED_DIR / "realtime/realtime_passages.parquet"

    df.to_csv(output_csv, index=False)
    df.to_parquet(output_parquet, index=False)

    print(f"OK : {len(df)} passages extraits")
    print(f"CSV : {output_csv}")
    print(f"Parquet : {output_parquet}")
    print("\nAperçu :")
    print(df.head())