import csv
import html
import os
from pathlib import Path
from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, countDistinct, desc, round as spark_round, sum as spark_sum, when

from config import data_path
from spark_session import create_spark_session

HYBRID_PATH = data_path("final/hybrid_transport_monitoring.parquet")
LINE_PERFORMANCE_PATH = data_path("analytics/line_performance")
STOP_PERFORMANCE_PATH = data_path("analytics/stop_performance")
DISRUPTION_REASONS_PATH = data_path("analytics/disruption_reasons")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
REPORT_DIR_VALUE = Path(os.getenv("REPORT_DIR", "reports"))
REPORT_DIR = REPORT_DIR_VALUE if REPORT_DIR_VALUE.is_absolute() else PROJECT_ROOT / REPORT_DIR_VALUE
EXPORTS_DIR = REPORT_DIR / "exports"
ASSETS_DIR = REPORT_DIR / "assets"
SUMMARY_PATH = REPORT_DIR / "results_summary.md"
DASHBOARD_PATH = REPORT_DIR / "results_dashboard.html"

DISRUPTION_REASON_LABELS = {
    "cancelled_service": "Service annule",
    "missing_realtime_status": "Statut temps reel manquant",
    "delay": "Retard",
    "normal": "Normal",
}


def ensure_directories() -> None:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    EXPORTS_DIR.mkdir(parents=True, exist_ok=True)
    ASSETS_DIR.mkdir(parents=True, exist_ok=True)


def to_records(df: DataFrame, limit: int) -> List[Dict[str, object]]:
    return [row.asDict(recursive=True) for row in df.limit(limit).collect()]


def write_csv(path: Path, rows: List[Dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fieldnames = list(rows[0].keys())
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def pct(numerator: int, denominator: int) -> float:
    if not denominator:
        return 0.0
    return round((numerator / denominator) * 100, 2)


def format_number(value: object) -> str:
    if value is None:
        return "0"
    if isinstance(value, int):
        return f"{value:,}".replace(",", " ")
    if isinstance(value, float):
        if value.is_integer():
            return f"{int(value):,}".replace(",", " ")
        return f"{value:.2f}".replace(".", ",")
    return str(value)


def format_percent(value: object) -> str:
    if value is None:
        return "0 %"
    return f"{float(value):.2f} %".replace(".", ",")


def line_label(row: Dict[str, object]) -> str:
    line_code = str(row.get("line_code") or "ligne inconnue")
    route_short_name = row.get("route_short_name")
    if route_short_name and route_short_name != line_code:
        return f"{line_code} ({route_short_name})"
    return line_code


def reason_label(value: object) -> str:
    raw_value = str(value or "inconnu")
    return DISRUPTION_REASON_LABELS.get(raw_value, raw_value.replace("_", " "))


def format_minutes(value: object) -> str:
    if value is None:
        return "0 min"
    return f"{float(value):.2f} min".replace(".", ",")


def has_rows(df: DataFrame) -> bool:
    return df.limit(1).count() > 0


def select_top_rows(df: DataFrame, preferred_filter, preferred_order, fallback_order, limit: int) -> List[Dict[str, object]]:
    filtered = df.filter(preferred_filter)
    if has_rows(filtered):
        return to_records(filtered.orderBy(*preferred_order), limit)
    return to_records(df.orderBy(*fallback_order), limit)


def svg_bar_chart(title: str, rows: List[Dict[str, object]], label_key: str, value_key: str, suffix: str) -> str:
    width = 760
    left_margin = 250
    chart_width = 430
    row_height = 34
    top_margin = 48
    height = top_margin + max(1, len(rows)) * row_height + 24

    if not rows:
        return (
            f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="120" viewBox="0 0 {width} 120">'
            f'<style>text{{font-family:Arial,sans-serif;fill:#1c1917}} .title{{font-size:18px;font-weight:bold}}</style>'
            f'<text class="title" x="16" y="28">{html.escape(title)}</text>'
            '<text x="16" y="64">Aucune donnee disponible</text>'
            "</svg>"
        )

    max_value = max(float(row.get(value_key) or 0.0) for row in rows) or 1.0
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        "<style>",
        "text{font-family:Arial,sans-serif;fill:#1c1917}",
        ".title{font-size:18px;font-weight:bold}",
        ".label{font-size:13px}",
        ".value{font-size:12px;font-weight:bold}",
        "</style>",
        f'<text class="title" x="16" y="28">{html.escape(title)}</text>',
    ]

    for index, row in enumerate(rows):
        y = top_margin + index * row_height
        raw_value = float(row.get(value_key) or 0.0)
        bar_width = max(2.0, (raw_value / max_value) * chart_width)
        label = html.escape(str(row.get(label_key) or "inconnu"))
        if suffix == "%":
            value_text = f"{raw_value:.2f} %".replace(".", ",")
        else:
            value_text = f"{int(raw_value):,} {suffix}".replace(",", " ").strip()
        parts.extend(
            [
                f'<text class="label" x="16" y="{y + 17}">{label}</text>',
                f'<rect x="{left_margin}" y="{y}" width="{bar_width:.1f}" height="18" rx="4" fill="#2563eb"/>',
                f'<text class="value" x="{left_margin + bar_width + 10:.1f}" y="{y + 14}">{html.escape(value_text)}</text>',
            ]
        )

    parts.append("</svg>")
    return "".join(parts)


def write_svg_chart(path: Path, title: str, rows: List[Dict[str, object]], label_key: str, value_key: str, suffix: str) -> None:
    path.write_text(svg_bar_chart(title, rows, label_key, value_key, suffix), encoding="utf-8")


def markdown_table(rows: List[Dict[str, object]], columns: List[str]) -> str:
    if not rows:
        return "_Aucune donnee disponible._"

    header = "| " + " | ".join(columns) + " |"
    separator = "| " + " | ".join(["---"] * len(columns)) + " |"
    body = []
    for row in rows:
        values = []
        for column in columns:
            value = row.get(column)
            if isinstance(value, float):
                values.append(format_number(value))
            else:
                values.append(str(value))
        body.append("| " + " | ".join(values) + " |")
    return "\n".join([header, separator] + body)


def html_table(rows: List[Dict[str, object]], columns: List[str]) -> str:
    if not rows:
        return "<p>Aucune donnee disponible.</p>"

    headers = "".join(f"<th>{html.escape(column)}</th>" for column in columns)
    body_rows = []
    for row in rows:
        cells = "".join(f"<td>{html.escape(str(row.get(column, '')))}</td>" for column in columns)
        body_rows.append(f"<tr>{cells}</tr>")
    return f"<table><thead><tr>{headers}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"


def build_summary_markdown(
    metrics: Dict[str, object],
    top_lines: List[Dict[str, object]],
    top_stops: List[Dict[str, object]],
    reasons: List[Dict[str, object]],
    sample_events: List[Dict[str, object]],
) -> str:
    insights = [
        f"Le jeu de donnees couvre **{format_number(metrics['distinct_lines'])} lignes** et **{format_number(metrics['distinct_stops'])} arrets**.",
        f"Sur **{format_number(metrics['total_observations'])} observations**, **{format_percent(metrics['disruption_rate_pct'])}** sont perturbees et **{format_percent(metrics['cancellation_rate_pct'])}** sont annulees.",
        f"Le retard moyen observe sur le reseau est de **{format_number(metrics['network_avg_delay_minutes'])} minutes**.",
    ]

    if top_lines:
        worst_line = top_lines[0]
        insights.append(
            "La ligne la plus exposee est **{0}** avec **{1}** d'observations perturbees.".format(
                line_label(worst_line),
                format_percent(worst_line.get("disruption_rate_pct")),
            )
        )

    if top_stops:
        worst_stop = top_stops[0]
        insights.append(
            "L'arret le plus fragile est **{0}** avec un retard moyen de **{1} minutes**.".format(
                worst_stop.get("stop_point_name", "inconnu"),
                format_number(worst_stop.get("avg_delay_minutes")),
            )
        )

    if reasons:
        top_reason = reasons[0]
        insights.append(
            "Le motif de perturbation dominant est **{0}** avec **{1} evenements**.".format(
                top_reason.get("motif", "inconnu"),
                format_number(top_reason.get("events")),
            )
        )

    return "\n".join(
        [
            "# Resultats interpretable du reseau",
            "",
            "## Ce qu'on apprend rapidement",
            *[f"- {item}" for item in insights],
            "",
            "## Chiffres cles",
            f"- Observations analysees : **{format_number(metrics['total_observations'])}**",
            f"- Lignes observees : **{format_number(metrics['distinct_lines'])}**",
            f"- Arrets observes : **{format_number(metrics['distinct_stops'])}**",
            f"- Taux de retard : **{format_percent(metrics['delay_rate_pct'])}**",
            f"- Taux de perturbation : **{format_percent(metrics['disruption_rate_pct'])}**",
            f"- Taux d'annulation : **{format_percent(metrics['cancellation_rate_pct'])}**",
            f"- Retard moyen reseau : **{format_number(metrics['network_avg_delay_minutes'])} minutes**",
            "",
            "## Top lignes les plus perturbees",
            markdown_table(
                [
                    {
                        "ligne": line_label(row),
                        "observations": row.get("observations"),
                        "retard_moyen_min": row.get("avg_delay_minutes"),
                        "taux_perturbation_pct": row.get("disruption_rate_pct"),
                    }
                    for row in top_lines
                ],
                ["ligne", "observations", "retard_moyen_min", "taux_perturbation_pct"],
            ),
            "",
            "## Top arrets les plus sensibles",
            markdown_table(
                [
                    {
                        "arret": row.get("stop_point_name"),
                        "observations": row.get("observations"),
                        "retard_moyen_min": row.get("avg_delay_minutes"),
                        "taux_perturbation_pct": row.get("disruption_rate_pct"),
                    }
                    for row in top_stops
                ],
                ["arret", "observations", "retard_moyen_min", "taux_perturbation_pct"],
            ),
            "",
            "## Motifs de perturbation",
            markdown_table(
                reasons,
                ["motif", "events", "share_pct"],
            ),
            "",
            "## Exemples concrets d'observations perturbees",
            markdown_table(
                sample_events,
                [
                    "line_code",
                    "stop_point_name",
                    "expected_departure_time",
                    "delay_minutes",
                    "disruption_reason",
                ],
            ),
            "",
            "## Livrables generes",
            "- `reports/results_summary.md`",
            "- `reports/results_dashboard.html`",
            "- `reports/exports/network_overview.csv`",
            "- `reports/exports/top_lines.csv`",
            "- `reports/exports/top_stops.csv`",
            "- `reports/exports/disruption_reasons.csv`",
            "- `reports/exports/sample_disruptions.csv`",
            "- `reports/assets/top_lines.svg`",
            "- `reports/assets/top_stops.svg`",
            "- `reports/assets/disruption_reasons.svg`",
        ]
    )


def build_dashboard_html(
    metrics: Dict[str, object],
    top_lines: List[Dict[str, object]],
    top_stops: List[Dict[str, object]],
    reasons: List[Dict[str, object]],
    sample_events: List[Dict[str, object]],
) -> str:
    cards = [
        ("Observations analysees", format_number(metrics["total_observations"])),
        ("Lignes observees", format_number(metrics["distinct_lines"])),
        ("Arrets observes", format_number(metrics["distinct_stops"])),
        ("Retard moyen", format_minutes(metrics["network_avg_delay_minutes"])),
        ("Taux de perturbation", format_percent(metrics["disruption_rate_pct"])),
        ("Taux d'annulation", format_percent(metrics["cancellation_rate_pct"])),
    ]
    cards_html = "".join(
        f'<div class="card"><div class="card-label">{html.escape(label)}</div><div class="card-value">{html.escape(value)}</div></div>'
        for label, value in cards
    )

    highlights = [
        "Cette page transforme les sorties Spark en constats faciles a lire.",
        "Les taux affiches permettent d'identifier rapidement les zones les plus sensibles du reseau.",
    ]
    if top_lines:
        highlights.append(
            "La ligne a surveiller en priorite est {0} avec {1} de perturbations.".format(
                html.escape(line_label(top_lines[0])),
                format_percent(top_lines[0].get("disruption_rate_pct")),
            )
        )
    if top_stops:
        highlights.append(
            "L'arret le plus sensible est {0} avec {1} de perturbations.".format(
                html.escape(str(top_stops[0].get("stop_point_name") or "inconnu")),
                format_percent(top_stops[0].get("disruption_rate_pct")),
            )
        )
    if reasons:
        highlights.append(
            "Le motif dominant est {0}.".format(
                html.escape(str(reasons[0].get("motif") or "inconnu"))
            )
        )

    top_lines_table = html_table(
        [
            {
                "Ligne": line_label(row),
                "Observations": row.get("observations"),
                "Retard moyen": format_minutes(row.get("avg_delay_minutes")),
                "Perturbation": format_percent(row.get("disruption_rate_pct")),
            }
            for row in top_lines
        ],
        ["Ligne", "Observations", "Retard moyen", "Perturbation"],
    )
    top_stops_table = html_table(
        [
            {
                "Arret": row.get("stop_point_name"),
                "Observations": row.get("observations"),
                "Retard moyen": format_minutes(row.get("avg_delay_minutes")),
                "Perturbation": format_percent(row.get("disruption_rate_pct")),
            }
            for row in top_stops
        ],
        ["Arret", "Observations", "Retard moyen", "Perturbation"],
    )
    reasons_table = html_table(
        [
            {
                "Motif": row.get("motif"),
                "Evenements": row.get("events"),
                "Part": format_percent(row.get("share_pct")),
            }
            for row in reasons
        ],
        ["Motif", "Evenements", "Part"],
    )
    sample_table = html_table(
        [
            {
                "Ligne": row.get("line_code"),
                "Arret": row.get("stop_point_name"),
                "Depart prevu": row.get("expected_departure_time"),
                "Retard": format_minutes(row.get("delay_minutes")),
                "Motif": row.get("disruption_reason"),
            }
            for row in sample_events
        ],
        ["Ligne", "Arret", "Depart prevu", "Retard", "Motif"],
    )

    return f"""<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="utf-8">
  <title>Dashboard Resultats Transport</title>
  <style>
    body {{
      font-family: Arial, sans-serif;
      margin: 0;
      background: #f4f6f8;
      color: #222;
    }}
    .page {{
      max-width: 1100px;
      margin: 0 auto;
      padding: 28px 20px 40px;
    }}
    h1, h2, h3 {{
      margin: 0 0 14px;
    }}
    p, li {{
      line-height: 1.5;
    }}
    .hero {{
      background: #16324f;
      color: #fff;
      padding: 24px;
      border-radius: 16px;
      margin-bottom: 20px;
    }}
    .hero p {{
      margin: 8px 0 0;
    }}
    .cards {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 12px;
      margin: 20px 0;
    }}
    .card {{
      background: white;
      border-radius: 12px;
      padding: 16px;
      border: 1px solid #d9e2ec;
    }}
    .card-label {{
      font-size: 12px;
      color: #52606d;
      margin-bottom: 6px;
    }}
    .card-value {{
      font-size: 26px;
      font-weight: 700;
      color: #102a43;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 16px;
      margin-bottom: 20px;
    }}
    .panel {{
      background: white;
      border-radius: 12px;
      padding: 18px;
      border: 1px solid #d9e2ec;
    }}
    img {{
      width: 100%;
      height: auto;
      border-radius: 8px;
      background: #f8fafc;
      border: 1px solid #e5e7eb;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 14px;
    }}
    th, td {{
      text-align: left;
      padding: 9px 6px;
      border-bottom: 1px solid #e5e7eb;
      vertical-align: top;
    }}
    th {{
      color: #52606d;
      font-weight: 600;
    }}
    .section-note {{
      color: #52606d;
      margin: 0 0 14px;
    }}
    .highlights {{
      margin: 0;
      padding-left: 18px;
    }}
    .footer {{
      color: #52606d;
      font-size: 14px;
    }}
    .footer code {{
      background: #eef2f7;
      padding: 2px 6px;
      border-radius: 6px;
    }}
  </style>
</head>
<body>
  <div class="page">
    <section class="hero">
      <h1>Dashboard de resultats</h1>
      <p>Lecture simple des resultats du projet: etat global du reseau, lignes et arrets sensibles, motifs de perturbation et exemples concrets.</p>
    </section>

    <section class="cards">
      {cards_html}
    </section>

    <section class="panel" style="margin-bottom: 20px;">
      <h2>Resume en 30 secondes</h2>
      <p class="section-note">Si tu dois presenter le projet rapidement, ce bloc suffit a raconter l'essentiel.</p>
      <ul class="highlights">
        {''.join(f'<li>{item}</li>' for item in highlights)}
      </ul>
    </section>

    <section class="grid">
      <div class="panel">
        <h2>1. Ou observe-t-on les perturbations ?</h2>
        <p class="section-note">Ce graphique montre les lignes les plus touchees.</p>
        <img src="assets/top_lines.svg" alt="Top lignes perturbees">
      </div>
      <div class="panel">
        <h2>2. Quels arrets sont les plus sensibles ?</h2>
        <p class="section-note">On identifie ici les arrets ou les perturbations se concentrent.</p>
        <img src="assets/top_stops.svg" alt="Top arrets sensibles">
      </div>
    </section>

    <section class="grid">
      <div class="panel">
        <h2>3. Pourquoi les trajets sont perturbes ?</h2>
        <p class="section-note">Les motifs les plus frequents permettent de comprendre l'origine des incidents.</p>
        <img src="assets/disruption_reasons.svg" alt="Motifs de perturbation">
      </div>
      <div class="panel">
        <h2>Top lignes</h2>
        <p class="section-note">Tableau de lecture rapide pour le rendu ou une capture d'ecran.</p>
        {top_lines_table}
      </div>
    </section>

    <section class="grid">
      <div class="panel">
        <h2>Top arrets</h2>
        <p class="section-note">Les arrets avec le plus de perturbations ou de retards moyens eleves.</p>
        {top_stops_table}
      </div>
      <div class="panel">
        <h2>Motifs de perturbation</h2>
        <p class="section-note">Version tabulaire des causes principales.</p>
        {reasons_table}
      </div>
    </section>

    <section class="panel">
      <h2>Exemples concrets</h2>
      <p class="section-note">Quelques observations reelles pour montrer comment les perturbations apparaissent dans les donnees.</p>
      {sample_table}
    </section>

    <section class="panel footer" style="margin-top: 20px;">
      <h2>Fichiers utiles</h2>
      <p>Version texte : <code>reports/results_summary.md</code></p>
      <p>Exports : <code>reports/exports/</code></p>
      <p>Graphiques : <code>reports/assets/</code></p>
    </section>
  </div>
</body>
</html>
"""


def main() -> None:
    ensure_directories()

    spark = create_spark_session("BuildResultsReport")

    hybrid = spark.read.parquet(HYBRID_PATH)
    line_performance = spark.read.parquet(LINE_PERFORMANCE_PATH)
    stop_performance = spark.read.parquet(STOP_PERFORMANCE_PATH)
    disruption_reasons = spark.read.parquet(DISRUPTION_REASONS_PATH).filter(col("disruption_reason") != "normal")

    metrics = (
        hybrid.agg(
            count("*").alias("total_observations"),
            countDistinct("line_code").alias("distinct_lines"),
            countDistinct("stop_point_name").alias("distinct_stops"),
            spark_sum(when(col("is_delayed"), 1).otherwise(0)).alias("delayed_observations"),
            spark_sum(when(col("is_disrupted"), 1).otherwise(0)).alias("disrupted_observations"),
            spark_sum(when(col("is_cancelled"), 1).otherwise(0)).alias("cancelled_observations"),
            spark_round(avg("delay_minutes"), 2).alias("network_avg_delay_minutes"),
        )
        .first()
        .asDict()
    )

    total_observations = int(metrics["total_observations"])
    delayed_observations = int(metrics["delayed_observations"])
    disrupted_observations = int(metrics["disrupted_observations"])
    cancelled_observations = int(metrics["cancelled_observations"])

    metrics["delay_rate_pct"] = pct(delayed_observations, total_observations)
    metrics["disruption_rate_pct"] = pct(disrupted_observations, total_observations)
    metrics["cancellation_rate_pct"] = pct(cancelled_observations, total_observations)

    top_lines = select_top_rows(
        line_performance,
        preferred_filter=col("disruption_rate_pct") > 0,
        preferred_order=[desc("disruption_rate_pct"), desc("observations"), desc("avg_delay_minutes")],
        fallback_order=[desc("observations"), desc("avg_delay_minutes")],
        limit=6,
    )
    top_stops = select_top_rows(
        stop_performance,
        preferred_filter=col("disruption_rate_pct") > 0,
        preferred_order=[desc("disruption_rate_pct"), desc("observations"), desc("avg_delay_minutes")],
        fallback_order=[desc("observations"), desc("avg_delay_minutes")],
        limit=6,
    )
    reasons = to_records(disruption_reasons.orderBy(desc("events")), 8)

    for row in reasons:
        row["motif"] = reason_label(row.get("disruption_reason"))
        row["share_pct"] = pct(int(row.get("events") or 0), disrupted_observations)

    sample_events = [
        {
            **row,
            "disruption_reason": reason_label(row.get("disruption_reason")),
        }
        for row in to_records(
            hybrid.filter(col("is_disrupted"))
            .select(
                "line_code",
                "stop_point_name",
                "expected_departure_time",
                "delay_minutes",
                "disruption_reason",
            )
            .orderBy(desc("delay_minutes")),
            10,
        )
    ]

    write_csv(EXPORTS_DIR / "network_overview.csv", [metrics])
    write_csv(
        EXPORTS_DIR / "top_lines.csv",
        [
            {
                "line": line_label(row),
                "observations": row.get("observations"),
                "avg_delay_minutes": row.get("avg_delay_minutes"),
                "delay_rate_pct": row.get("delay_rate_pct"),
                "disruption_rate_pct": row.get("disruption_rate_pct"),
            }
            for row in top_lines
        ],
    )
    write_csv(
        EXPORTS_DIR / "top_stops.csv",
        [
            {
                "stop_point_name": row.get("stop_point_name"),
                "observations": row.get("observations"),
                "avg_delay_minutes": row.get("avg_delay_minutes"),
                "disruption_rate_pct": row.get("disruption_rate_pct"),
            }
            for row in top_stops
        ],
    )
    write_csv(EXPORTS_DIR / "disruption_reasons.csv", reasons)
    write_csv(EXPORTS_DIR / "sample_disruptions.csv", sample_events)

    chart_lines = [
        {"label": line_label(row), "value": float(row.get("disruption_rate_pct") or 0.0)}
        for row in top_lines
    ]
    chart_stops = [
        {"label": str(row.get("stop_point_name") or "inconnu"), "value": float(row.get("disruption_rate_pct") or 0.0)}
        for row in top_stops
    ]
    chart_reasons = [
        {"label": str(row.get("motif") or "inconnu"), "value": float(row.get("events") or 0.0)}
        for row in reasons
    ]

    write_svg_chart(ASSETS_DIR / "top_lines.svg", "Top lignes par taux de perturbation", chart_lines, "label", "value", "%")
    write_svg_chart(ASSETS_DIR / "top_stops.svg", "Top arrets par taux de perturbation", chart_stops, "label", "value", "%")
    write_svg_chart(ASSETS_DIR / "disruption_reasons.svg", "Motifs de perturbation les plus frequents", chart_reasons, "label", "value", "events")

    SUMMARY_PATH.write_text(
        build_summary_markdown(metrics, top_lines, top_stops, reasons, sample_events),
        encoding="utf-8",
    )
    DASHBOARD_PATH.write_text(
        build_dashboard_html(metrics, top_lines, top_stops, reasons, sample_events),
        encoding="utf-8",
    )

    print("Rapport de restitution genere.")
    print(f"Resume : {SUMMARY_PATH}")
    print(f"Dashboard : {DASHBOARD_PATH}")
    print(f"Exports CSV : {EXPORTS_DIR}")

    spark.stop()


if __name__ == "__main__":
    main()
