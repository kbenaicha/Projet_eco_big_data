# Resultats interpretable du reseau

## Ce qu'on apprend rapidement
- Le jeu de donnees couvre **23 lignes** et **8 arrets**.
- Sur **373 observations**, **0,80 %** sont perturbees et **0,54 %** sont annulees.
- Le retard moyen observe sur le reseau est de **0,03 minutes**.
- La ligne la plus exposee est **C01742 (A)** avec **0,00 %** d'observations perturbees.
- L'arret le plus fragile est **La Défense** avec un retard moyen de **0 minutes**.
- Le motif de perturbation dominant est **cancelled_service** avec **2 evenements**.

## Chiffres cles
- Observations analysees : **373**
- Lignes observees : **23**
- Arrets observes : **8**
- Taux de retard : **0,00 %**
- Taux de perturbation : **0,80 %**
- Taux d'annulation : **0,54 %**
- Retard moyen reseau : **0,03 minutes**

## Top lignes les plus perturbees
| ligne | observations | retard_moyen_min | taux_perturbation_pct |
| --- | --- | --- | --- |
| C01742 (A) | 128 | -0,03 | 0 |

## Top arrets les plus sensibles
| arret | observations | retard_moyen_min | taux_perturbation_pct |
| --- | --- | --- | --- |
| La Défense | 286 | 0 | 0,70 |

## Motifs de perturbation
| disruption_reason | events | share_pct |
| --- | --- | --- |
| cancelled_service | 2 | 66,67 |
| missing_realtime_status | 1 | 33,33 |

## Exemples concrets d'observations perturbees
| line_code | stop_point_name | expected_departure_time | delay_minutes | disruption_reason |
| --- | --- | --- | --- | --- |
| C01729 | La Défense | 2026-03-25T15:31:10.000Z | 1,33 | cancelled_service |
| C00754 | La Défense - Terminal Jules Verne | 2026-03-25T16:20:00.000Z | 0 | missing_realtime_status |
| C01729 | La Défense | 2026-03-25T15:59:20.000Z | 0 | cancelled_service |

## Livrables generes
- `reports/results_summary.md`
- `reports/results_dashboard.html`
- `reports/exports/network_overview.csv`
- `reports/exports/top_lines.csv`
- `reports/exports/top_stops.csv`
- `reports/exports/disruption_reasons.csv`
- `reports/exports/sample_disruptions.csv`
- `reports/assets/top_lines.svg`
- `reports/assets/top_stops.svg`
- `reports/assets/disruption_reasons.svg`