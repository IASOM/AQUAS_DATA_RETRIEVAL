# Configuracio d'alarmes PREDAP

Aquest document defineix una proposta operativa per configurar alarmes sobre les
unitats que prediu PREDAP a partir de l'historic dels ultims 5 anys.

Objectius:

- Per cada unitat predictiva, calcular el volum historic alt: percentil 95 de
  `n_total` i resum del 5% superior.
- Proposar llindars de creixement percentual anormal per finestres de 7, 14,
  30, 60, 182 i 365 dies.
- Evitar alarmes sorolloses en unitats amb molt poc volum.
- Separar increments grans pero plausibles d'increments anomalament grans.

Nota de nomenclatura: "5% superior" vol dir tall al percentil 95 (`p95`).
"25% superior" vol dir tall al percentil 75 (`p75`).

## Fitxer d'entrada

El calcul s'ha de fer sobre un Parquet final observat, preferentment:

```text
data/finals/demand_diagnosis_joined.parquet
```

Si es vol configurar nomes demanda o nomes diagnostics, tambe es pot usar:

```text
data/demand_pipeline/finals/demand_final.parquet
data/diagnosis_pipeline/finals/diagnosis_final.parquet
```

Sempre que el fitxer tingui columnes d'imputacio, les files imputades s'han
d'excloure per definir llindars historics. Les alarmes s'han de calibrar amb
dades reals observades.

## Unitats predictives

Una unitat predictiva es una columna numerica de recompte que PREDAP prediu.
Per configurar alarmes territorials, les principals son:

```text
DEMAND__TOTAL__UP__<geo_id>
DEMAND__TOTAL__RS__<geo_id>
DIAGNOSIS__TOTAL__UP__<geo_id>
DIAGNOSIS__TOTAL__RS__<geo_id>
DIAGNOSIS__ICD10_3__<grup>__UP__<geo_id>
DIAGNOSIS__ICD10_3__<grup>__RS__<geo_id>
```

Es recomana excloure columnes de metadata:

```text
timestamp
__is_imputed
__imputation_method
__imputation_source_last_date
__imputation_created_at
```

## Finestra historica

Per cada unitat, prendre els ultims 5 anys observats disponibles:

```text
historic_start = last_observed_date - 5 anys + 1 dia
historic_end = last_observed_date
```

Si hi ha menys de 3 anys de dades reals, el llindar s'ha de marcar com a
provisional.

## Percentil 5% superior de n total

Per cada unitat `u`, sigui `x_u(t)` el recompte diari observat.

Calculs recomanats:

```text
n_p95_5y = percentil 95 de x_u(t) en els ultims 5 anys
n_top5_days_5y = nombre de dies amb x_u(t) >= n_p95_5y
n_top5_sum_5y = suma de x_u(t) en aquests dies del 5% superior
n_top5_mean_5y = mitjana de x_u(t) en aquests dies
n_median_5y = mediana diaria dels ultims 5 anys
```

Per configurar alarmes diaries, `n_p95_5y` es el llindar natural de volum alt.
Per alarmes a horitzo, cal usar el percentil 95 de la suma acumulada d'aquell
horitzo:

```text
n_p95_7d_5y = percentil 95 de la suma mobil de 7 dies
n_p95_14d_5y = percentil 95 de la suma mobil de 14 dies
n_p95_30d_5y = percentil 95 de la suma mobil de 30 dies
n_p95_60d_5y = percentil 95 de la suma mobil de 60 dies
n_p95_182d_5y = percentil 95 de la suma mobil de 182 dies
n_p95_365d_5y = percentil 95 de la suma mobil de 365 dies
```

La resta de camps `n_top5_*` documenten el volum real que representa el 5%
superior, tant diari com per horitzo.

## Creixement percentual

Per cada finestra `w` en dies:

```text
w = 7, 14, 30, 60, 182, 365
```

Calcular el volum recent i el volum anterior:

```text
recent_w(t) = suma de x_u(t-w+1 ... t)
previous_w(t) = suma de x_u(t-2w+1 ... t-w)
growth_pct_w(t) = 100 * (recent_w(t) - previous_w(t)) / max(previous_w(t), min_baseline_w)
```

On:

```text
min_baseline_w = max(5, percentil 25 historic de previous_w)
```

Aixo evita creixements infinits quan el periode anterior tenia 0 o molt pocs
casos.

## Llindars suggerits

Es recomana configurar dos nivells:

```text
alerta_groga = creixement gran
alerta_vermella = creixement anomal
```

Llindars percentuals inicials, abans de calibrar amb dades historiques:

| Finestra | Creixement gran | Creixement anomal | Lectura operativa |
|---:|---:|---:|---|
| 7 dies | +80% | +150% | Senyal molt sensible; nomes activar si tambe hi ha volum suficient. |
| 14 dies | +60% | +120% | Bona finestra per brots curts i canvis rapids. |
| 30 dies | +40% | +90% | Finestra principal per senyals mensuals. |
| 60 dies | +30% | +70% | Detecta canvis sostinguts. |
| 182 dies | +20% | +45% | Semestral; controlar canvis estructurals o estacionals. |
| 365 dies | +15% | +35% | Anual; nomes hauria d'activar amb canvis molt consistents. |

Per produccio, aquests llindars s'han d'ajustar empíricament per cada unitat:

```text
yellow_growth_threshold_w = max(llindar_base_groc_w, percentil 95 historic de growth_pct_w positiu)
red_growth_threshold_w = max(llindar_base_vermell_w, percentil 99 historic de growth_pct_w positiu)
```

Si una unitat te molta volatilitat historica, el percentil historic pujara el
llindar i reduira falsos positius.

## Condicions minimes per disparar alarma

Una alarma de creixement nomes s'hauria d'activar si compleix totes les
condicions:

```text
recent_w >= max(10, 0.25 * n_p95_w_5y)
previous_w >= min_baseline_w
growth_pct_w >= threshold_w
```

Per unitats de baix volum, afegir una condicio absoluta:

```text
recent_w - previous_w >= 5
```

Aixo evita alarmes per increments tipus 1 -> 3 casos.

## Sortida recomanada per configurar alarmes

El fitxer final de configuracio hauria de tenir una fila per unitat predictiva:

```text
predap_alarm_thresholds.csv
```

Columnes:

```text
unit
domain
geo_level
geo_id
diagnosis_group
last_observed_date
history_days
n_p95_5y
n_top5_days_5y
n_top5_sum_5y
n_top5_mean_5y
n_median_5y
n_p95_7d_5y
n_top5_windows_7d_5y
n_top5_sum_7d_5y
n_top5_mean_7d_5y
growth_yellow_7d
growth_red_7d
n_p95_14d_5y
n_top5_windows_14d_5y
n_top5_sum_14d_5y
n_top5_mean_14d_5y
growth_yellow_14d
growth_red_14d
n_p95_30d_5y
n_top5_windows_30d_5y
n_top5_sum_30d_5y
n_top5_mean_30d_5y
growth_yellow_30d
growth_red_30d
n_p95_60d_5y
n_top5_windows_60d_5y
n_top5_sum_60d_5y
n_top5_mean_60d_5y
growth_yellow_60d
growth_red_60d
n_p95_182d_5y
n_top5_windows_182d_5y
n_top5_sum_182d_5y
n_top5_mean_182d_5y
growth_yellow_182d
growth_red_182d
n_p95_365d_5y
n_top5_windows_365d_5y
n_top5_sum_365d_5y
n_top5_mean_365d_5y
growth_yellow_365d
growth_red_365d
min_recent_7d
min_recent_14d
min_recent_30d
min_recent_60d
min_recent_182d
min_recent_365d
quality_flag
```

Valors recomanats de `quality_flag`:

```text
ok
low_history
low_volume
missing_recent_data
```

## Pseudocodi de calcul

El repo inclou una versio executable d'aquest calcul:

```powershell
python .\scripts\calculate_predap_alarm_thresholds.py --input .\data\finals\demand_diagnosis_joined.parquet --output .\predap_alarm_thresholds.csv
```

Si vols calcular nomes demanda:

```powershell
python .\scripts\calculate_predap_alarm_thresholds.py --input .\data\demand_pipeline\finals\demand_final.parquet --output .\predap_alarm_thresholds_demand.csv
```

Si vols calcular nomes diagnostics:

```powershell
python .\scripts\calculate_predap_alarm_thresholds.py --input .\data\diagnosis_pipeline\finals\diagnosis_final.parquet --output .\predap_alarm_thresholds_diagnosis.csv
```

Si vols calcular el 25% superior en lloc del 5% superior:

```powershell
python .\scripts\calculate_predap_alarm_thresholds.py --input .\data\finals\demand_diagnosis_joined.parquet --output .\predap_alarm_thresholds_top25.csv --upper-tail-pct 25
```

```python
import pandas as pd

WINDOWS = [7, 14, 30, 60, 182, 365]
BASE_YELLOW = {7: 80, 14: 60, 30: 40, 60: 30, 182: 20, 365: 15}
BASE_RED = {7: 150, 14: 120, 30: 90, 60: 70, 182: 45, 365: 35}

df = pd.read_parquet("data/finals/demand_diagnosis_joined.parquet")
df["timestamp"] = pd.to_datetime(df["timestamp"])

if "__is_imputed" in df.columns:
    df = df[~df["__is_imputed"].astype(str).str.lower().isin(["true", "1"])]

last_day = df["timestamp"].max().normalize()
start_day = last_day - pd.DateOffset(years=5) + pd.Timedelta(days=1)
hist = df[(df["timestamp"] >= start_day) & (df["timestamp"] <= last_day)].copy()

units = [
    c for c in hist.columns
    if c != "timestamp"
    and not c.startswith("__")
    and pd.api.types.is_numeric_dtype(hist[c])
]

rows = []
for unit in units:
    s = (
        hist[["timestamp", unit]]
        .dropna()
        .set_index("timestamp")[unit]
        .astype(float)
        .sort_index()
    )

    n_p95 = s.quantile(0.95)
    top5 = s[s >= n_p95]
    row = {
        "unit": unit,
        "last_observed_date": last_day.date().isoformat(),
        "history_days": int(s.shape[0]),
        "n_p95_5y": float(n_p95),
        "n_top5_days_5y": int(top5.shape[0]),
        "n_top5_sum_5y": float(top5.sum()),
        "n_top5_mean_5y": float(top5.mean()),
        "n_median_5y": float(s.median()),
    }

    for w in WINDOWS:
        recent = s.rolling(w).sum()
        previous = recent.shift(w)
        min_baseline = max(5, previous.quantile(0.25))
        growth = 100 * (recent - previous) / previous.clip(lower=min_baseline)
        positive_growth = growth[growth > 0].dropna()

        p95_growth = positive_growth.quantile(0.95) if not positive_growth.empty else 0
        p99_growth = positive_growth.quantile(0.99) if not positive_growth.empty else 0

        row[f"growth_yellow_{w}d"] = float(max(BASE_YELLOW[w], p95_growth))
        row[f"growth_red_{w}d"] = float(max(BASE_RED[w], p99_growth))
        recent = s.rolling(w, min_periods=w).sum()
        n_p95_w = recent.dropna().quantile(0.95)
        row[f"n_p95_{w}d_5y"] = float(n_p95_w)
        row[f"min_recent_{w}d"] = float(max(10, 0.25 * n_p95_w))

    row["quality_flag"] = "ok" if s.shape[0] >= 365 * 3 and s.sum() >= 100 else "low_volume"
    rows.append(row)

pd.DataFrame(rows).to_csv("predap_alarm_thresholds.csv", index=False)
```

## Recomanacio final

Per arrencar configuracio:

- Usar `n_p95_5y` com a llindar de volum alt diari per unitat.
- Usar `n_p95_<horitzo>d_5y` com a llindar de volum alt acumulat per horitzo.
- Usar `growth_yellow_30d` i `growth_red_30d` com a alarma principal.
- Usar 7 i 14 dies com a alarmes rapides, pero amb `min_recent` activat.
- Usar 182 i 365 dies com a alarmes de tendencia, no com a urgencia.
- Revisar manualment les unitats amb `quality_flag != ok`.
