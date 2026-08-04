# Diagnosis And Territory Selections

Selection files live in:

```text
selections/
```

## Diagnosis Selections

File:

```text
selections/selected_diagnosis_codes.csv
```

Format:

```csv
ICD10_3,feature_name,definition_ca
J00-J06,G01,Infeccions respiratories agudes i simptomatologia
F30-F39,G04,Salut mental: ansietat, depressio i trastorns adaptatius
U07.1,D08,COVID-19
```

Columns:

| Column | Meaning |
| --- | --- |
| `ICD10_3` | ICD10 3-character code or range. Examples: `J00`, `J00-J06`, `U07.1` |
| `feature_name` | Output code used in the column name. Examples: `G01`, `D08` |
| `definition_ca` | Human-readable Catalan description. It is not used in the column name |

`U07.1` is normalized to `U07` because the pipeline works at ICD10 3-character level.

## Current Diagnosis Groups

| Group | Definition |
| --- | --- |
| `G01` | Acute respiratory infections and symptomatology |
| `G02` | Asthma, COPD, and chronic respiratory diseases / exacerbations |
| `G03` | Gastroenteritis, digestive infections, and symptoms |
| `G04` | Mental health: anxiety, depression, and adjustment disorders |
| `G05` | Musculoskeletal pain, low back pain, and minor injuries |
| `D01` | Acute upper respiratory infection |
| `D02` | Influenza-like syndrome |
| `D03` | Acute bronchiolitis |
| `D04` | Infectious gastroenteritis |
| `D05` | Low back pain |
| `D06` | Anxiety / anxiety disorder |
| `D07` | Type 2 diabetes |
| `D08` | COVID-19 |
| `D09` | Essential hypertension |

## RS Selections

File:

```text
selections/selected_rs.csv
```

Format:

```csv
geo_id,RS
RS_64,GIRONA
RS_67,CATALUNYA CENTRAL
```

`RS` must match the source value from `UPperRS.xlsx`. `geo_id` is used in output column names.

## UP / Micro Selections

File:

```text
selections/selected_up.csv
```

Format:

```csv
geo_id,UP,name
MICRO_01,00348,CAP Bages / Manresa
MICRO_08,06311,CUAP Cotxeres
```

`UP` filters source rows. `geo_id` is used in output column names.
