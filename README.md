# AQUAS / PREDAP Data Pipelines

Projecte Python per extreure dades de SQL Server / Azure Synapse, transformar-les i generar sortides Parquet per a l'analisi de demanda assistencial i diagnostics.

La versio activa del projecte es la implementacio optimitzada amb Parquet. El comandament principal es:

```bash
python run_pipeline.py
```

`run_pipeline.py` es mante com a entrada estable i delega internament a `run_pipeline_optimized.py`.

## Que fa el projecte

El codi implementa dos pipelines principals:

| Pipeline | Taula origen | Objectiu | Sortida principal |
| --- | --- | --- | --- |
| Demanda | `z_inv.P1038_visites` | Comptar visites per dia i generar variables agregades per Catalunya, RS i UP | `data/demand_pipeline/finals/demand_final.parquet` |
| Diagnostics | `z_inv.P1038_prstb015r_filtrat` | Comptar diagnostics per dia, codi diagnostic, RS i UP, opcionalment filtrant per codis seleccionats | `data/diagnosis_pipeline/finals/diagnosis_final.parquet` |

També pot unir les dues sortides finals en un sol fitxer:

```text
data/finals/demand_diagnosis_joined.parquet
```

## Flux general

1. Carrega la configuracio des de variables d'entorn i valors per defecte a `config/config.py`.
2. Obre connexio ODBC a SQL Server / Azure Synapse.
3. Detecta el rang de dates disponible a la taula origen.
4. Processa les dades per anys per reduir consum de memoria.
5. Neteja i transforma camps de data, UP, RS, tipus de visita o codi diagnostic.
6. Escriu fitxers incrementals en Parquet dins `data/*/incremental/`.
7. Reconstrueix la sortida final en Parquet dins `data/*/finals/`.
8. Opcionalment fa un join per `timestamp` entre demanda i diagnostics.

## Estructura actual

```text
AQUAS_INTEGRATION/
  config/
    config.py
  pipelines/
    shared/
      db.py
      utils.py
      parquet_storage.py
      final_joiner.py
      logging_config.py
    demand/
      incremental_optimized.py
      aggregation_optimized.py
      transformations.py
    diagnosis/
      incremental_optimized.py
      aggregation_optimized.py
      __init__.py
  data/
    demand_pipeline/
    diagnosis_pipeline/
    finals/
  src/
    ... codi legacy
  run_pipeline.py
  run_pipeline_optimized.py
  requirements.txt
  .env.example
  UPperRS.xlsx
```

## Instal·lacio

Requisits:

- Python 3.9 o superior
- ODBC Driver 18 for SQL Server
- Acces a la base de dades `aquas`

Preparacio:

```bash
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
Copy-Item .env.example .env
python setup.py
```

Edita `.env` amb els valors reals de connexio i rutes locals.

## Configuracio

Variables principals:

```env
DB_SERVER=synw-aquas.sql.azuresynapse.net
DB_DATABASE=aquas
AUTH_MODE=ActiveDirectoryIntegrated
BASE_DIR=C:/path/to/AQUAS_INTEGRATION
UP_RS_FILE=C:/path/to/AQUAS_INTEGRATION/UPperRS.xlsx
LOG_LEVEL=INFO
```

`UP_RS_FILE` ha d'apuntar a l'Excel amb el full `UP per RS`. El pipeline utilitza aquest fitxer per mapar codis UP a RS.

## Execucio

Executar demanda i diagnostics:

```bash
python run_pipeline.py
```

Executar nomes demanda:

```bash
python run_pipeline.py --demand
```

Executar nomes diagnostics:

```bash
python run_pipeline.py --diagnosis
```

Executar demanda, diagnostics i join final:

```bash
python run_pipeline.py --all
```

Executar amb dades sintetiques locals, sense connexio a la base de dades:

```bash
python run_pipeline.py --sample --all
```

Aquest mode llegeix els CSVs de `data/sample/input/` i escriu els Parquet a `data/sample/output/`.

Generar una mostra sintetica multi-any, crear finals Parquet i exportar tambe CSVs llegibles:

```bash
python -B scripts/create_multiyear_sample.py
```

Per defecte genera dades diaries des de `2008-01-01` fins a `2012-12-31`.
Es pot canviar el rang:

```bash
python -B scripts/create_multiyear_sample.py --start 2008-01-01 --end 2025-12-31
```

Fer nomes el join de finals ja generats:

```bash
python run_pipeline.py --join-final
```

Veure opcions disponibles:

```bash
python run_pipeline.py --help
```

## Sortides

| Fitxer | Contingut |
| --- | --- |
| `data/demand_pipeline/incremental/*.parquet` | Blocs incrementals de demanda |
| `data/demand_pipeline/finals/demand_final.parquet` | Matriu final de demanda |
| `data/diagnosis_pipeline/incremental/*.parquet` | Blocs incrementals de diagnostics |
| `data/diagnosis_pipeline/finals/diagnosis_final.parquet` | Matriu final de diagnostics |
| `data/finals/demand_diagnosis_joined.parquet` | Demanda i diagnostics units per `timestamp` |
| `data/sample/output/` | Sortides generades pel mode `--sample` |
| `data/sample/multiyear_input/` | CSVs sintetiques multi-any generades per `scripts/create_multiyear_sample.py` |
| `data/sample/multiyear_output/` | Finals Parquet i CSV de la mostra multi-any |

## Dades sintetiques

El projecte inclou dades petites d'exemple per provar el pipeline sense ODBC ni permisos de base de dades:

| Fitxer | Contingut |
| --- | --- |
| `data/sample/input/up_rs.csv` | Mapping UP -> RS |
| `data/sample/input/demand_visits.csv` | Visites sintetiques per al pipeline de demanda |
| `data/sample/input/diagnosis_visits.csv` | Diagnostics sintetics |
| `data/sample/input/selected_codes.csv` | Codis diagnostics que es conservaran en el filtre |

Comandes utils:

```bash
python run_pipeline.py --sample --demand
python run_pipeline.py --sample --diagnosis
python run_pipeline.py --sample --all
python run_pipeline.py --sample --join-final
```

També es poden passar carpetes alternatives:

```bash
python run_pipeline.py --sample --all --sample-input-dir data/sample/input --sample-output-dir data/sample/output
```

### Mostra multi-any per validar files diaries

El script `scripts/create_multiyear_sample.py` crea una mostra sintetica mes gran per validar que el pipeline conserva tots els dies de tots els anys processats. Genera:

| Fitxer | Contingut |
| --- | --- |
| `data/sample/multiyear_input/up_rs.csv` | Mapping UP -> RS de prova |
| `data/sample/multiyear_input/demand_visits.csv` | Visites sintetiques repartides per tots els dies del rang |
| `data/sample/multiyear_input/diagnosis_visits.csv` | Diagnostics sintetics repartits per tots els dies del rang |
| `data/sample/multiyear_input/selected_codes.csv` | Codis diagnostics inclosos en la mostra |

I escriu aquestes sortides finals:

| Fitxer | Format | Contingut |
| --- | --- | --- |
| `data/sample/multiyear_output/demand_pipeline/finals/demand_final.parquet` | Parquet | Final de demanda |
| `data/sample/multiyear_output/demand_pipeline/finals/demand_final.csv` | CSV | Copia llegible del final de demanda |
| `data/sample/multiyear_output/diagnosis_pipeline/finals/diagnosis_final.parquet` | Parquet | Final de diagnostics |
| `data/sample/multiyear_output/diagnosis_pipeline/finals/diagnosis_final.csv` | CSV | Copia llegible del final de diagnostics |
| `data/sample/multiyear_output/finals/demand_diagnosis_joined.parquet` | Parquet | Final unit demanda + diagnostics |
| `data/sample/multiyear_output/finals/demand_diagnosis_joined.csv` | CSV | Copia llegible del final unit |

La validacio executada amb el rang per defecte dona:

| Sortida | Files | Columnes | Rang |
| --- | ---: | ---: | --- |
| Demand final | 1827 | 121 | `2008-01-01` -> `2012-12-31` |
| Diagnosis final | 1827 | 17 | `2008-01-01` -> `2012-12-31` |
| Joined final | 1827 | 137 | `2008-01-01` -> `2012-12-31` |

Per comprovar els resultats generats:

```bash
python -B -c "import pandas as pd; df=pd.read_parquet('data/sample/multiyear_output/finals/demand_diagnosis_joined.parquet'); print(df.shape); print(df[['timestamp']].head()); print(df[['timestamp']].tail())"
```

## Nota important sobre el Parquet final de 35 files

Si `demand_final.parquet` queda amb nomes unes poques files, per exemple 35 files, tot i haver processat anys complets, la causa probable era la retencio dels incrementals. La versio optimitzada escrivia chunks historics a `data/*/incremental/`, pero despres aplicava `retention_days=90`. Quan es reconstruia un historic des de 2008, els chunks de 2008-2025 quedaven per sota del tall de 90 dies i s'esborraven abans de crear el final. Per aixo nomes sobrevivia el chunk mes recent.

Aixo s'ha corregit aixi:

- `ParquetIncrementalManager` usa `retention_days=None` per defecte, de manera que conserva tots els chunks incrementals durant una reconstruccio historica.
- Els runners optimitzats de demanda i diagnostics passen `retention_days=None`.
- Els noms dels chunks incrementals inclouen microsegons i un identificador curt aleatori per evitar col.lisions quan es guarden diversos fitxers dins el mateix segon.

Si ja existeixen sortides dolentes d'una execucio anterior, elimina els incrementals i finals abans de regenerar:

```powershell
Remove-Item -Recurse -Force .\data\demand_pipeline\incremental
Remove-Item -Recurse -Force .\data\demand_pipeline\finals
Remove-Item -Recurse -Force .\data\diagnosis_pipeline\incremental
Remove-Item -Recurse -Force .\data\diagnosis_pipeline\finals
```

Despres torna a executar:

```bash
python run_pipeline.py --all
```

Si la pipeline de diagnostics falla amb un missatge de token expirat d'Azure/SQL, reinicia la sessio o torna a autenticar-te i repeteix l'execucio. Aquest error no esta relacionat amb el nombre de files del Parquet final.

## Components principals

### `config/config.py`

Centralitza servidor, base de dades, noms de taules, columnes, rutes de dades i fitxer `UPperRS.xlsx`.

### `pipelines/shared/db.py`

Construeix la connexio `pyodbc` amb ODBC Driver 18 i autenticacio `ActiveDirectoryIntegrated`.

### `pipelines/shared/utils.py`

Conte utilitats comunes: lectura de rangs de dates, particio per anys, consultes SQL per finestres temporals i funcions legacy de CSV/estat.

### `pipelines/shared/parquet_storage.py`

Gestiona incrementals Parquet, metadades, retencio opcional de fitxers antics i escriptura de sortides finals. En reconstruccions historiques s'ha de mantenir `retention_days=None` per conservar tots els dies abans de l'agregacio final.

### `pipelines/demand/`

Processa visites:

- Converteix `DATA_VISITA` a dia.
- Normalitza `UP`.
- Afegeix `RS` a partir de l'Excel `UPperRS.xlsx`.
- Classifica tipus de visita en presencial, domiciliaria, telefonica o `NA`.
- Agrega comptatges per dia i per dimensions com lloc, situacio, servei i tipus.

### `pipelines/diagnosis/`

Processa diagnostics:

- Valida que la taula origen tingui les columnes requerides.
- Filtra codis si existeix `data/diagnosis_pipeline/selected_codes/selected_codes.csv`.
- Converteix la data de visita a `timestamp` diari.
- Afegeix `RS` a partir de l'Excel `UPperRS.xlsx`.
- Genera comptatges per codi diagnostic, RS i UP.

### `pipelines/shared/final_joiner.py`

Uneix `demand_final.parquet` i `diagnosis_final.parquet` per `timestamp`, afegeix prefixos `DEMAND_` i `DIAGNOSIS_`, i desa el resultat final.

## Validacio rapida

Aquestes comprovacions no requereixen connexio a la base de dades:

```bash
python run_pipeline.py --help
python run_pipeline.py --sample --all
python -B scripts/create_multiyear_sample.py
python -m compileall -q run_pipeline.py run_pipeline_optimized.py config pipelines validate_project.py check_columns.py
```

Per executar els pipelines reals cal tenir `.env`, ODBC i permisos de base de dades configurats.

## Notes de manteniment

- `src/` es codi legacy amb rutes absolutes antigues i duplicacio de logica. No es la via recomanada.
- `check_columns.py` encara importa configuracio legacy de `src/diagnosis`. Es pot migrar a `config/config.py` o eliminar si ja no es fa servir.
- `src/daily_run.py` conte errors de runtime evidents (`df.combined`, `df_copmbined`, `args.run_now`) i rutes absolutes antigues. No s'hauria d'usar sense refactor.
- `data/` conte sortides generades i actualment esta versionat. Si les dades son grans, sensibles o reproduibles, convindria treure-les del control de versions i ignorar `data/`.
- Hi ha dues convencions de nom per l'Excel de mapping: `UPperRS.xlsx` i `UP per RS.xlsx`. La configuracio actual utilitza `UPperRS.xlsx`; millor mantenir una sola copia canonica.
- L'agregacio optimitzada fusiona els incrementals nous amb el Parquet final existent abans de sobreescriure'l. Tot i aixo, si canvieu l'esquema de columnes, valideu que el final conservi totes les variables esperades per dia.

## Fitxers que es podrien simplificar

Prioritat alta:

- Consolidar el codi actiu a `pipelines/` i arxivar o eliminar `src/` quan s'hagi validat que ja no cal.
- Decidir si `data/` s'ha de versionar. En molts projectes de pipelines es millor versionar codi i configuracio, no sortides generades.
- Deixar un sol fitxer de mapping UP-RS.

Prioritat mitjana:

- Unificar documentacio dispersa (`QUICKSTART.md`, `PROJECT_STRUCTURE.md`, `MIGRATION.md`, `OPTIMIZED_PIPELINE.md`, etc.) o marcar clarament quina documentacio es historica.
- Fer que `check_columns.py` reutilitzi la configuracio central.
- Afegir tests petits per transformacions i agregacions abans de tocar l'esquema de sortida.
