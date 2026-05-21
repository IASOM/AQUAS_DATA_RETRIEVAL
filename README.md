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
| Diagnostics | `z_inv.P1038_prstb015r_filtrat` | Comptar totals reals de diagnostics per dia i generar variables de codis seleccionats per Catalunya, RS i UP | `data/diagnosis_pipeline/finals/diagnosis_final.parquet` |

TambÃ© pot unir les dues sortides finals en un sol fitxer:

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

Regla de dates: els incrementals, els finals i el fitxer unit no han de contenir files posteriors a avui. Les consultes fan servir de tall de fi el principi de dema, de manera que inclouen tot el dia d'avui pero exclouen qualsevol data futura. L'escriptura Parquet tambe elimina files futures si arriben per error.

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
      selected_codes/
    finals/
  selections/
    selected_diagnosis_codes.csv
    selected_rs.csv
    selected_up.csv
  src/
    ... codi legacy
  run_pipeline.py
  run_pipeline_optimized.py
  requirements.txt
  .env.example
  UPperRS.xlsx
```

## InstalÂ·lacio

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
SELECTED_RS_FILE=C:/path/to/AQUAS_INTEGRATION/selections/selected_rs.csv
SELECTED_UP_FILE=C:/path/to/AQUAS_INTEGRATION/selections/selected_up.csv
SELECTED_DIAGNOSIS_CODES_FILE=C:/path/to/AQUAS_INTEGRATION/selections/selected_diagnosis_codes.csv
MAX_DIAGNOSIS_FEATURES=200000
LOG_LEVEL=INFO
```

`UP_RS_FILE` ha d'apuntar a l'Excel amb el full `UP per RS`. El pipeline utilitza aquest fitxer per mapar codis UP a RS.

### Seleccions de demanda per RS i UP

El pipeline de demanda tambe pot limitar les columnes especifiques de RS i UP sense alterar els totals globals:

- `DEMANDA_TOTAL` es calcula amb totes les visites.
- Les variables globals sense RS/UP, com `demanda_SERVEI_CODI_INF`, tambe es calculen amb totes les visites.
- `selections/selected_rs.csv` limita les columnes agrupades per RS, com `demanda_SERVEI_CODI_INF_<RS>` i `demanda__TOTAL_RS_<RS>`.
- `selections/selected_up.csv` limita les columnes agrupades per UP, com `demanda_SERVEI_CODI_INF_<UP>` i `demanda__TOTAL_UP_<UP>`.

Els CSVs son d'una sola columna:

```csv
RS
BARCELONA
GIRONA
```

```csv
UP
00001
00025
00103
```

Si no hi ha fitxer de RS o UP amb valors, s'inclouen totes les RS o totes les UP. Les UP es normalitzen amb zeros a l'esquerra, per exemple `1` -> `00001`. Aquests fitxers son compartits per demanda i diagnostics.

### Seleccions de diagnostics, RS i UP

El pipeline de diagnostics separa dos conceptes que son importants per no barrejar totals amb filtres:

- Els totals `DIAG_TOTAL`, `DIAG_TOTAL_RS_*` i `DIAG_TOTAL_UP_*` es calculen amb tots els diagnostics de la taula origen.
- Les variables de codi `DIAG_CODE_*`, `DIAG_RS_<codi_o_grup>_<RS>` i `DIAG_UP_<codi_o_grup>_<UP>` es calculen nomes per als codis diagnostics seleccionats.
- Els fitxers de RS i UP no canvien el total general; nomes limiten quines columnes agrupades per RS o UP s'escriuen al Parquet final.

Fitxers de seleccio:

| Fitxer | Exemple de capcalera | Efecte |
| --- | --- | --- |
| `selections/selected_diagnosis_codes.csv` | `ICD10_3,feature_name` | Llista de codis ICD10 de 3 caracters que generen variables `DIAG_CODE_*` i variables per grup. La segona columna es opcional i permet agrupar codis diferents sota el mateix nom de variable. Codis com `J00.9` es normalitzen a `J00`. |
| `selections/selected_rs.csv` | `RS` | Si existeix i conte valors, limita les columnes `DIAG_TOTAL_RS_*` i `DIAG_RS_<codi_o_grup>_<RS>` a aquestes RS. Es el mateix fitxer compartit amb demanda. |
| `selections/selected_up.csv` | `UP` | Si existeix i conte valors, limita les columnes `DIAG_TOTAL_UP_*` i `DIAG_UP_<codi_o_grup>_<UP>` a aquestes UP. Es el mateix fitxer compartit amb demanda. |

Els CSV de RS i UP son d'una sola columna. El CSV de diagnostics pot tenir una segona columna `feature_name`. Si `feature_name` esta buida, el nom de variable sera el codi ICD10 normalitzat. Si diversos codis tenen el mateix `feature_name`, el pipeline els suma en una sola variable.

Exemple sense agrupacions:

```csv
ICD10_3,feature_name
J00
I10
A09
```

Exemple agrupant diversos codis respiratoris:

```csv
ICD10_3,feature_name
J00,RESPIRATORI
J02,RESPIRATORI
J03,RESPIRATORI
I10,HIPERTENSIO
```

Aquest exemple genera columnes com `DIAG_CODE_RESPIRATORI`, `DIAG_RS_RESPIRATORI_BARCELONA` i `DIAG_UP_RESPIRATORI_00001`, on `RESPIRATORI` suma `J00`, `J02` i `J03`.

```csv
RS
BARCELONA
GIRONA
```

```csv
UP
00001
00025
00103
```

Si els fitxers compartits de RS o UP no existeixen o no tenen valors, s'inclouen totes les RS o totes les UP. Per compatibilitat, els codis diagnostics encara poden carregar-se des de la ruta legacy `diagnosis_pipeline/selected_codes/selected_codes.csv`, pero la ruta recomanada es `selections/selected_diagnosis_codes.csv`. Si no hi ha fitxer de codis diagnostics amb valors, el pipeline conserva els totals pero omet les variables especifiques de codi per evitar matrius massa amples.

Per protegir memoria, `MAX_DIAGNOSIS_FEATURES` limita el nombre de columnes de diagnostics que es poden crear. Si s'arriba a aquest limit, normalment vol dir que falta una seleccio o que la columna de codi diagnostic no s'ha normalitzat com s'esperava.

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

Per defecte, si no passes dates, el pipeline continua de manera incremental: si ja hi ha un final anterior, processa des de l'ultim dia guardat + 1 fins avui; si no hi ha cap final anterior, processa des de `2008-01-01` fins avui.

Forcar un rang de dates manual, amb data inicial i final incloses:

```bash
python run_pipeline.py --all --start-date 2024-01-01 --end-date 2024-12-31
python run_pipeline.py --demand --start-date 2026-05-01
python run_pipeline.py --diagnosis --end-date 2026-05-20
```

Si nomes passes `--start-date`, el final del rang es avui. Si nomes passes `--end-date`, l'inici continua sent incremental: ultim dia processat + 1, o `2008-01-01` si encara no hi ha final.

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

Convertir qualsevol Parquet final o incremental a CSV o Excel:

```bash
python run_pipeline.py --convert-parquet data/demand_pipeline/finals/demand_final.parquet --to csv
python run_pipeline.py --convert-parquet data/diagnosis_pipeline/incremental/incremental_20260520_135721_740331_c9093212_038.parquet --to excel --output exports/diagnosis_incremental.xlsx
```

Si no es passa `--output`, el fitxer es crea al mateix directori i amb el mateix nom base, canviant nomes l'extensio a `.csv` o `.xlsx`.

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

Cap sortida incremental, final o unida hauria de tenir timestamps posteriors a avui. Si el sistema origen retorna dates futures, es descarten abans d'escriure el Parquet.

## Incremental diari

Despres d'una primera reconstruccio historica, les execucions seguents son diaries:

- El pipeline busca l'ultim dia processat a la metadata incremental i, si cal, al Parquet final.
- Si no troba cap dia processat, comenca a `2008-01-01` o a la data minima configurada a `MIN_VALID_DATE`.
- La query seguent comenca a `ultim_dia_processat + 1 dia`, no al mateix dia. Aixo evita tornar a llegir parcialment un dia ja agregat quan la data origen conte hores.
- La finestra acaba a l'end exclusive del maxim dia disponible, sempre limitat a avui. Per exemple, si l'ultim dia processat es `2026-05-20` i avui es `2026-05-21`, nomes consulta `2026-05-21 00:00` -> `2026-05-22 00:00`.
- Si passes `--start-date`, aquesta data substitueix l'inici incremental i permet reprocessar un rang antic. `--end-date` limita el final del rang, sempre sense permetre dates futures.
- Els incrementals nous es fusionen amb el final existent per `timestamp`. Si algun dia se solapa, el dia nou substitueix completament el dia antic i no se suma.
- Despres de reconstruir el final, els Parquets incrementals ja processats s'eliminen, pero la metadata queda guardada per saber on continuar.

Si cal reprocessar dies antics per canvis retroactius a la base de dades, executa el rang amb `--start-date` i `--end-date`. El mode normal sense dates esta optimitzat per afegir dies nous, no per detectar modificacions historiques.

## Dades sintetiques

El projecte inclou dades petites d'exemple per provar el pipeline sense ODBC ni permisos de base de dades:

| Fitxer | Contingut |
| --- | --- |
| `data/sample/input/up_rs.csv` | Mapping UP -> RS |
| `data/sample/input/demand_visits.csv` | Visites sintetiques per al pipeline de demanda |
| `data/sample/input/diagnosis_visits.csv` | Diagnostics sintetics |
| `data/sample/input/selected_codes.csv` | Codis diagnostics que generen variables especifiques de codi; pot incloure una segona columna `feature_name` com `selections/selected_diagnosis_codes.csv` |

Comandes utils:

```bash
python run_pipeline.py --sample --demand
python run_pipeline.py --sample --diagnosis
python run_pipeline.py --sample --all
python run_pipeline.py --sample --join-final
```

TambÃ© es poden passar carpetes alternatives:

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
| `data/sample/multiyear_input/selected_codes.csv` | Codis diagnostics que generen variables especifiques de codi; pot incloure una segona columna `feature_name` |

I escriu aquestes sortides finals:

| Fitxer | Format | Contingut |
| --- | --- | --- |
| `data/sample/multiyear_output/demand_pipeline/finals/demand_final.parquet` | Parquet | Final de demanda |
| `data/sample/multiyear_output/demand_pipeline/finals/demand_final.csv` | CSV | Copia llegible del final de demanda |
| `data/sample/multiyear_output/diagnosis_pipeline/finals/diagnosis_final.parquet` | Parquet | Final de diagnostics |
| `data/sample/multiyear_output/diagnosis_pipeline/finals/diagnosis_final.csv` | CSV | Copia llegible del final de diagnostics |
| `data/sample/multiyear_output/finals/demand_diagnosis_joined.parquet` | Parquet | Final unit demanda + diagnostics |
| `data/sample/multiyear_output/finals/demand_diagnosis_joined.csv` | CSV | Copia llegible del final unit |

La demanda inclou tres nivells de variables: totals globals sense agrupacio (demanda_SERVEI_CODI_INF, demanda_TIPUS_CLASS_C9C), totals per RS (demanda_SERVEI_CODI_INF_RS_BARCELONA) i totals per UP (demanda_SERVEI_CODI_INF_00101). Les seleccions de RS/UP de demanda nomes limiten les columnes agrupades; `DEMANDA_TOTAL` i les variables globals continuen incloent totes les visites. Diagnostics inclou totals reals amb tots els diagnostics (`DIAG_TOTAL`, `DIAG_TOTAL_RS_*`, `DIAG_TOTAL_UP_*`) i variables especifiques per als codis seleccionats o grups de codis (`DIAG_CODE_J00`, `DIAG_CODE_RESPIRATORI`, `DIAG_RS_RESPIRATORI_BARCELONA`, `DIAG_UP_RESPIRATORI_00001`).

La validacio executada amb el rang per defecte dona:

| Sortida | Files | Columnes | Rang |
| --- | ---: | ---: | --- |
| Demand final | 1827 | 139 | `2008-01-01` -> `2012-12-31` |
| Diagnosis final | 1827 | 17 | `2008-01-01` -> `2012-12-31` |
| Joined final | 1827 | 155 | `2008-01-01` -> `2012-12-31` |

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

Abans de guardar incrementals o finals, elimina qualsevol fila amb `timestamp` posterior a avui.

### `pipelines/demand/`

Processa visites:

- Converteix `DATA_VISITA` a dia.
- Normalitza `UP`.
- Afegeix `RS` a partir de l'Excel `UPperRS.xlsx`.
- Classifica tipus de visita en presencial, domiciliaria, telefonica o `NA`.
- Agrega comptatges globals per dia i per dimensions com lloc, situacio, servei i tipus amb totes les visites.
- Usa `selections/selected_rs.csv` i `selections/selected_up.csv` per limitar quines RS i UP apareixen en les columnes agrupades de demanda, sense canviar `DEMANDA_TOTAL` ni les variables globals.
- No consulta ni guarda visites amb data futura.

### `pipelines/diagnosis/`

Processa diagnostics:

- Valida que la taula origen tingui les columnes requerides.
- Consulta la base agregant en SQL per dia, UP i codi diagnostic normalitzat de 3 caracters.
- Converteix la data de visita a `timestamp` diari.
- Afegeix `RS` a partir de l'Excel `UPperRS.xlsx`.
- Genera totals amb tots els diagnostics: `DIAG_TOTAL`, `DIAG_TOTAL_RS_*` i `DIAG_TOTAL_UP_*`.
- Genera variables de codis seleccionats o grups de codis: `DIAG_CODE_*`, `DIAG_RS_<codi_o_grup>_<RS>` i `DIAG_UP_<codi_o_grup>_<UP>`.
- Si `selections/selected_diagnosis_codes.csv` conte `feature_name`, usa aquest nom en les variables i suma tots els codis que comparteixen el mateix nom.
- Usa `selections/selected_rs.csv` i `selections/selected_up.csv` per limitar quines RS i UP apareixen en les columnes agrupades, sense canviar el total general.
- No consulta ni guarda diagnostics amb data futura.

### `pipelines/shared/final_joiner.py`

Uneix `demand_final.parquet` i `diagnosis_final.parquet` per `timestamp`, afegeix prefixos `DEMAND_` i `DIAGNOSIS_`, descarta timestamps futurs i desa el resultat final.

## Validacio rapida

Aquestes comprovacions no requereixen connexio a la base de dades:

```bash
python run_pipeline.py --help
python run_pipeline.py --sample --all
python run_pipeline.py --sample --all --start-date 2026-01-02 --end-date 2026-01-03
python run_pipeline.py --convert-parquet data/sample/output/finals/demand_diagnosis_joined.parquet --to csv
python -B scripts/create_multiyear_sample.py
python -m compileall -q run_pipeline.py run_pipeline_optimized.py config pipelines validate_project.py
```

Per executar els pipelines reals cal tenir `.env`, ODBC i permisos de base de dades configurats.

## Notes de manteniment

- `data/` conte sortides generades i actualment esta versionat. Si les dades son grans, sensibles o reproduibles, convindria treure-les del control de versions i ignorar `data/`.
- Hi ha dues convencions de nom per l'Excel de mapping: `UPperRS.xlsx` i `UP per RS.xlsx`. La configuracio actual utilitza `UPperRS.xlsx`; millor mantenir una sola copia canonica.
- L'agregacio optimitzada fusiona els incrementals nous amb el Parquet final existent abans de sobreescriure'l. Tot i aixo, si canvieu l'esquema de columnes, valideu que el final conservi totes les variables esperades per dia.

## Fitxers que es podrien simplificar

Prioritat alta:

- Decidir si `data/` s'ha de versionar. En molts projectes de pipelines es millor versionar codi i configuracio, no sortides generades.

Prioritat mitjana:

- Unificar documentacio dispersa (`QUICKSTART.md`, `PROJECT_STRUCTURE.md`, `MIGRATION.md`, `OPTIMIZED_PIPELINE.md`, etc.) o marcar clarament quina documentacio es historica.
- Fer que `check_columns.py` reutilitzi la configuracio central.
- Afegir tests petits per transformacions i agregacions abans de tocar l'esquema de sortida.



