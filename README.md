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

Tambe pot unir les dues sortides finals en un sol fitxer:

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

Si la taula origen encara no ha carregat dades fins avui, o fins al `--end-date` que has demanat, les sortides finals s'allarguen fins al dia objectiu amb valors imputats. El dia objectiu mai pot ser posterior a avui. Aquests dies es marquen amb `__is_imputed = True`, `__imputation_method`, `__imputation_source_last_date` i `__imputation_created_at`. La imputacio fa servir la mitjana historica del mateix dia i mes en anys anteriors, amb mitjana global de la columna com a fallback. En una execucio posterior, quan les dades reals ja existeixin al servidor, aquestes files imputades no compten com a dades processades i es substitueixen pels valors reals.

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
  scripts/
    check_source_upload_metadata.py
    create_multiyear_sample.py
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

`UP_RS_FILE` ha d'apuntar a l'Excel amb el full `UP per RS`. El pipeline utilitza aquest fitxer per mapar codis UP a RS. Si `UPperRS.xlsx` falta en una copia local del repositori, restaura'l amb:

```powershell
git restore UPperRS.xlsx
```

Si el fitxer esta en una altra ubicacio, posa la ruta real a `.env`:

```env
UP_RS_FILE=C:/ruta/al/fitxer/UPperRS.xlsx
```

### Seleccions de demanda per RS i UP

El pipeline de demanda tambe pot limitar les columnes especifiques de RS i UP sense alterar els totals globals:

- `DEMAND__TOTAL` es calcula amb totes les visites.
- Les variables globals sense RS/UP, com `DEMAND__SERVEI_CODI__INF`, tambe es calculen amb totes les visites.
- `selections/selected_rs.csv` limita les columnes agrupades per RS, com `DEMAND__SERVEI_CODI__INF__RS__<RS>` i `DEMAND__TOTAL__RS__<RS>`.
- `selections/selected_up.csv` limita les columnes agrupades per UP, com `DEMAND__SERVEI_CODI__INF__UP__<UP>` i `DEMAND__TOTAL__UP__<UP>`.

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

- Els totals `DIAGNOSIS__TOTAL`, `DIAGNOSIS__TOTAL__RS__*` i `DIAGNOSIS__TOTAL__UP__*` es calculen amb tots els diagnostics de la taula origen.
- Les variables de codi `DIAGNOSIS__ICD10_3__*`, `DIAGNOSIS__ICD10_3__<codi_o_grup>__RS__<RS>` i `DIAGNOSIS__ICD10_3__<codi_o_grup>__UP__<UP>` es calculen nomes per als codis diagnostics seleccionats.
- Els fitxers de RS i UP no canvien el total general; nomes limiten quines columnes agrupades per RS o UP s'escriuen al Parquet final.

Fitxers de seleccio:

| Fitxer | Exemple de capcalera | Efecte |
| --- | --- | --- |
| `selections/selected_diagnosis_codes.csv` | `ICD10_3,feature_name,definition_ca` | Llista de codis o rangs ICD10 de 3 caracters que generen variables `DIAGNOSIS__ICD10_3__*` i variables per grup. `feature_name` permet crear sortides com `G01` o `D01`; `definition_ca` desa la definicio en catala i no forma part del nom de variable. Codis com `U07.1` es normalitzen a `U07`. Rangs com `J00-J06` s'expandeixen. |
| `selections/selected_rs.csv` | `geo_id,RS` | Si existeix i conte valors, limita les columnes `DIAGNOSIS__TOTAL__RS__*` i `DIAGNOSIS__ICD10_3__<codi_o_grup>__RS__<geo_id>`. La columna `RS` ha de coincidir amb el valor font d'`UPperRS.xlsx`; `geo_id` es fa servir al nom final, com `RS_64`. |
| `selections/selected_up.csv` | `geo_id,UP,name` | Si existeix i conte valors, limita les columnes `DIAGNOSIS__TOTAL__UP__*` i `DIAGNOSIS__ICD10_3__<codi_o_grup>__UP__<geo_id>`. La columna `UP` filtra pel codi UP; `geo_id` es fa servir al nom final, com `MICRO_01`. |

El CSV de diagnostics pot tenir `feature_name` i `definition_ca`. Si `feature_name` esta buida, el nom de variable sera el codi ICD10 normalitzat. Si diversos codis o rangs tenen el mateix `feature_name`, el pipeline els suma en una sola variable. `definition_ca` queda disponible com a descripcio humana i no afecta el naming. El mateix codi diagnostic pot apareixer en diverses files: en aquest cas contribueix a totes les variables indicades.

Subconjunts diagnostics configurats actualment:

| Grup | Definicio |
| --- | --- |
| `G01` | Infeccions respiratories agudes i simptomatologia |
| `G02` | Asma, MPOC i malalties respiratories croniques / exacerbacions |
| `G03` | Gastroenteritis i infeccions digestives + simptomes |
| `G04` | Salut mental: ansietat, depressio i trastorns adaptatius |
| `G05` | Dolor musculoesqueletic, lumbalgia i lesions menors |
| `D01` | Infeccio respiratoria aguda de vies altes |
| `D02` | Sindrome gripal |
| `D03` | Bronquiolitis aguda |
| `D04` | Gastroenteritis infecciosa |
| `D05` | Lumbalgia / dolor lumbar |
| `D06` | Ansietat / trastorn d'ansietat |
| `D07` | Diabetis tipus 2 |
| `D08` | COVID-19 |
| `D09` | Hipertensio essencial |

Els subconjunts territorials configurats actualment son les RS `RS_71`, `RS_78`, `RS_79`, `RS_76`, `RS_75`, `RS_62`, `RS_67`, `RS_64`, `RS_61`, `RS_70`, `RS_63` i `RS_00`, i els micro `MICRO_01`, `MICRO_02`, `MICRO_03A`, `MICRO_03B`, `MICRO_04A`, `MICRO_04B`, `MICRO_05`, `MICRO_06A`, `MICRO_06B`, `MICRO_07` i `MICRO_08`.

Exemple sense agrupacions:

```csv
ICD10_3,feature_name,definition_ca
J00,J00,Refredat comu
I10,D09,Hipertensio essencial
A09,D04,Gastroenteritis infecciosa
```

Exemple agrupant diversos codis respiratoris:

```csv
ICD10_3,feature_name,definition_ca
J00,RESPIRATORI,Infeccions respiratories
J02,RESPIRATORI,Infeccions respiratories
J03,RESPIRATORI,Infeccions respiratories
I10,HIPERTENSIO,Hipertensio essencial
```

Aquest exemple genera columnes com `DIAGNOSIS__ICD10_3__RESPIRATORI`, `DIAGNOSIS__ICD10_3__RESPIRATORI__RS__BARCELONA` i `DIAGNOSIS__ICD10_3__RESPIRATORI__UP__00001`, on `RESPIRATORI` suma `J00`, `J02` i `J03`.

Exemple mantenint un codi individual i, alhora, afegint-lo a un grup:

```csv
ICD10_3,feature_name,definition_ca
J00,J00,Refredat comu
J00,RESPIRATORI,Infeccions respiratories
J02,RESPIRATORI,Infeccions respiratories
```

Aquest cas genera tant `DIAGNOSIS__ICD10_3__J00` com `DIAGNOSIS__ICD10_3__RESPIRATORI`. Els casos `J00` compten a totes dues variables, i `J02` nomes a `RESPIRATORI`.

```csv
geo_id,RS
RS_64,GIRONA
RS_61,LLEIDA
```

```csv
geo_id,UP,name
MICRO_01,00348,CAP Bages / Manresa
MICRO_08,06311,CUAP Cotxeres
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

### Com s'interpreta `--end-date`

`--end-date` marca fins quin dia vols tenir cobert el Parquet final, pero no obliga la base de dades a tenir dades reals fins aquell dia. El pipeline separa sempre dos conceptes:

| Concepte | Que vol dir | Exemple amb `--end-date 2026-06-02` |
| --- | --- | --- |
| Rang real observat | Dies que existeixen a la taula origen i que es consulten a Synapse | Si la taula nomes te dades fins `2026-05-27`, el log dira `Processing new demand days: 2026-01-01 -> 2026-05-27` |
| Rang final desitjat | Dies que han d'apareixer al Parquet final | El final s'allarga fins `2026-06-02` amb files imputades per `2026-05-28` -> `2026-06-02` |

Per tant, veure un log que acaba el dia 27 no vol dir que `--end-date 2026-06-02` s'hagi ignorat. Vol dir que el maxim dia real disponible a la font era el 27. Els dies fins al 2 de juny es creen despres en la fase d'imputacio i queden marcats amb `__is_imputed = True`.

Tant demanda com diagnostics apliquen el mateix comportament. Si la font conte timestamps futurs o corruptes, per exemple una data com `4512-06-23`, aquestes dates s'ignoren ja en la consulta que calcula el `MIN/MAX`: la query nomes considera dates des de `MIN_VALID_DATE` fins abans de dema. Aixo evita que pandas falli amb `OutOfBoundsDatetime` i tambe evita que una data futura falsa ampliï el rang real observat.

Executar amb dades sintetiques locals, sense connexio a la base de dades:

```bash
python run_pipeline_optimized.py --sample --all --sample-output-dir data/sample/toy_check
```

Aquest mode llegeix els CSVs de `data/sample/input/`, carrega les seleccions compartides de `selections/` i escriu els Parquet a la carpeta indicada amb `--sample-output-dir`. La toy data actual esta pensada per validar tant demanda com diagnostics amb el naming canonical i amb els subconjunts `G01`-`G05`, `D01`-`D09`, `RS_..` i `MICRO_..`.

Comprovacions rapides de la toy data:

```bash
python -c "import pandas as pd; df=pd.read_parquet('data/sample/toy_check/demand_pipeline/finals/demand_final.parquet'); print(df.shape); print('\n'.join(df.columns[:30]))"
python -c "import pandas as pd; df=pd.read_parquet('data/sample/toy_check/diagnosis_pipeline/finals/diagnosis_final.parquet'); print(df.shape); print('\n'.join(df.columns))"
python -c "import pandas as pd; df=pd.read_parquet('data/sample/toy_check/finals/demand_diagnosis_joined.parquet'); print(df.shape)"
```

En diagnostics, la toy data ha de contenir columnes com:

```text
DIAGNOSIS__ICD10_3__G01
DIAGNOSIS__ICD10_3__G04
DIAGNOSIS__ICD10_3__D08
DIAGNOSIS__ICD10_3__G01__RS__RS_67
DIAGNOSIS__ICD10_3__G01__UP__MICRO_01
DIAGNOSIS__TOTAL__RS__RS_67
DIAGNOSIS__TOTAL__UP__MICRO_01
```

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

Veure files d'un Parquet per rang de dates:

```bash
python run_pipeline.py --show-parquet data/demand_pipeline/finals/demand_final.parquet --start-date 2026-05-20 --end-date 2026-05-28
python run_pipeline.py --show-parquet data/demand_pipeline/finals/demand_final.parquet --start-date 2026-05-20 --end-date 2026-05-28 --parquet-columns timestamp,__is_imputed,__imputation_method --parquet-limit 0
```

Comprovar les columnes d'imputacio i el recompte de files observades/imputades:

```bash
python run_pipeline.py --check-imputation data/demand_pipeline/finals/demand_final.parquet
python run_pipeline.py --check-imputation data/diagnosis_pipeline/finals/diagnosis_final.parquet
python run_pipeline.py --write-imputation-metadata data/demand_pipeline/finals/demand_final.parquet
```

Cada vegada que s'escriu un Parquet final tambe es guarden fitxers de metadata d'imputacio al mateix directori:

| Fitxer | Contingut |
| --- | --- |
| `*_imputation_metadata.json` | Resum amb nombre total de files imputades, recompte per dataset i llista de dates imputades |
| `*_imputed_rows.csv` | Una fila per data imputada amb metode, ultima data real usada i moment de creacio |

Eliminar files d'un Parquet per rang de dates inclusiu:

```bash
python run_pipeline.py --delete-parquet-rows data/demand_pipeline/finals/demand_final.parquet --start-date 2026-05-26 --end-date 2026-05-28 --dry-run
python run_pipeline.py --delete-parquet-rows data/demand_pipeline/finals/demand_final.parquet --start-date 2026-05-26 --end-date 2026-05-28
```

L'eliminacio escriu una copia de seguretat al costat del Parquet abans de sobreescriure'l. Si el fitxer es un final dins `data/*_pipeline/finals/`, tambe crea o sincronitza `data/*_pipeline/incremental/metadata.parquet` amb l'ultim timestamp no imputat anterior a l'inici del rang eliminat. Per exemple, si elimines `2026-01-01` -> `2026-05-29`, el cursor queda a `2025-12-31`. Aixi la propera execucio normal pot tornar a carregar els dies eliminats, encara que el Parquet final conservi dies posteriors. Amb `--dry-run` nomes es mostra que passaria, sense modificar ni el Parquet ni la metadata.

Veure opcions disponibles:

```bash
python run_pipeline.py --help
```

## Comprovar data d'upload a Synapse

El script `scripts/check_source_upload_metadata.py` intenta respondre si Synapse exposa una data real de carrega o ingestio de les taules origen. No modifica dades: nomes fa consultes de lectura.

```powershell
python .\scripts\check_source_upload_metadata.py
```

Que comprova:

- Acces directe a `z_inv.P1038_visites` i `z_inv.P1038_prstb015r_filtrat`.
- Metadata de l'objecte (`create_date`, `modify_date`) des de `INFORMATION_SCHEMA` i `sys.objects`.
- Columnes amb noms tipus `load`, `upload`, `ingest`, `refresh`, `created`, `modified`, etc.
- Peticions recents de carrega a `sys.dm_pdw_exec_requests`, si l'usuari te permis `VIEW DATABASE STATE`.

Important: `modify_date` no es una data fiable d'upload de dades. En views o taules pot indicar quan es va crear o modificar l'objecte, no quan es van carregar les files. Si no hi ha columna d'auditoria ni permisos per mirar la DMV, no es pot saber exactament la darrera carrega des d'aquest codi.

Si la DMV falla per permisos, es pot saltar:

```powershell
python .\scripts\check_source_upload_metadata.py --skip-dmv
```

Si el driver instal.lat te un altre nom:

```powershell
python .\scripts\check_source_upload_metadata.py --driver "ODBC Driver 17 for SQL Server"
```

## Sortides

| Fitxer | Contingut |
| --- | --- |
| `data/demand_pipeline/incremental/*.parquet` | Blocs incrementals de demanda |
| `data/demand_pipeline/finals/demand_final.parquet` | Matriu final de demanda |
| `data/diagnosis_pipeline/incremental/*.parquet` | Blocs incrementals de diagnostics |
| `data/diagnosis_pipeline/finals/diagnosis_final.parquet` | Matriu final de diagnostics |
| `data/finals/demand_diagnosis_joined.parquet` | Demanda i diagnostics units per `timestamp` |
| `data/sample/toy_check/` | Sortides de validacio recomanades per la toy data amb `--sample-output-dir data/sample/toy_check` |
| `data/sample/output/` | Sortides alternatives si executes el mode `--sample` sense canviar la carpeta per defecte |
| `data/sample/multiyear_input/` | CSVs sintetiques multi-any generades per `scripts/create_multiyear_sample.py` |
| `data/sample/multiyear_output/` | Finals Parquet i CSV de la mostra multi-any |

Cap sortida incremental, final o unida hauria de tenir timestamps posteriors a avui. Si el sistema origen retorna dates futures, es descarten abans d'escriure el Parquet.

Les sortides finals poden contenir files imputades fins avui, o fins al `--end-date` si n'has passat un d'anterior a avui, quan la base de dades origen encara no ha publicat dades per als darrers dies. Aquestes files es poden identificar amb les columnes de control descrites a la seccio seguent.

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

## Imputacio fins al dia objectiu

La base de dades origen pot actualitzar-se de manera asincrona. Per exemple, avui pot ser `2026-05-28`, pero l'ultima data real disponible a Synapse pot ser `2026-05-25`. Abans aquests dies posteriors podien quedar com a zeros, cosa que feia semblar que hi havia activitat real igual a zero. Ara el pipeline els tracta com a valors no observats i els imputa. El dia objectiu es `--end-date` si l'has passat i no es futur; si no, es avui.

Com funciona:

- El pipeline consulta dades reals nomes fins al maxim dia disponible a la taula origen, limitat per avui o per `--end-date`.
- Despres de construir el final amb dades reals, completa el calendari diari de `demand_final.parquet` i `diagnosis_final.parquet` fins al dia objectiu.
- Les dates sense entrada al Parquet final es calculen amb la mitjana historica del mateix dia i mes en anys anteriors.
- Si una columna no te historial per aquell mateix dia i mes, usa la mitjana observada de la columna com a fallback.
- Les files reals es marquen amb `__is_imputed = False`.
- Les files estimades es marquen amb `__is_imputed = True`.

Columnes de control afegides als finals:

| Columna | Significat |
| --- | --- |
| `__is_imputed` | `True` si la fila es estimada, `False` si ve de dades reals |
| `__imputation_method` | Metode utilitzat, ara `same_month_day_mean` |
| `__imputation_source_last_date` | Ultim dia real disponible quan es va fer la imputacio |
| `__imputation_created_at` | Moment en que es va crear la imputacio |

Quan la base de dades origen s'actualitza mes tard, aquestes files imputades no compten com a dades reals processades. El pipeline calcula l'ultim dia processat ignorant `__is_imputed = True`, torna a consultar els dies pendents i substitueix les estimacions pels valors reals.

Exemple:

```text
Avui: 2026-05-28
Ultima data real a Synapse: 2026-05-25
Final escrit: fins a 2026-05-28
Files 2026-05-26, 2026-05-27 i 2026-05-28: __is_imputed = True
```

Si el dia seguent Synapse ja conte dades fins a `2026-05-28`, una nova execucio normal:

```bash
python run_pipeline.py --all
```

reprocessara `2026-05-26` -> `2026-05-28` amb valors reals i eliminara les estimacions d'aquells dies.

## Dades sintetiques

El projecte inclou dades petites d'exemple per provar el pipeline sense ODBC ni permisos de base de dades:

| Fitxer | Contingut |
| --- | --- |
| `data/sample/input/up_rs.csv` | Mapping UP -> RS |
| `data/sample/input/demand_visits.csv` | Visites sintetiques per al pipeline de demanda |
| `data/sample/input/diagnosis_visits.csv` | Diagnostics sintetics amb codis que activen `G01`-`G05` i `D01`-`D09` |
| `data/sample/input/selected_codes.csv` | Fallback local dels codis diagnostics; el mode sample normal prioritza `selections/selected_diagnosis_codes.csv` |

El mode sample tambe usa les seleccions reals compartides:

| Fitxer | Us en sample |
| --- | --- |
| `selections/selected_diagnosis_codes.csv` | Defineix `G01`-`G05`, `D01`-`D09`, rangs ICD10 i `definition_ca` |
| `selections/selected_rs.csv` | Mapeja RS font a IDs de sortida com `RS_64` |
| `selections/selected_up.csv` | Mapeja codis UP a IDs de sortida com `MICRO_01` |

Comandes utils:

```bash
python run_pipeline_optimized.py --sample --demand --sample-output-dir data/sample/toy_check
python run_pipeline_optimized.py --sample --diagnosis --sample-output-dir data/sample/toy_check
python run_pipeline_optimized.py --sample --all --sample-output-dir data/sample/toy_check
python run_pipeline_optimized.py --sample --join-final --sample-output-dir data/sample/toy_check
```

Tambe es poden passar carpetes alternatives:

```bash
python run_pipeline_optimized.py --sample --all --sample-input-dir data/sample/input --sample-output-dir data/sample/output
```

### Mostra multi-any per validar files diaries

El script `scripts/create_multiyear_sample.py` crea una mostra sintetica mes gran per validar que el pipeline conserva tots els dies de tots els anys processats. Genera:

Nota: aquesta mostra multi-any serveix sobretot per validar continuitat diaria i retencio d'historic. Per validar el naming actual de demanda/diagnostics i els subconjunts `G01`-`G05`, `D01`-`D09`, `RS_..` i `MICRO_..`, usa la toy data principal amb `run_pipeline_optimized.py --sample --all --sample-output-dir data/sample/toy_check`.

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

La demanda inclou tres nivells de variables: totals globals sense agrupacio (`DEMAND__SERVEI_CODI__INF`, `DEMAND__TIPUS_CLASS__C9C`), totals per RS (`DEMAND__SERVEI_CODI__INF__RS__BARCELONA`) i totals per UP (`DEMAND__SERVEI_CODI__INF__UP__00101`). Les seleccions de RS/UP de demanda nomes limiten les columnes agrupades; `DEMAND__TOTAL` i les variables globals continuen incloent totes les visites. Diagnostics inclou totals reals amb tots els diagnostics (`DIAGNOSIS__TOTAL`, `DIAGNOSIS__TOTAL__RS__*`, `DIAGNOSIS__TOTAL__UP__*`) i variables especifiques per als codis seleccionats o grups de codis (`DIAGNOSIS__ICD10_3__J00`, `DIAGNOSIS__ICD10_3__RESPIRATORI`, `DIAGNOSIS__ICD10_3__RESPIRATORI__RS__BARCELONA`, `DIAGNOSIS__ICD10_3__RESPIRATORI__UP__00001`).

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

Quan calcula el `MIN/MAX` de dates d'una taula origen, aplica un limit superior abans de dema. Aixo inclou tot avui, pero exclou dates futures o corruptes abans que pandas les converteixi a `Timestamp`.

### `pipelines/shared/parquet_storage.py`

Gestiona incrementals Parquet, metadades, retencio opcional de fitxers antics i escriptura de sortides finals. En reconstruccions historiques s'ha de mantenir `retention_days=None` per conservar tots els dies abans de l'agregacio final.

Abans de guardar incrementals o finals, elimina qualsevol fila amb `timestamp` posterior a avui.

Quan llegeix l'ultim timestamp del Parquet final, ignora les files amb `__is_imputed = True`. Aixo permet que una execucio posterior substitueixi estimacions per dades reals quan el servidor ja les tingui disponibles.

### `pipelines/shared/imputation.py`

Conte la logica d'imputacio de cua fins al dia objectiu:

- Elimina files imputades antigues abans de fusionar dades reals noves.
- Afegeix les columnes `__is_imputed`, `__imputation_method`, `__imputation_source_last_date` i `__imputation_created_at`.
- Calcula estimacions amb la mitjana historica del mateix dia i mes.
- Mante separades les files observades i les estimades per poder substituir-les en futures execucions.

### `pipelines/demand/`

Processa visites:

- Converteix `DATA_VISITA` a dia.
- Normalitza `UP`.
- Afegeix `RS` a partir de l'Excel `UPperRS.xlsx`.
- Classifica tipus de visita en presencial, domiciliaria, telefonica o `NA`.
- Agrega comptatges globals per dia i per dimensions com lloc, situacio, servei i tipus amb totes les visites.
- Usa `selections/selected_rs.csv` i `selections/selected_up.csv` per limitar quines RS i UP apareixen en les columnes agrupades de demanda, sense canviar `DEMAND__TOTAL` ni les variables globals.
- No consulta ni guarda visites amb data futura.

### `pipelines/diagnosis/`

Processa diagnostics:

- Valida que la taula origen tingui les columnes requerides.
- Consulta la base agregant en SQL per dia, UP i codi diagnostic normalitzat de 3 caracters.
- Converteix la data de visita a `timestamp` diari.
- Afegeix `RS` a partir de l'Excel `UPperRS.xlsx`.
- Genera totals amb tots els diagnostics: `DIAGNOSIS__TOTAL`, `DIAGNOSIS__TOTAL__RS__*` i `DIAGNOSIS__TOTAL__UP__*`.
- Genera variables de codis seleccionats o grups de codis: `DIAGNOSIS__ICD10_3__*`, `DIAGNOSIS__ICD10_3__<codi_o_grup>__RS__<geo_id>` i `DIAGNOSIS__ICD10_3__<codi_o_grup>__UP__<geo_id>`.
- Si `selections/selected_diagnosis_codes.csv` conte `feature_name`, usa aquest nom en les variables i suma tots els codis o rangs que comparteixen el mateix nom.
- Usa `selections/selected_rs.csv` i `selections/selected_up.csv` per limitar quines RS i UP apareixen en les columnes agrupades, i pot mapar-les a IDs de sortida com `RS_64` o `MICRO_01`, sense canviar el total general.
- No consulta ni guarda diagnostics amb data futura.

### `pipelines/shared/final_joiner.py`

Uneix `demand_final.parquet` i `diagnosis_final.parquet` per `timestamp`, conserva les columnes canoniques `DEMAND__...` i `DIAGNOSIS__...`, descarta timestamps futurs i desa el resultat final.

### `scripts/check_source_upload_metadata.py`

Script de diagnostic per comprovar si Synapse exposa alguna pista sobre la data real d'upload o ingestio de les fonts. Revisa columnes d'auditoria, metadata d'objecte i, si hi ha permisos, historial recent de peticions de carrega. No escriu res a la base de dades.

## Validacio rapida

Aquestes comprovacions no requereixen connexio a la base de dades:

```bash
python run_pipeline_optimized.py --help
python run_pipeline_optimized.py --sample --all --sample-output-dir data/sample/toy_check
python run_pipeline_optimized.py --sample --all --sample-output-dir data/sample/toy_check --start-date 2026-01-02 --end-date 2026-01-03
python run_pipeline_optimized.py --convert-parquet data/sample/toy_check/finals/demand_diagnosis_joined.parquet --to csv
python -B scripts/create_multiyear_sample.py
python -m compileall -q run_pipeline.py run_pipeline_optimized.py config pipelines validate_project.py
python -m pytest -q
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
