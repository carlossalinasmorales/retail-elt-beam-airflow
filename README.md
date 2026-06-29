## Retail ELT con Apache Beam y Airflow

Este proyecto construye un pipeline ELT distribuido para una empresa de retail con operaciones en Santiago, Lima y Buenos Aires. La solución utiliza **Apache Beam** para el procesamiento de datos y **Apache Airflow** para la orquestación.

## Objetivo del proyecto

El objetivo es ingerir fuentes de ventas heterogéneas, estandarizarlas en una **Persistent Staging Area (PSA)**, validarlas con un **Data Contract**, convertir los montos locales a **USD** y publicar el resultado curado en una **Gold Layer**.

## Arquitectura

El pipeline está compuesto por dos jobs independientes de Apache Beam orquestados por un DAG de Airflow:

1. **Job 1 - Ingesta a PSA**
   - Lee en paralelo las tres fuentes de entrada:
     - CSV desde Santiago
     - JSON Lines desde Buenos Aires
     - Parquet desde Lima
   - Homogeneiza todos los registros a un esquema común
   - Escribe salida histórica e inmutable particionada por `proc_date=YYYY-MM-DD`
   - Guarda la capa PSA en **Apache Parquet**

2. **Job 2 - Transformación a Gold**
   - Lee la partición PSA correspondiente a la fecha de ejecución
   - Valida el contrato de datos
   - Envía registros inválidos a una **Dead-Letter Queue (DLQ)** en formato JSON
   - Convierte los registros válidos a USD usando `tipo_cambio.csv`
   - Escribe la salida final curada en la capa Gold en Parquet

3. **DAG de Airflow**
   - Fuerza el orden de ejecución: `Job 1 >> Job 2`
   - Usa la macro de Airflow `{{ ds }}` para la fecha de proceso
   - Aplica reintentos y `retry_delay` para resiliencia
   - Limpia la partición del día antes de escribir para mantener idempotencia en reejecuciones de la misma fecha

## Estructura del proyecto

```text
retail-elt-beam-airflow/
├── dags/
│   └── dag_orden_ventas_latam.py
├── beam-jobs/
│   ├── job1_ingest_to_psa.py
│   └── job2_transform_gold.py
├── data/
│   ├── inputs/
│   └── outputs/
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
└── README.md
```

## Scripts Python

### `dags/dag_orden_ventas_latam.py`

DAG de Airflow que orquesta el pipeline completo.

Responsabilidades:
- eliminar la partición de salida existente para la fecha de ejecución
- ejecutar primero el Job 1
- ejecutar el Job 2 solo si el Job 1 finaliza con éxito
- aplicar reintentos y tiempo de espera entre reintentos

Comportamiento clave:
- usa `{{ ds }}` como fecha de procesamiento
- evita duplicados en una misma fecha limpiando la partición antes de ejecutar

### `beam-jobs/job1_ingest_to_psa.py`

Job de Apache Beam para la ingesta y creación de la capa PSA.

Responsabilidades:
- leer CSV, JSON y Parquet en paralelo
- mapear todas las fuentes a un esquema unificado
- preservar `monto_original` como **string**
- inyectar `moneda_origen` según la procedencia
- agregar `ingestado_at`
- escribir el resultado en PSA en formato Parquet

Parámetro de ejecución:
- `--proc_date` (obligatorio): fecha de procesamiento usada para construir la ruta de salida PSA

### `beam-jobs/job2_transform_gold.py`

Job de Apache Beam para validación y transformación.

Responsabilidades:
- leer la partición PSA de la fecha solicitada
- validar reglas del contrato:
  - `monto_original` debe ser numérico
  - `monto_original` debe ser `> 0`
  - `ciudad` debe ser una de `Santiago`, `Lima`, `Buenos Aires`
- enviar registros inválidos a DLQ en JSON
- convertir registros válidos a USD usando `tipo_cambio.csv`
- escribir la salida válida en Gold en Parquet

Parámetro de ejecución:
- `--proc_date` (obligatorio): fecha de procesamiento usada para leer PSA y escribir Gold/DLQ

## Datos de entrada

Archivos esperados en `data/inputs/`:

- `ventas_santiago.csv`
- `ventas_buenos_aires.json`
- `ventas_lima.parquet`
- `tipo_cambio.csv`

## Datos de salida

En el estado actual de desarrollo local, las salidas se escriben en:

- `data/outputs/psa/proc_date=YYYY-MM-DD/`
- `data/outputs/gold/proc_date=YYYY-MM-DD/`
- `data/outputs/errors/proc_date=YYYY-MM-DD/`

## Cómo ejecutar

### 1. Instalar dependencias locales con uv

```bash
uv sync
```

Dependencias opcionales de desarrollo:

```bash
uv sync --group dev
```

### 2. Construir e iniciar Airflow

```bash
docker compose build --no-cache
docker compose up airflow-init
docker compose up -d
```

### 3. Abrir la interfaz de Airflow

```text
http://localhost:8080
```

### 4. Ejecutar el DAG

Activar el DAG `retail_sales_beam_pipeline` y lanzarlo manualmente desde la interfaz.

## Ejecutar los jobs manualmente

También se pueden ejecutar los jobs Beam manualmente con uv.

### Job 1

```bash
uv run python beam-jobs/job1_ingest_to_psa.py --proc_date 2026-06-29
```

### Job 2

```bash
uv run python beam-jobs/job2_transform_gold.py --proc_date 2026-06-29
```

## Notas sobre Airflow

- Airflow 3 se ejecuta mediante Docker
- las dependencias se resuelven con **uv** a través de `pyproject.toml`
- `apache-airflow` está fijado en `pyproject.toml` para coincidir con la versión de la imagen Docker
- el scheduler necesita un `JWT secret` compartido y una `execution API URL` válida para ejecutar tareas correctamente en Airflow 3

## Notas FinOps

- **Parquet** se utiliza en PSA y Gold porque es un formato columnar con mejor compresión y menor costo de lectura que formatos de texto fila a fila.
- La capa **PSA** preserva particiones históricas por fecha de proceso, mejorando auditabilidad y reprocesamiento.
- El pipeline aísla registros inválidos en la **DLQ** en lugar de botar el proceso completo, reduciendo costo operativo en ejecuciones diarias.

## Limitaciones / brechas actuales

- El cruce con `tipo_cambio.csv` actualmente se carga en memoria con un diccionario Python en lugar de un Side Input distribuido de Beam.
- La estructura actual usa `beam-jobs/` y `data/outputs/...`; si la entrega exige estructura literal, esto podría requerir renombre.
- La deduplicación en la capa Gold no está implementada explícitamente.

## Anexo de uso de IA

Todo uso de IA en este trabajo fue revisado y editado manualmente antes de aplicar cambios.

### Prompts utilizados

- `Responde el modelo de las 4 preguntas de BEAM (What, When, Where, How) con este contexto`
- `Has un ejemplo de ingesta de datos utilizando schemas y comparalo con pandas`
- `Como puedo visualizar santiago_to_output(df_santiago) en formato tabla de una df de pandas`
- `Cual es la mejor manerad e unificar multiples fuentes de datos con beam`
- `Muestrame como guardar lo que crea un pipeline en beam con este formato: carpeta historica (PSA) añadiendo la fecha de procesamiento (proc date=YYYY-MM-DD).`
- `Escribe el docker compose para levantar airflow`
- `Dame la manera mas simple de pasarle argumentos para que el date de airflow funcione con beam con el codigo actual`
- `Configura el dockerfile y docker compose basandote en uv para correr airflow`
