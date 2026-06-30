[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/)
[![Apache Beam](https://img.shields.io/badge/apache%20beam-2.57.0-orange.svg)](https://beam.apache.org/)
[![Apache Airflow](https://img.shields.io/badge/apache%20airflow-3.2.2-teal.svg)](https://airflow.apache.org/)
[![uv](https://img.shields.io/badge/dependencies-uv-purple.svg)](https://docs.astral.sh/uv/)
[![Docker Compose](https://img.shields.io/badge/docker%20compose-local%20stack-2496ED.svg)](https://docs.docker.com/compose/)

# Retail ELT con Apache Beam y Airflow

Este proyecto construye un pipeline ELT distribuido para una empresa de retail con operaciones en Santiago, Lima y Buenos Aires. La solución utiliza **Apache Beam** para el procesamiento de datos y **Apache Airflow** para la orquestación.

## ⭕ Objetivo del proyecto

El objetivo es ingerir fuentes de ventas heterogéneas, estandarizarlas en una **Persistent Staging Area (PSA)**, validarlas con un **Data Contract**, convertir los montos locales a **USD** y publicar el resultado curado en una **Gold Layer**.

## ⭕ Arquitectura

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

## ⭕ Estructura del proyecto

```text
retail-elt-beam-airflow/
├── dags/
│   └── dag_orden_ventas_latam.py
├── beam_jobs/
│   ├── job1_ingest_to_psa.py
│   └── job2_transform_gold.py
├── generate-data/
│   ├── generar_datos_mock.py
│   └── generar_datos_random.py
├── data/
│   ├── inputs/
│   ├── psa/
│   ├── gold/
│   └── errors/
├── docker-compose.yml
├── Dockerfile
├── pyproject.toml
└── README.md
```

## ⭕ Scripts Python

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

### `beam_jobs/job1_ingest_to_psa.py`

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

### `beam_jobs/job2_transform_gold.py`

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

### `generate-data/generar_datos_mock.py`

Script auxiliar para crear un conjunto pequeño y determinístico de datos de prueba basados en el caso de estudio de la tarea.

Responsabilidades:
- crear la carpeta `data/inputs/` si no existe
- generar el archivo CSV de Santiago
- generar el archivo Parquet de Lima
- generar el archivo JSON Lines de Buenos Aires
- generar el archivo maestro `tipo_cambio.csv`

Uso recomendado:
- sirve para poblar rápidamente el proyecto con el mismo escenario esperado por la rúbrica
- es útil para una demostración controlada del pipeline

### `generate-data/generar_datos_random.py`

Script auxiliar para generar datasets aleatorios y parametrizables por sede.

Responsabilidades:
- pedir por consola la cantidad de registros por sede
- pedir el porcentaje de errores por sede
- generar datos válidos e inválidos de forma aleatoria
- producir archivos de entrada listos para probar validaciones, DLQ y comportamiento del pipeline

Uso recomendado:
- sirve para probar distintos escenarios de calidad de datos
- permite estresar el Data Contract con montos negativos, ciudades inválidas y valores corruptos

## ⭕ Datos de entrada

Archivos esperados en `data/inputs/`:

- `ventas_santiago.csv`
- `ventas_buenos_aires.json`
- `ventas_lima.parquet`
- `tipo_cambio.csv`

## ⭕ Datos de salida

En el estado actual de desarrollo local, las salidas se escriben en:

- `data/psa/proc_date=YYYY-MM-DD/`
- `data/gold/proc_date=YYYY-MM-DD/`
- `data/errors/proc_date=YYYY-MM-DD/`

## ⭕‼️ Cómo ejecutar y probar el proyecto 

> **Sección principal de uso del proyecto.**
> Si solo necesitas levantar el entorno y validar el pipeline, sigue estos pasos en orden.

### Prerrequisitos

Antes de ejecutar el proyecto debes tener instalado:

- **Python 3.12**
- **uv**
- **Docker** 🐳
- **Docker Compose**

### Instalar uv

Si no tienes `uv`, puedes instalarlo con:

```bash
pip install uv
```

O siguiendo la documentación oficial de uv:

```text
https://docs.astral.sh/uv/
```

## Preparación del entorno local

### Crear el entorno virtual

Desde la raíz del proyecto:

```bash
uv venv
```

Esto crea un entorno virtual local en `.venv/`.

### Instalar dependencias del proyecto

```bash
uv sync
```

Si también quieres instalar dependencias opcionales de desarrollo:

```bash
uv sync --group dev
```


## Prueba con datos aleatorios y errores controlados

> Esta opción permite generar distintos volúmenes de datos y porcentajes de error para probar validaciones y DLQ.

1. Ejecutar el generador aleatorio:

```bash
uv run python generate-data/generar_datos_random.py
```

2. Ingresar por consola:
- cantidad de registros para Santiago
- porcentaje de errores para Santiago
- cantidad de registros para Lima
- porcentaje de errores para Lima
- cantidad de registros para Buenos Aires
- porcentaje de errores para Buenos Aires

3. Levantar Airflow si aún no está levantado:

```bash
docker compose build --no-cache
docker compose up airflow-init
docker compose up -d
```

4. Lanzar el DAG desde Airflow con un trigger manual para no esperar a la hora de programacion (entrar a http://localhost:8080)

5. Verificar que:
- los registros válidos lleguen a Gold
- los registros corruptos lleguen a Errors
- la ejecución no falle por filas inválidas aisladas


---

## ⭕ Opcional: Ejecutar los jobs manualmente

También se pueden ejecutar los jobs Beam manualmente con uv teniendo airflow activo en Docker.

### Job 1

```bash
docker compose exec airflow-scheduler python /opt/airflow/beam_jobs/job1_ingest_to_psa.py --proc_date 2026-06-29
```

### Job 2

```bash
docker compose exec airflow-scheduler python /opt/airflow/beam_jobs/job2_transform_gold.py --proc_date 2026-06-29
```

---

## ⭕ Notas sobre Airflow

- Airflow 3 se ejecuta mediante Docker
- las dependencias se resuelven con **uv** a través de `pyproject.toml`
- `apache-airflow` está fijado en `pyproject.toml` para coincidir con la versión de la imagen Docker
- el scheduler necesita un `JWT secret` compartido y una `execution API URL` válida para ejecutar tareas correctamente en Airflow 3
- la carpeta `./data` del proyecto está montada al contenedor como `/opt/airflow/data` mediante un **bind mount** en `docker-compose.yml`; por eso los jobs escriben usando rutas `/opt/airflow/...` dentro del contenedor, pero los archivos aparecen también en la carpeta local `data/`

## ⭕ Notas FinOps

- **Parquet** se utiliza en PSA y Gold porque es un formato columnar con mejor compresión y menor costo de lectura que formatos de texto fila a fila.
- La capa **PSA** preserva particiones históricas por fecha de proceso, mejorando auditabilidad y reprocesamiento.
- El pipeline aísla registros inválidos en la **DLQ** en lugar de botar el proceso completo, reduciendo costo operativo en ejecuciones diarias.

## ❗ Anexo de uso de IA

Todo uso de IA en este trabajo fue revisado y editado manualmente antes de aplicar cambios.

### Prompts utilizados

- `Responde el modelo de las 4 preguntas de BEAM (What, When, Where, How) con este contexto`
- `Has un ejemplo de ingesta de datos utilizando schemas y comparalo con pandas`
- `Como puedo visualizar santiago_to_output(df_santiago) en formato tabla de una df de pandas`
- `Cual es la mejor manera de unificar multiples fuentes de datos con beam`
- `Muestrame como guardar lo que crea un pipeline en beam con este formato: carpeta historica (PSA) añadiendo la fecha de procesamiento (proc date=YYYY-MM-DD).`
- `Escribe el docker compose para levantar airflow`
- `Dame la manera mas simple de pasarle argumentos para que el date de airflow funcione con beam con el codigo actual`
- `Configura el dockerfile y docker compose basandote en uv para correr airflow`
- `Revisa los archivos de beam_jobs y dags y hazme un listado de mejoras para lectura del codigo como eliminar ruido, etc`
