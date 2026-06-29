## BEAM Model

### What?
Construir un pipeline de datos con **Apache Beam** compuesto por dos jobs: uno para la ingesta y homogenización de datos hacia la capa PSA, y otro para validar, transformar y generar la capa Gold en USD.

### When?
El pipeline se ejecuta **diariamente** mediante **Apache Airflow**, utilizando la macro `{{ ds }}` para garantizar idempotencia y permitir reprocesamientos.

### Where?
Los datos provienen de **Santiago (CSV), Lima (Parquet) y Buenos Aires (JSON)**. Se almacenan primero en la **PSA** (Parquet), luego en la **Gold Layer**, mientras que los registros inválidos se envían a la **Dead-Letter Queue (DLQ)**.

### How?
Se implementan dos jobs de **Apache Beam** orquestados por **Airflow**. El primero realiza la ingesta y homogenización de datos; el segundo aplica el Data Contract, valida los registros, convierte los montos a USD usando un archivo de tipos de cambio y almacena los datos válidos en Gold y los errores en la DLQ.



Iniciar airflow por primera vez con docker compose
docker compose up airflow-init
docker compose up


Notas:
-No se hicieron mas validaciones ni manejo de errores que las solicitadas por falta de tiempo



## Uso de IA
Todo uso de IA utilziado en este trabajo fue **checkeado y editado** detalladamente despues de obtener las respuestas.

-----
### Responder el modelo de las 4 pregutnas de BEAM rapidamente
Prompt: "Responde el modelo de las 4 preguntas de BEAM (What, When, Where, How) con este contexto"

-----

### job_ingest_to_psa.py
Prompt: "Has un ejemplo de ingesta de datos utilizando schemas y comparalo con pandas"

### notebooks/data_transformations.ipynb
Prompt: "Como puedo visualizar santiago_to_output(df_santiago) en formato tabla de una df de pandas" 

### job_ingest_to_psa.py
Prompt: "Cual es la mejor manerad e unificar multiples fuentes de datos con beam"

### job_ingest_to_psa.py
Prompt: "Muestrame como guardar lo que crea un pipeline en beam con este formato: carpeta historica (PSA) añadiendo la fecha de procesamiento
(proc date=YYYY-MM-DD)."

### docker-compose.yml
Prompt: "Escribe el docker compose para levantar airflow"


### job_ingest_to_psa.py y job2_transform_gold.py
Prompt: "Dame la manera mas simple de pasarle argumentos para que el date de airflow funcione con beam con el codigo actual"
