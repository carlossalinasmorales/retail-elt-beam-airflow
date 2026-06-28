## BEAM Model

### What?
Construir un pipeline de datos con **Apache Beam** compuesto por dos jobs: uno para la ingesta y homogenización de datos hacia la capa PSA, y otro para validar, transformar y generar la capa Gold en USD.

### When?
El pipeline se ejecuta **diariamente** mediante **Apache Airflow**, utilizando la macro `{{ ds }}` para garantizar idempotencia y permitir reprocesamientos.

### Where?
Los datos provienen de **Santiago (CSV), Lima (Parquet) y Buenos Aires (JSON)**. Se almacenan primero en la **PSA** (Parquet), luego en la **Gold Layer**, mientras que los registros inválidos se envían a la **Dead-Letter Queue (DLQ)**.

### How?
Se implementan dos jobs de **Apache Beam** orquestados por **Airflow**. El primero realiza la ingesta y homogenización de datos; el segundo aplica el Data Contract, valida los registros, convierte los montos a USD usando un archivo de tipos de cambio y almacena los datos válidos en Gold y los errores en la DLQ.