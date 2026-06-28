import csv
import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.parquetio import ReadFromParquet, WriteToParquet
import pyarrow as pa
from pathlib import Path

#Data dirs
data_inputs = "../data/inputs/"
data_santiago = "ventas_santiago.csv"
data_buenos_aires = "ventas_buenos_aires.json"
data_lima = "ventas_lima.parquet"

data_outputs_psa = "../data/outputs/psa/"

# ---------- Funciones globales ----------

def ingestion_time():
    return datetime.now(timezone.utc).isoformat()

# ---------- Santiago (CSV) ----------

def parse_csv_santiago(line):
    row = next(csv.reader([line]))

    return {
        "id_transaccion": row[0],
        "ciudad": row[1],
        "monto": row[2],
        "fecha": row[3],
    }


def santiago_to_output(row):
    return {
        "id_transaccion": row["id_transaccion"],
        "ciudad": row["ciudad"],
        "monto_original": row["monto"],
        "moneda_origen": "CLP",
        "fecha_transaccion": row["fecha"],
        "ingestado_at": ingestion_time(),
    }


# ---------- Buenos Aires (JSON) ----------

def parse_json_buenos_aires(line):
    row = json.loads(line)
    return {
        "tx_id": row["tx_id"],
        "sucursal": row["sucursal"],
        "total": str(row["total"]),
        "timestamp": row["timestamp"],
    }


def buenos_aires_to_output(row):
    return {
        "id_transaccion": row["tx_id"],
        "ciudad": row["sucursal"],
        "monto_original": row["total"],
        "moneda_origen": "ARS",
        "fecha_transaccion": row["timestamp"],
        "ingestado_at": ingestion_time(),
    }


# ---------- Lima (Parquet) ----------

def lima_to_output(row):
    return {
        "id_transaccion": str(row["transaction_id"]),
        "ciudad": str(row["city"]),
        "monto_original": str(row["local_value"]),
        "moneda_origen": "PEN",
        "fecha_transaccion": str(row["sales_date"]),
        "ingestado_at": ingestion_time(),
    }



#*************Pipeline*************

#Se define el schema para guardar parquet (es un duplicado del que SalesOutput)
schema_parquet = pa.schema([
    ("id_transaccion", pa.string()),
    ("ciudad", pa.string()),
    ("monto_original", pa.string()),
    ("moneda_origen", pa.string()),
    ("fecha_transaccion", pa.string()),
    ("ingestado_at", pa.string()),
])

#Se crea el outputh path apra guardar en formato historico
proc_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
output_path = (
    Path(data_outputs_psa)
    / f"proc_date={proc_date}"
    / "sales"
)

with beam.Pipeline() as p:
    sales_santiago_csv = (
        p
        | "Leer CSV Santiago" >> beam.io.ReadFromText(data_inputs + data_santiago, skip_header_lines=1)
        | "Parsear CSV Santiago" >> beam.Map(parse_csv_santiago)
        | "Dar formato de output a CSV Santiago" >> beam.Map(santiago_to_output)
    )
    sales_buenos_aires_json = (
        p
        | "Leer JSON Buenos Aires" >> beam.io.ReadFromText(data_inputs + data_buenos_aires)
        | "Parsear JSON Buenos Aires" >> beam.Map(parse_json_buenos_aires)
        | "Dar formato de output a JSON Buenos Aires" >> beam.Map(buenos_aires_to_output)
    )
    sales_lima_parquet = (
        p
        | "Leer PARQUET Lima" >> beam.io.ReadFromParquet(data_inputs + data_lima)
        | "Dar formato de output a PARQUET Lima" >> beam.Map(lima_to_output)
    )
    sales_unificadas = (
        (sales_santiago_csv, sales_lima_parquet, sales_buenos_aires_json)
        | "Unificar data" >> beam.Flatten()
    )
    sales_unificadas | "Guardar Parquet" >> WriteToParquet(
        file_path_prefix=str(output_path),
        schema=schema_parquet,
        file_name_suffix=".parquet"
    )

