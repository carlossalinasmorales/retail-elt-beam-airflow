import csv
import json
# from datetime import datetime, timezone
import argparse

import apache_beam as beam
from apache_beam.io.parquetio import ReadFromParquet, WriteToParquet
import pyarrow as pa
from pathlib import Path


#Data dirs test
# data_inputs_test = "../data/inputs/"
# data_outputs_psa_test = "../data/psa/"
# data_outputs_errors_test = "../data/errors/"
# data_outputs_gold_test = "../data/gold/"

#Data dirs airflow
data_inputs = "/opt/airflow/data/inputs/"
data_outputs_psa = "/opt/airflow/data/psa/"
data_outputs_errors = "/opt/airflow/data/errors/"
data_outputs_gold = "/opt/airflow/data/gold/"

#Funciones para paths

def read_psa_path(proc_date) -> str:
    return str(Path(data_outputs_psa)/ f"proc_date={proc_date}"/ "psa-sales*.parquet")


#Funcion para parsear tipo de cambio
class ParsearYValidarContrato(beam.DoFn):
    def process(self, row):
        errores = []

        try:
            monto_original = float(row["monto_original"])

            if monto_original <= 0:
                errores.append("monto menor a cero")

        except Exception:
            errores.append("Monto no corresponde a un formato numerico valido (float/int)")
            monto_original = None

        ciudad = str(row.get("ciudad", ""))

        if ciudad not in ["Santiago", "Lima", "Buenos Aires"]:
            errores.append("Ciudad no autorizada en contrato regional")

        if errores:
            yield beam.pvalue.TaggedOutput(
                "corruptos",
                {
                    "raw_record": dict(row),
                    "motivo_rechazo": " o ".join(errores),
                },
            )
            return

        yield {
            "id_transaccion": str(row["id_transaccion"]),
            "ciudad": ciudad,
            "monto_original": monto_original,
            "moneda_origen": str(row["moneda_origen"]),
            "fecha_transaccion": str(row["fecha_transaccion"]),
        }

#Parsear tipo de cambio como side input de Beam
def parse_tipo_cambio(line):
    row = next(csv.reader([line]))
    return (row[0], float(row[2]))  # (codigo_moneda, factor_usd)


#Convertir a gold usando side input
class ConvertirGoldUSD(beam.DoFn):
    def process(self, row, tipo_cambio):
        factor_usd = tipo_cambio[row["moneda_origen"]]

        yield {
            "id_transaccion": row["id_transaccion"],
            "ciudad": row["ciudad"],
            "monto_usd": round(row["monto_original"] * factor_usd, 3),
            "fecha_compras": row["fecha_transaccion"],
        }

#schema gold parquet
schema_gold = pa.schema([
    ("id_transaccion", pa.string()),
    ("ciudad", pa.string()),
    ("monto_usd", pa.float64()),
    ("fecha_compras", pa.string()),
])

#Date para test de script
# proc_date = datetime.now(timezone.utc).strftime("%Y-%m-%d") 

#Date como argumento para que funcione con airflow
parser = argparse.ArgumentParser()
parser.add_argument("--proc_date", required=True)
args, beam_args = parser.parse_known_args()
proc_date = args.proc_date


output_gold_path = (Path(data_outputs_gold)/ f"proc_date={proc_date}"/ "gold-sales")
output_errors_path = (Path(data_outputs_errors)/ f"proc_date={proc_date}"/ "anomalies-sales")

with beam.Pipeline(argv=beam_args) as p:

    # Side input: leer tipo de cambio como PCollection de pares (codigo_moneda, factor_usd)
    tipo_cambio = (
        p
        | "Leer tipo de cambio" >> beam.io.ReadFromText(
            data_inputs + "tipo_cambio.csv", skip_header_lines=1
        )
        | "Parsear tipo de cambio" >> beam.Map(parse_tipo_cambio)
    )

    resultado = (
        p
        | "Leer PSA" >> ReadFromParquet(read_psa_path(proc_date))
        | "Parsear y validar contrato" >> beam.ParDo(
            ParsearYValidarContrato()
        ).with_outputs(
            "corruptos",
            main="validos"
        )
    )

    validos = resultado.validos
    corruptos = resultado.corruptos

    # Deduplicar por id_transaccion antes de convertir a USD
    validos_dedup = (
        validos
        | "Key por id_transaccion" >> beam.Map(lambda row: (row["id_transaccion"], row))
        | "Deduplicar" >> beam.GroupByKey()
        | "Tomar primer registro" >> beam.Map(lambda kv: next(iter(kv[1])))
    )

    gold = (
        validos_dedup
        | "Convertir monto a USD" >> beam.ParDo(
            ConvertirGoldUSD(), beam.pvalue.AsDict(tipo_cambio)
        )
    )

    (
        corruptos
        | "Errores a JSON" >> beam.Map(
            lambda row: json.dumps(row, ensure_ascii=False)
        )
        | "Guardar DLQ" >> beam.io.WriteToText(
            file_path_prefix=str(output_errors_path),
            file_name_suffix=".json"
        )
    )

    (
        gold
        | "Guardar Capa Gold Parquet" >> WriteToParquet(
            file_path_prefix=str(output_gold_path),
            schema=schema_gold,
            file_name_suffix=".parquet"
        )
    )
