import csv
import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.parquetio import ReadFromParquet, WriteToParquet
import pyarrow as pa
from pathlib import Path


#Data dirs
data_inputs = "../data/inputs/"
data_outputs_psa = "../data/outputs/psa/"
data_outputs_errors = "../data/outputs/errors/"
data_outputs_gold = "../data/outputs/gold/"

#Funciones para paths
def get_proc_date():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def read_psa_path(proc_date) -> str:
    return str(Path(data_outputs_psa)/ f"proc_date={proc_date}"/ "sales*.parquet")



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

#Funcion apra cargar tipo de cambio
def cargar_tipo_cambio():
    tipo_cambio = {}

    with open(Path(data_inputs) / "tipo_cambio.csv", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            tipo_cambio[row["codigo_moneda"]] = float(row["factor_usd"])

    return tipo_cambio


TIPO_CAMBIO = cargar_tipo_cambio()


#Convertir a gold
#Dado la estructura del etl asumimos que moneda origen siempre existe en TIPO_CAMBIO
class ConvertirGoldUSD(beam.DoFn):
    def process(self, row):
        factor_usd = TIPO_CAMBIO[row["moneda_origen"]]

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


proc_date = get_proc_date()
output_gold_path = (Path(data_outputs_gold)/ f"proc_date={proc_date}"/ "sales")
output_errors_path = (Path(data_outputs_errors)/ f"proc_date={proc_date}"/ "sales")

with beam.Pipeline() as p:

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

    gold = (
        validos
        | "Convertir monto a USD" >> beam.ParDo(ConvertirGoldUSD())
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