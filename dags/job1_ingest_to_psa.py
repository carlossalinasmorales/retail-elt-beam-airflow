import csv
import json
from typing import NamedTuple #Libreria para crear estrucutras de datos con nomrbes para cada campo
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io.parquetio import ReadFromParquet, WriteToParquet
import pyarrow as pa

#Data dirs
data_inputs = "./data/inputs/"
data_outputs_psa = "./data/onputs/psa/"

#*************Schemas*************

#----OUTPUT----
class SalesOutput(NamedTuple):
    id_transaccion: str
    ciudad: str
    monto_original: str
    moneda_origen: str
    fecha_transaccion: str
    ingestado_at: str

#----Santiago----
class SalesSantiagoInput(NamedTuple):
    id_transaccion: str
    ciudad: str
    monto: str
    fecha: str

#----Buenos Aires----
class SalesBuenosAiresInput(NamedTuple):
    tx_id: str
    sucursal: str
    total: str
    timestamp: str

#----Lima----
class SalesLimaInput(NamedTuple):
    transaccion_id: str
    city: str
    local_value: str
    sales_date: str


#*************Functions*************
def ingestion_time():
    #Funcion que devuelve el el momento actual exacto en UTC ISO
    return datetime.now(timezone.utc).isoformat()

#---Funciones especificas para Santiago (CSV)---
def parse_csv_santiago(line):
    row = next(csv.reader([line]))
    return SalesSantiagoInput(
        id_transaccion=row[0],
        ciudad=row[1],
        monto=row[2],
        fecha=row[3],
    )

def santiago_to_output(row):
    return SalesOutput(
        id_transaccion=row.id_transaccion,
        ciudad=row.ciudad,
        monto_original=row.monto,
        moneda_origen="CLP",
        fecha_transaccion=row.fecha,
        ingestado_at=ingestion_time(),
    )



#*************Pipeline*************
beam.coders.registry.register_coder(SalesOutput, beam.coders.RowCoder)

beam.coders.registry.register_coder(SalesSantiagoInput, beam.coders.RowCoder)
beam.coders.registry.register_coder(SalesBuenosAiresInput, beam.coders.RowCoder)
beam.coders.registry.register_coder(SalesLimaInput, beam.coders.RowCoder)
