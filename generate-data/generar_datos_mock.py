import os
import pandas as pd
import json
from datetime import datetime

def crear_entorno_datos():
    # 1. Crear la estructura de carpetas de entrada si no existe
    inputs_dir = os.path.join("../", "data", "inputs")
    os.makedirs(inputs_dir, exist_ok=True)
    print(f"📁 Carpetas creadas o verificadas en: {inputs_dir}")

    # Definir la fecha base del caso de estudio
    fecha_proceso = "2026-06-10"

    # ==========================================
    # FUENTE A: SANTIAGO (ventas_santiago.csv)
    # ==========================================
    csv_path = os.path.join(inputs_dir, "ventas_santiago.csv")
    datos_santiago = {
        "id_transaccion": ["STGO-001", "STGO-002"],
        "ciudad": ["Santiago", "Santiago"],
        "monto": [25000, 42000],
        "fecha": [fecha_proceso, fecha_proceso]
    }
    df_santiago = pd.DataFrame(datos_santiago)
    df_santiago.to_csv(csv_path, index=False)
    print(f"✅ Archivo CSV creado: {csv_path}")

    # ==========================================
    # FUENTE B: LIMA (ventas_lima.parquet)
    # ==========================================
    parquet_path = os.path.join(inputs_dir, "ventas_lima.parquet")
    datos_lima = {
        "transaction_id": ["LIMA-981", "LIMA-982"],
        "city": ["Lima", "Lima"],
        "local_value": [150.50, 89.90],
        "sales_date": [fecha_proceso, fecha_proceso]
    }
    df_lima = pd.DataFrame(datos_lima)
    # Requerirá tener instalado pyarrow o fastparquet
    df_lima.to_parquet(parquet_path, index=False)
    print(f"✅ Archivo Parquet creado: {parquet_path}")

    # ==========================================
    # FUENTE C: BUENOS AIRES (ventas_buenos_aires.json)
    # ==========================================
    json_path = os.path.join(inputs_dir, "ventas_buenos_aires.json")
    # Formato JSON Lines: Un objeto JSON por línea
    registros_ba = [
        {"tx_id": "BA-551", "sucursal": "Buenos Aires", "total": 12000, "timestamp": fecha_proceso},
        {"tx_id": "BA-552", "sucursal": "BsAs_Erronea", "total": -500, "timestamp": fecha_proceso},
        {"tx_id": "BA-553", "sucursal": "Buenos Aires", "total": "NULO_ERROR", "timestamp": fecha_proceso}
    ]
    
    with open(json_path, "w", encoding="utf-8") as f:
        for registro in registros_ba:
            f.write(json.dumps(registro) + "\n")
    print(f"✅ Archivo JSON Lines creado: {json_path}")

    # ==========================================
    # FUENTE D: TIPOS DE CAMBIO (tipo_cambio.csv)
    # ==========================================
    tc_path = os.path.join(inputs_dir, "tipo_cambio.csv")
    datos_tc = {
        "codigo_moneda": ["CLP", "PEN", "ARS"],
        "pais": ["Chile", "Peru", "Argentina"],
        "factor_usd": [0.0011, 0.27, 0.0010]
    }
    df_tc = pd.DataFrame(datos_tc)
    df_tc.to_csv(tc_path, index=False)
    print(f"✅ Archivo Maestro de Tipo de Cambio creado: {tc_path}")
    
    print("\n🚀 Generación de datos mock finalizada con éxito. Listos para el pipeline.")

if __name__ == "__main__":
    crear_entorno_datos()