import os
import random
import json
import pandas as pd
from datetime import datetime, timedelta

def solicitar_parametros_sede(nombre_sede, admite_texto_en_monto=False):
    print(f"\n--- Configuración para la sede de {nombre_sede} ---")
    
    # 1. Cantidad de registros
    while True:
        try:
            total = int(input(f"👉 Cantidad de registros a generar para {nombre_sede}: "))
            if total >= 0:
                break
            print("❌ Ingrese un número mayor o igual a cero.")
        except ValueError:
            print("❌ Entrada inválida. Debe ser un número entero.")
            
    # 2. Porcentaje de error
    porcentaje = 0.0
    if total > 0:
        while True:
            try:
                porcentaje = float(input(f"👉 Porcentaje de datos erróneos para {nombre_sede} (0 a 100): "))
                if 0 <= porcentaje <= 100:
                    break
                print("❌ El porcentaje debe estar entre 0 y 100.")
            except ValueError:
                print("❌ Entrada inválida. Debe ser un número.")
                
    return total, porcentaje / 100.0

def generar_fecha_aleatoria():
    fecha_base = datetime(2026, 6, 10)
    dias_restar = random.randint(0, 30)
    return (fecha_base - timedelta(days=dias_restar)).strftime("%Y-%m-%d")

# =====================================================================
# GENERADORES ESPECÍFICOS POR SEDE (Garantizan la salud de los formatos)
# =====================================================================

def generar_santiago(id_num, es_erroneo):
    ciudades_erroneas = ["Santiago_Err", "Stgo_Centro", "Valparaiso"]
    ciudad = "Santiago"
    monto = random.randint(5000, 150000)
    
    if es_erroneo:
        tipo_falla = random.choice(["ciudad_invalida", "monto_negativo"])
        if tipo_falla == "ciudad_invalida":
            ciudad = random.choice(ciudades_erroneas)
        elif tipo_falla == "monto_negativo":
            monto = random.randint(-50000, -500)
            
    return {
        "id_transaccion": f"STGO-{id_num:06d}",
        "ciudad": ciudad,
        "monto": monto,
        "fecha": generar_fecha_aleatoria()
    }

def generar_lima(id_num, es_erroneo):
    ciudades_erroneas = ["Lim_Mal", "Callao", "Arequipa"]
    ciudad = "Lima"
    # Parquet mantendrá float/double de manera segura sin romper PyArrow
    monto = round(random.uniform(10.0, 500.0), 2)
    
    if es_erroneo:
        tipo_falla = random.choice(["ciudad_invalida", "monto_negativo"])
        if tipo_falla == "ciudad_invalida":
            ciudad = random.choice(ciudades_erroneas)
        elif tipo_falla == "monto_negativo":
            monto = round(random.uniform(-150.0, -1.0), 2)
            
    return {
        "transaction_id": f"LIMA-{id_num:06d}",
        "city": ciudad,
        "local_value": monto,
        "sales_date": generar_fecha_aleatoria()
    }

def generar_buenos_aires(id_num, es_erroneo):
    ciudades_erroneas = ["BsAs_Erronea", "Baires_Mal", "Cordoba"]
    ciudad = "Buenos Aires"
    monto = random.randint(1000, 90000)
    
    if es_erroneo:
        # JSON permite meter strings libremente simulando corrupción de la API de origen
        tipo_falla = random.choice(["ciudad_invalida", "monto_negativo", "monto_texto"])
        if tipo_falla == "ciudad_invalida":
            ciudad = random.choice(ciudades_erroneas)
        elif tipo_falla == "monto_negativo":
            monto = random.randint(-20000, -100)
        elif tipo_falla == "monto_texto":
            monto = random.choice(["TIMEOUT_REF", "NULO_ERROR", "NaN", "CRITICAL_ERR"])
            
    return {
        "tx_id": f"BA-{id_num:06d}",
        "sucursal": ciudad,
        "total": monto,
        "timestamp": generar_fecha_aleatoria()
    }

# =====================================================================
# ORQUESTACIÓN PRINCIPAL
# =====================================================================

def main():
    print("====================================================")
    print("⚙️ CONFIGURACIÓN GRANULAR DE DATA MOCK - RETAIL LATAM")
    print("====================================================")
    
    cant_stgo, pct_stgo = solicitar_parametros_sede("Santiago (CSV)")
    cant_lima, pct_lima = solicitar_parametros_sede("Lima (Parquet)")
    cant_ba, pct_ba = solicitar_parametros_sede("Buenos Aires (JSON Lines)")
    
    inputs_dir = os.path.join("data", "inputs")
    os.makedirs(inputs_dir, exist_ok=True)
    
    print("\n🧠 Generando datasets customizados...")

    # 1. Procesar Santiago
    if cant_stgo > 0:
        rows = [generar_santiago(i, random.random() < pct_stgo) for i in range(1, cant_stgo + 1)]
        pd.DataFrame(rows).to_csv(os.path.join(inputs_dir, "ventas_santiago.csv"), index=False)
    
    # 2. Procesar Lima (Protegido contra fallas de tipos de PyArrow)
    if cant_lima > 0:
        rows = [generar_lima(i, random.random() < pct_lima) for i in range(1, cant_lima + 1)]
        pd.DataFrame(rows).to_parquet(os.path.join(inputs_dir, "ventas_lima.parquet"), index=False)
        
    # 3. Procesar Buenos Aires (Con capacidad de inyectar strings ruidosos)
    if cant_ba > 0:
        json_path = os.path.join(inputs_dir, "ventas_buenos_aires.json")
        with open(json_path, "w", encoding="utf-8") as f:
            for i in range(1, cant_ba + 1):
                registro = generar_buenos_aires(i, random.random() < pct_ba)
                f.write(json.dumps(registro) + "\n")

    # 4. Generar tabla de tipo de cambio estática
    pd.DataFrame({
        "codigo_moneda": ["CLP", "PEN", "ARS"],
        "pais": ["Chile", "Peru", "Argentina"],
        "factor_usd": [0.0011, 0.27, 0.0010]
    }).to_csv(os.path.join(inputs_dir, "tipo_cambio.csv"), index=False)

    print("\n🚀 ¡Simulación finalizada con éxito sin conflictos de tipos!")
    print(f"📁 Destino: {inputs_dir}/")
    print(f"📊 Resumen: Santiago ({cant_stgo} recs) | Lima ({cant_lima} recs) | Buenos Aires ({cant_ba} recs)")

if __name__ == "__main__":
    main()