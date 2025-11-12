
# ¿Qué hace este script?
# Traduce los códigos de zona a nombres legibles.
# Enriquecer el dataset con columnas PU_Zone, DO_Zone, PU_Borough, DO_Borough.
# Genera un resumen visual de las zonas más frecuentes de origen.
# Deja todo listo para análisis por barrio, mapas o segmentación.


import pandas as pd
import os
ruta_archivo = os.path.join("../data", "raw", "yellow_tripdata_2025-01.parquet")
ruta_archivo2 = os.path.join("../data", "raw", "taxi_zone_lookup.csv")
try:
    # Validación de existencia
    if not os.path.exists(ruta_archivo):
        raise FileNotFoundError
    if not os.path.exists(ruta_archivo2):
        raise FileNotFoundError

    # Carga con motor explícito
    df = pd.read_parquet(ruta_archivo, engine="pyarrow")
    # Cargar tabla de zonas
    zonas = pd.read_csv(ruta_archivo2)

except FileNotFoundError:
    print("❌ Error: Ruta incorrecta o archivo no encontrado")
except Exception as e:
    print(f"⚠️ Error inesperado: {e}")

# Cargar tabla de zonas

#zonas = pd.read_csv(ruta_archivo)

# Renombrar columnas para claridad
zonas.columns = ["LocationID", "Borough", "Zone", "ServiceZone"]

# Mapear zonas de origen y destino
df = df.merge(zonas, how="left", left_on="PULocationID", right_on="LocationID")
df = df.rename(columns={"Borough": "PU_Borough", "Zone": "PU_Zone"})
df = df.drop(columns=["LocationID", "ServiceZone"])

df = df.merge(zonas, how="left", left_on="DOLocationID", right_on="LocationID")
df = df.rename(columns={"Borough": "DO_Borough", "Zone": "DO_Zone"})
df = df.drop(columns=["LocationID", "ServiceZone"])

# Guardar dataset enriquecido
#df.to_csv("../data/yellow_tripdata_enriquecido.csv", index=False)

# Resumen visual por zona de origen
resumen = df["PU_Zone"].value_counts().head(10)
print("🗺️ Top 10 zonas de origen:")
print(resumen)
