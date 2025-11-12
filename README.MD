# 🗽 Análisis de Datos: NYC Yellow Taxi Trips

Este laboratorio explora el dataset público de viajes en taxi amarillo de la ciudad de Nueva York. Se prioriza la trazabilidad, limpieza  y documentación para facilitar el onboarding y la auditoría técnica.

---

## 📦 Dataset

- **Fuente:** NYC Taxi & Limousine Commission (TLC)
- **URL:** https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
- **Formato:** CSV
- **Tamaño estimado:** ~1–2 GB por mes
- **Año analizado:** 2025 (mes de enero)
- **Link del DF:** https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet

---

## 🧾 Diccionario de columnas

| Columna                   | Descripción |
|--------                   |-------------|
| `VendorID`                | ID del proveedor de tecnología (1 o 2) |
| `tpep_pickup_datetime`    | Fecha y hora de inicio del viaje |
| `tpep_dropoff_datetime`   | Fecha y hora de fin del viaje |
| `passenger_count`         | Número de pasajeros |
| `trip_distance`           | Distancia del viaje (millas) |
| `RatecodeID`              | Tipo de tarifa (ver tabla abajo) |
| `store_and_fwd_flag`      | ¿Se almacenó el viaje antes de enviarse? (Y/N) |
| `PULocationID`            | Zona de recogida (ID TLC) |
| `DOLocationID`            | Zona de destino (ID TLC) |
| `payment_type`            | Método de pago (ver tabla abajo) |
| `fare_amount`             | Tarifa base |
| `extra`                   | Cargos adicionales (hora pico, etc.) |
| `mta_tax`                 | Impuesto de la MTA |
| `tip_amount`              | Propina |
| `tolls_amount`            | Peajes |
| `improvement_surcharge`   | Recargo de mejora (0.30 USD) |
| `total_amount`            | Total cobrado |
| `congestion_surcharge`    | Recargo por congestión (si aplica) |

### `RatecodeID` (tipo de tarifa)

| Código | Descripción |
|--------|-------------|
| 1 | Tarifa estándar |
| 2 | JFK |
| 3 | Newark |
| 4 | Nassau o Westchester |
| 5 | Tarifa negociada |
| 6 | Viaje compartido |

### `payment_type` (forma de pago)

| Código | Descripción |
|--------|-------------|
| 1 | Crédito |
| 2 | Efectivo |
| 3 | Sin cargo |
| 4 | Disputa |
| 5 | Otro |
| 6 | Débito |

---

## 🗺️ Mapeo de zonas

Las columnas `PULocationID` y `DOLocationID` representan zonas geográficas codificadas. Se pueden traducir usando el archivo `taxi_zone_lookup.csv`, que incluye:

- `LocationID`
- `Borough` (ej. Manhattan, Brooklyn)
- `Zone` (ej. Upper East Side, JFK Airport)
- `service_zone` (ej. Yellow Zone, Boro Zone)


## ⚙️ Reproducibilidad

- `environment.yml`: entorno Conda exportado
- `scripts/`: limpieza, EDA, modelado
- `notebooks/`: exploración visual y validación
- `README.md`: documentación principal

---

## 📌 Objetivos posibles

- Analizar demanda por zona y hora
- Visualizar patrones de propinas y métodos de pago
- Analisis Geográfico

---

## 🔗 Abrir en Colab

| Archivos | Link |
|------|-----|
| **ETL_NYC_Taxis.ipynb**| [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/CHardy21/DA_NYC_Taxis_Yellow_LAB/blob/main/notebooks/ETL_NYC_Taxis.ipynb)

| **EDA_NYC_Taxis.ipynb**| [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/CHardy21/DA_NYC_Taxis_Yellow_LAB/blob/main/notebooks/EDA_NYC_Taxis.ipynb)
 |






## 🧠 Créditos y fuentes

- [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- [Microsoft Open Datasets](https://learn.microsoft.com/en-us/azure/open-datasets/dataset-taxi-yellow)
