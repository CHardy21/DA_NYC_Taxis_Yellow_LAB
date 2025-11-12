import pandas as pd

def insertar_columna(df, nueva_col, despues_de):
    """Inserta una columna en el DataFrame justo después de otra"""
    cols = df.columns.tolist()
    idx = cols.index(despues_de)
    nueva_orden = cols[:idx+1] + [nueva_col] + cols[idx+1:]
    return df[nueva_orden]

def mapear_ratecode(df: pd.DataFrame) -> pd.DataFrame:
    if "RateCode" in df.columns:
        return df
    ratecode_map = {
        1: "Standard rate",
        2: "JFK",
        3: "Newark",
        4: "Nassau or Westchester",
        5: "Negotiated fare",
        6: "Group ride",
        99: "Unknown"
    }
    df["RateCode"] = df["RatecodeID"].map(ratecode_map).fillna("Unknown")
    return insertar_columna(df, "RateCode", "RatecodeID")

def mapear_payment_type(df: pd.DataFrame) -> pd.DataFrame:
    if "PaymentType" in df.columns:
        return df
    payment_map = {
        1: "Credit card",
        2: "Cash",
        3: "No charge",
        4: "Dispute",
        5: "Unknown",
        6: "Voided trip"
    }
    df["PaymentType"] = df["payment_type"].map(payment_map).fillna("Unknown")
    return insertar_columna(df, "PaymentType", "payment_type")

def mapear_store_and_fwd(df: pd.DataFrame) -> pd.DataFrame:
    flag_map = {
        "Y": "Stored and forwarded",
        "N": "Not stored",
        None: "Unknown"
    }
    df["StoreAndFwd"] = df["store_and_fwd_flag"].map(flag_map).fillna("Unknown")
    return insertar_columna(df, "StoreAndFwd", "store_and_fwd_flag")

def aplicar_transformaciones(df: pd.DataFrame) -> pd.DataFrame:
    df = mapear_ratecode(df)
    df = mapear_payment_type(df)
    df = mapear_store_and_fwd(df)
    return df

def eliminar_columnas_originales(df: pd.DataFrame) -> pd.DataFrame:
    columnas = ["RatecodeID", "payment_type", "store_and_fwd_flag"]
    return df.drop(columns=[col for col in columnas if col in df.columns])


def cli():
    import argparse
    from pathlib import Path

    parser = argparse.ArgumentParser(description="Transformaciones categóricas en dataset de taxis")
    parser.add_argument("--csv", type=str, required=True, help="Ruta al archivo CSV o Parquet")
    parser.add_argument("--out", type=str, default="data/processed/taxi_transformado.csv", help="Ruta de salida")
    args = parser.parse_args()

    path = Path(args.csv)
    if not path.exists():
        raise FileNotFoundError(f"❌ Archivo no encontrado: {path.resolve()}")

    df = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_csv(path)
    df_transformado = aplicar_transformaciones(df)
    df_transformado.to_csv(args.out, index=False)
    print(f"✅ Transformaciones aplicadas. Archivo guardado en: {args.out}")

if __name__ == "__main__":
    cli()
