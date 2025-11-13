#!/usr/bin/env python3
import pandas as pd
import math
import os
import argparse

def split_by_rows(df, max_mb=100, output_dir="splits", base_name="part", fmt="csv"):
    """
    Divide un DataFrame en chunks aproximados según cálculo de filas.
    """
    total_bytes = df.memory_usage(deep=True).sum()
    total_mb = total_bytes / (1024**2)
    rows_per_chunk = math.floor(len(df) * (max_mb / total_mb))

    os.makedirs(output_dir, exist_ok=True)
    num_chunks = math.ceil(len(df) / rows_per_chunk)

    for i in range(num_chunks):
        start = i * rows_per_chunk
        end = (i + 1) * rows_per_chunk
        chunk = df.iloc[start:end]

        filename = os.path.join(output_dir, f"{base_name}_rows_{i+1}.{fmt}")
        if fmt == "csv":
            chunk.to_csv(filename, index=False)
        elif fmt == "parquet":
            chunk.to_parquet(filename, index=False)
        print(f"[rows] Guardado {filename} con {len(chunk)} filas")


def split_dynamic(df, max_mb=100, output_dir="splits", base_name="part", fmt="csv"):
    """
    Divide un DataFrame midiendo el tamaño real de cada archivo exportado.
    Ajusta dinámicamente hasta que cada archivo quede ≤ max_mb.
    """
    os.makedirs(output_dir, exist_ok=True)
    start = 0
    part = 1
    while start < len(df):
        end = start + 10000  # tamaño inicial de prueba
        while end <= len(df):
            chunk = df.iloc[start:end]
            filename = os.path.join(output_dir, f"{base_name}_dyn_{part}.{fmt}")
            if fmt == "csv":
                chunk.to_csv(filename, index=False)
            elif fmt == "parquet":
                chunk.to_parquet(filename, index=False)

            size_mb = os.path.getsize(filename) / (1024**2)
            if size_mb > max_mb:
                # retroceder y guardar el chunk anterior
                end = end - 1000 if end - 1000 > start else start + 1
                chunk = df.iloc[start:end]
                if fmt == "csv":
                    chunk.to_csv(filename, index=False)
                elif fmt == "parquet":
                    chunk.to_parquet(filename, index=False)
                size_mb = os.path.getsize(filename) / (1024**2)
                print(f"[dyn] Guardado {filename} con {len(chunk)} filas ({size_mb:.2f} MB)")
                start = end
                part += 1
                break
            else:
                end += 10000
        else:
            # último bloque
            chunk = df.iloc[start:]
            filename = os.path.join(output_dir, f"{base_name}_dyn_{part}.{fmt}")
            if fmt == "csv":
                chunk.to_csv(filename, index=False)
            elif fmt == "parquet":
                chunk.to_parquet(filename, index=False)
            size_mb = os.path.getsize(filename) / (1024**2)
            print(f"[dyn] Guardado {filename} con {len(chunk)} filas ({size_mb:.2f} MB)")
            break


def main():
    parser = argparse.ArgumentParser(description="Dividir DataFrame en partes según tamaño máximo en MB")
    parser.add_argument("--input", required=True, help="Ruta del archivo CSV/Parquet de entrada")
    parser.add_argument("--max-mb", type=int, default=100, help="Tamaño máximo por archivo en MB")
    parser.add_argument("--output-dir", default="splits", help="Directorio de salida")
    parser.add_argument("--base-name", default="part", help="Prefijo de nombre de archivo")
    parser.add_argument("--format", choices=["csv", "parquet"], default="csv", help="Formato de salida")
    parser.add_argument("--mode", choices=["rows", "dynamic"], default="rows", help="Modo de partición")

    args = parser.parse_args()

    # Cargar dataset
    if args.input.endswith(".csv"):
        df = pd.read_csv(args.input)
    elif args.input.endswith(".parquet"):
        df = pd.read_parquet(args.input)
    else:
        raise ValueError("Formato de entrada no soportado. Usa CSV o Parquet.")

    if args.mode == "rows":
        split_by_rows(df, args.max_mb, args.output_dir, args.base_name, args.format)
    else:
        split_dynamic(df, args.max_mb, args.output_dir, args.base_name, args.format)


if __name__ == "__main__":
    # Opción 1: partición aproximada por filas
    # python split_df.py --input dataset.csv --max-mb 100 --mode rows --format parquet

    # Opción 2: partición dinámica midiendo tamaño real
    # python split_df.py --input dataset.csv --max-mb 100 --mode dynamic --format csv

    main()
