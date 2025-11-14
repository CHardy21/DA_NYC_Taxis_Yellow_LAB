import pandas as pd
import os
import math
import gc
import argparse

def split_by_rows(df, dataset_name, max_mb=100, output_dir="splits", base_name="part", fmt="csv"):
    total_bytes = df.memory_usage(deep=True).sum()
    total_mb = total_bytes / (1024**2)
    rows_per_chunk = math.floor(len(df) * (max_mb / total_mb))

    os.makedirs(output_dir, exist_ok=True)
    num_chunks = math.ceil(len(df) / rows_per_chunk)

    for i in range(num_chunks):
        start = i * rows_per_chunk
        end = (i + 1) * rows_per_chunk
        chunk = df.iloc[start:end]

        filename = os.path.join(output_dir, f"{dataset_name}_{base_name}_rows_{i+1}.{fmt}")
        if fmt == "csv":
            chunk.to_csv(filename, index=False)
        elif fmt == "parquet":
            chunk.to_parquet(filename, index=False)
        print(f"[rows] Parte {i+1}/{num_chunks} creada correctamente: {os.path.relpath(filename)} ({len(chunk)} filas)")


def split_dynamic(df, dataset_name, max_mb=100, output_dir="splits", base_name="part", fmt="csv"):
    os.makedirs(output_dir, exist_ok=True)
    start = 0
    part = 1
    while start < len(df):
        end = start + 10000
        while end <= len(df):
            chunk = df.iloc[start:end]
            filename = os.path.join(output_dir, f"{dataset_name}_{base_name}_dyn_{part}.{fmt}")
            if fmt == "csv":
                chunk.to_csv(filename, index=False)
            elif fmt == "parquet":
                chunk.to_parquet(filename, index=False)

            size_mb = os.path.getsize(filename) / (1024**2)
            if size_mb > max_mb:
                end = end - 1000 if end - 1000 > start else start + 1
                chunk = df.iloc[start:end]
                filename = os.path.join(output_dir, f"{dataset_name}_{base_name}_dyn_{part}.{fmt}")
                if fmt == "csv":
                    chunk.to_csv(filename, index=False)
                elif fmt == "parquet":
                    chunk.to_parquet(filename, index=False)
                size_mb = os.path.getsize(filename) / (1024**2)
                print(f"[dyn] Guardado {os.path.relpath(filename)} con {len(chunk)} filas ({size_mb:.2f} MB)")
                start = end
                part += 1
                break
            else:
                end += 10000
        else:
            chunk = df.iloc[start:]
            filename = os.path.join(output_dir, f"{dataset_name}_{base_name}_dyn_{part}.{fmt}")
            if fmt == "csv":
                chunk.to_csv(filename, index=False)
            elif fmt == "parquet":
                chunk.to_parquet(filename, index=False)
            size_mb = os.path.getsize(filename) / (1024**2)
            print(f"[dyn] Guardado {os.path.relpath(filename)} con {len(chunk)} filas ({size_mb:.2f} MB)")
            break


def run_split(input_path, mode="rows", max_mb=100, output_dir="splits", base_name="part", fmt="csv"):
    root = os.getcwd()
    output_dir = os.path.join(root, output_dir)
    dataset_name = os.path.splitext(os.path.basename(input_path))[0]  # nombre del dataset
    try:
        print("🔍 Buscando dataset...")
        if input_path.endswith(".csv"):
            df = pd.read_csv(input_path)
        elif input_path.endswith(".parquet"):
            df = pd.read_parquet(input_path, engine="pyarrow")
        else:
            print("❌ Formato no soportado. Usa CSV o Parquet.")
            return
        print(f"✔️ Dataset encontrado con {len(df)} filas.")
        print("🚀 Iniciando particionamiento...")

        if mode == "rows":
            total_bytes = df.memory_usage(deep=True).sum()
            total_mb = total_bytes / (1024**2)
            rows_per_chunk = math.floor(len(df) * (max_mb / total_mb))
            num_chunks = math.ceil(len(df) / rows_per_chunk)
            print(f"Se generarán {num_chunks} partes aproximadamente.")
            split_by_rows(df, dataset_name, max_mb=max_mb, output_dir=output_dir, base_name=base_name, fmt=fmt)
        elif mode == "dynamic":
            print("Modo dinámico: número de partes dependerá del tamaño real.")
            split_dynamic(df, dataset_name, max_mb=max_mb, output_dir=output_dir, base_name=base_name, fmt=fmt)
        else:
            print(f"❌ Modo {mode} no reconocido. Usa 'rows' o 'dynamic'.")
            return

        print("✔️ El dataframe fue particionado correctamente.")
    except Exception as e:
        print(f"❌ Error durante el particionamiento: {e}")
    finally:
        del df
        gc.collect()
        print("🧹 Memoria liberada.")


if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="Dividir datasets grandes en partes")
    parser.add_argument("--input", required=True, help="Ruta del archivo CSV o Parquet")
    parser.add_argument("--mode", choices=["rows", "dynamic"], default="rows", help="Modo de particionado")
    parser.add_argument("--max-mb", type=int, default=100, help="Tamaño máximo por archivo en MB")
    parser.add_argument("--output-dir", default="splits", help="Directorio de salida")
    parser.add_argument("--base-name", default="part", help="Prefijo de los archivos")
    parser.add_argument("--fmt", choices=["csv", "parquet"], default="csv", help="Formato de salida")

    args = parser.parse_args()
    try:
        run_split(
            input_path=args.input,
            mode=args.mode,
            max_mb=args.max_mb,
            output_dir=args.output_dir,
            base_name=args.base_name,
            fmt=args.fmt
        )
    except:
        print("python splitter.py --input [your_dataset.parquet] --mode [rows or dynamic] --max-mb [100]")
