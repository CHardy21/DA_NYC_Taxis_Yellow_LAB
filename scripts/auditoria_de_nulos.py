import pandas as pd
from pathlib import Path

def auditar_nulos_df(df: pd.DataFrame) -> pd.DataFrame:
    """Audita valores nulos en un DataFrame ya cargado"""
    nulos = df.isnull().sum()
    total = len(df)
    porcentaje = (nulos / total * 100).round(2)

    resumen = pd.DataFrame({
        "Nulos": nulos,
        "Porcentaje": porcentaje,
        "Tipo": df.dtypes
    })
    resumen_filtrado = resumen[resumen["Nulos"] > 0].sort_values(by="Nulos", ascending=False)
    return resumen_filtrado

def sugerencias(resumen: pd.DataFrame):
    """Sugiere acciones según tipo de dato"""
    print("\n🧠 Sugerencias:")
    for col in resumen.index:
        tipo = resumen.loc[col, "Tipo"]
        if tipo == "object":
            print(f"→ '{col}': imputar con 'Unknown' o modo")
        elif "float" in str(tipo) or "int" in str(tipo):
            print(f"→ '{col}': imputar con mediana o eliminar si son pocos")
        elif "datetime" in str(tipo):
            print(f"→ '{col}': revisar si se puede eliminar la fila")
        else:
            print(f"→ '{col}': revisar manualmente")

def cli():
    """Bloque CLI para ejecución desde consola"""
    import argparse
    parser = argparse.ArgumentParser(description="Auditoría de nulos en CSV")
    parser.add_argument("--csv", type=str, required=True, help="Ruta al archivo CSV")
    args = parser.parse_args()

    path = Path(args.csv)
    if not path.exists():
        raise FileNotFoundError(f"❌ No se encontró el archivo: {path.resolve()}")

    df = pd.read_csv(path)
    resumen = auditar_nulos_df(df)

    print("🔍 Auditoría de valores nulos:")
    print(resumen)

    if resumen.empty:
        print("\n✅ No hay valores nulos. El dataset está limpio.")
    else:
        sugerencias(resumen)
        print("\n⚠️ Hay columnas con nulos. Considerá imputar o eliminar antes de exportar.")

if __name__ == "__main__":
    cli()
