from pathlib import Path
import sys
import datetime

def preparar_entorno(log=True):
    """
    Configura el entorno de ejecución detectando la raíz del proyecto (usando config.py como ancla).
    Agrega rutas clave al sys.path para permitir importaciones limpias desde cualquier subnivel.
    Su uso es para ejecuciones desde la consola.
    """
    base = Path(__file__).resolve().parent

    # Subimos hasta encontrar config.py
    proyecto_root = base
    while not (proyecto_root / "config.py").exists() and proyecto_root != proyecto_root.parent:
        proyecto_root = proyecto_root.parent

    if not (proyecto_root / "config.py").exists():
        raise RuntimeError("No se encontró config.py en la jerarquía de carpetas.")

    rutas = {
        "root": proyecto_root,
        "scripts": proyecto_root / "scripts",
        "data": proyecto_root / "data",
        "notebooks": proyecto_root / "notebooks",
    }

    activadas = []

    for nombre, ruta in rutas.items():
        ruta_str = str(ruta)
        if ruta.exists() and ruta_str not in sys.path:
            sys.path.append(ruta_str)
            activadas.append((nombre, ruta_str))
            print(f"✔️ Ruta '{nombre}' agregada: {ruta_str}")
        elif ruta.exists():
            print(f"➜ Ruta '{nombre}' ya presente: {ruta_str}")
        else:
            print(f"⚠️ Ruta '{nombre}' no existe: {ruta_str}")

    if log:
        registrar_log(activadas)

    detectar_entorno()

def registrar_log(rutas_activadas):
    log_path = Path("logs/setup_env.log")
    log_path.parent.mkdir(exist_ok=True)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    with log_path.open("a", encoding="utf-8") as f:
        f.write(f"\n[{timestamp}] Rutas activadas:\n")
        for nombre, ruta in rutas_activadas:
            f.write(f" - {nombre}: {ruta}\n")

def detectar_entorno():
    try:
        shell = get_ipython().__class__.__name__
        if "ZMQInteractiveShell" in shell:
            print("➜ Entorno Jupyter detectado.")
        else:
            print("⚠️ No parece ser un entorno Jupyter.")
    except NameError:
        print("⚠️ No se detectó entorno Jupyter (probablemente ejecución desde consola).")
