from pathlib import Path
import sys
import datetime

def configurar_imports(log=True):
    """
    Detecta automáticamente la raíz del proyecto buscando carpetas clave.
    Agrega rutas para importación desde notebooks o scripts.
    """
    try:
        base = Path(__file__).resolve().parent
    except NameError:
        base = Path().resolve()  # fallback para notebooks

    # Subimos hasta encontrar una carpeta que contenga 'scripts' y 'notebooks'
    proyecto_root = base
    while not all((proyecto_root / carpeta).exists() for carpeta in ["scripts", "notebooks"]) and proyecto_root != proyecto_root.parent:
        proyecto_root = proyecto_root.parent

    if not all((proyecto_root / carpeta).exists() for carpeta in ["scripts", "notebooks"]):
        raise RuntimeError("No se pudo detectar la raíz del proyecto (carpetas 'scripts' y 'notebooks' no encontradas).")

    rutas = {
        "root": proyecto_root,
        "scripts": proyecto_root / "scripts",
        "data": proyecto_root / "data",
        "notebooks": proyecto_root / "notebooks",
    }

    activadas = []

    for nombre, ruta in rutas.items():
        ruta_str = str(ruta)
        if ruta_str not in sys.path:
            sys.path.append(ruta_str)
            activadas.append((nombre, ruta_str))
            print(f"✔️ Ruta '{nombre}' agregada: {ruta_str}")
        else:
            print(f"➜ Ruta '{nombre}' ya presente: {ruta_str}")

    if log:
        registrar_log(activadas)

    detectar_entorno()

def registrar_log(rutas_activadas):
    log_path = Path("logs/setup_path.log")
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
