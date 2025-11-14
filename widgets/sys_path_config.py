from pathlib import Path
import sys, os

def pretty_path(path):
    rel = os.path.relpath(path, os.getcwd())
    return rel if rel != "." else str(path)

def configure_sys_path():
    """
    Detecta la raíz del proyecto buscando config.py y ajusta sys.path.
    Devuelve la ruta detectada o None si no se encontró.
    """
    base = Path().resolve()
    while not (base / "config.py").exists() and base != base.parent:
        base = base.parent

    if not (base / "config.py").exists():
        print(f"❌ No se encontró config.py en la jerarquía de carpetas desde: {Path().resolve()}")
        print("➜ sys.path no fue modificado.")
        return None
    else:
        if str(base) not in sys.path:
            sys.path.insert(0, str(base))
            print(f"✔️ sys.path configurado con raíz del proyecto: [ {os.path.relpath(base)} ] ")
        else:
            print(f"✔️ sys.path ya estaba configurado con raíz del proyecto: [ {os.path.relpath(base)} ]")

        project_root = os.path.normcase(str(base))
        jupyterRoot = os.path.normcase(os.getcwd())
        print(f"✔️ Root detectado: [ {os.path.relpath(project_root)} ]")
        if jupyterRoot == project_root:
            print(f"✔️ Jupyter arrancó en el root del proyecto.")
        return project_root

# --- Ejecución automática al importar ---
#_project_root = configure_sys_path()
