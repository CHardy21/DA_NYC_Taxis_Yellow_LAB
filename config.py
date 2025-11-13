# config.py (en NYC_Taxi_Lab)
from pathlib import Path
import sys

def set_project_root():
    """
    Detecta automáticamente la raíz del proyecto usando este archivo como referencia.
    Agrega la carpeta raíz al sys.path.
    """
    root = Path(__file__).resolve().parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))
        print(f"✔️ sys.path configurado con raíz del proyecto: {root}")
    else:
        print(f"✔️ sys.path ya contiene la raíz: {root}")
