from setuptools import setup, find_packages
# Ejecute desde consola con su entonrno activo.
# pip install -e .

setup(
    name="CHardy Extensions for DA",          
    version="0.1.0",                 
    packages=find_packages(include=["scripts", "widgets", "scripts.*", "widgets.*"]),
    install_requires=[
        "ipywidgets",
        "notebook"
    ],
    description="Paquete modular con widgets y scripts para análisis de datos",
    author="Christian Hardy",
    python_requires=">=3.8",
)
