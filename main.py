import pandas as pd
from main_function import tabla_jerarquica
from dataframes import *

# ============================================================
# GENERACIÓN DE TABLAS
# Cada tupla es (dataframe, nombre de columna en el resultado)
# ============================================================
indicadores = [
    nna, "0.#TotalNNA"
]

# ============================================================
# MERGE DE TODAS LAS TABLAS
# Se parte de la primera tabla y se van uniendo las demás
# por codigo y nivel, que son las columnas geográficas comunes.
# ============================================================
tablas = [tabla_jerarquica(df, nombre) for df, nombre in indicadores]

resultado = tablas[0]
for t in tablas[1:]:
    resultado = resultado.merge(t[["codigo", "nivel", t.columns[-1]]], on=["codigo", "nivel"], how="left")

# ============================================================
# EXPORT
# ============================================================
resultado.to_excel("resultado.xlsx", index=False)
print("Listo. Archivo generado: resultado.xlsx")