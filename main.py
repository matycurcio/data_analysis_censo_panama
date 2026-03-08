import pandas as pd
from main_function import tabla_jerarquica
from dataframes import *

# ============================================================
# GENERACIÓN DE TABLAS
# Cada tupla es (dataframe, nombre de columna en el resultado)
# ============================================================
indicadores = [
    (nna, "0.#TotalNNA"),
    (hogares_con_nna, "0. #TotalHogaresconNNA"),
    (ninos, "1.#Niños"),
    (ninas, "2.#Niñas"),
    (nna_0_6, "2.Edad#0-6"),
    (nna_7_12,"2.Edad#7-12"),
    (nna_13_17,"2.Edad#13-17"),
    (indigena, "3.Etnia#Indígena"),
    (afro, "3.Etnia#Afro"),
    (afro_indigena, "3.Etnia#Afro e Indígena"),
    (otras_etnias, "3.Etnia#Otras (ni afro ni indígena)")
]

# ============================================================
# MERGE DE TODAS LAS TABLAS
# Se parte de la primera tabla y se van uniendo las demás
# por codigo y nivel, que son las columnas geográficas comunes.
# ============================================================
tablas = [tabla_jerarquica(df, nombre) for df, nombre in indicadores]

resultado = tablas[0]
for t in tablas[1:]:
    cols = [c for c in t.columns if c not in ["nivel", "orden_nivel"]]
    resultado = resultado.merge(t[cols], on="codigo", how="left")

# Mover columna nivel al final y eliminarla
cols = [c for c in resultado.columns if c != "nivel"]
resultado = resultado[cols]

# ============================================================
# EXPORT
# ============================================================
resultado.to_excel("resultado.xlsx", index=False)
print("Listo. Archivo generado: resultado.xlsx")