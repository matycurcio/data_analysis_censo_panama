import pandas as pd
from main_function import tabla_jerarquica
from dataframes import *

# ============================================================
# GENERACIÓN DE TABLAS
# Cada tupla es (dataframe, nombre de columna en el resultado)
# ============================================================
indicadores = [
    (df_persona, "1.#TotalPersonasenHogPart"),
    (nna, "1.#TotalNNAenHogPart"),
    (ninos, "1.NNASexo #Hombres"),
    (ninas, "1.NNASexo #Mujeres"),
    *[(nna_por_rango_sexo[k], f"1.nna_{k}") for k in nna_por_rango_sexo],
    (df_hogar, "1.#TotalHogaresPart"),
    (hogares_con_nna, "1. #TotalHogaresconNNA"),
    (nna_jefe["mig_internacional"], "NNA con jefe migrante internacional"),
    (nna_jefe["mig_nacional"], "NNA con jefe migrante nacional"),
    (nna_jefe["no_mig"], "NNA con jefe no migrante"),
    (indigena, "3.Etnia#Indígena"),
    (afro, "3.Etnia#Afro"),
    (afro_indigena, "3.Etnia#Afro e Indígena"),
    (otras_etnias, "3.Etnia#Otras (ni afro ni indígena)"),
    *[(asistencia_por_edad_sexo[k], f"4y5. Asistencia escolar. #{('Niñas' if 'nina' in k else 'Niños')} {k.split('_')[0]} años {'dentro' if 'dentro' in k else 'fuera'} del sistema educativo") for k in asistencia_por_edad_sexo],
    (sobreedad, "6. Sobreedad. #NNA 8-19años con rezago escolar"),
    (sin_sobreedad, "6. Sobreedad. #NNA 8-19años sin rezago escolar"),
    (maternidad_10_14, "7. Maternidad adolescente #niñas 10-14años madres"),
    (no_maternidad_10_14, "7. Maternidad adolescente #niñas 10-14años NO madres"),
    (maternidad_15_19, "7. Maternidad adolescente #niñas 15-19años madres"),
    (no_maternidad_15_19, "7. Maternidad adolescente #niñas 15-19años NO madres"),
    (maternidad_15_17, "7. Maternidad adolescente #niñas 15-17años madres"),
    (no_maternidad_15_17, "7. Maternidad adolescente #niñas 15-17años NO madres"),
    *[(df, titulo) for df, titulo in trabajo.values()],
    (agua_segura, "9. Agua segura. #NNA con acceso a agua segura"),
    (agua_no_segura, "9. Agua segura. #NNA sin acceso a agua segura"),
    (hacinado,    "10. Hacinamiento: Cantidad de NNA que habitan en condiciones de hacinamiento"),
    (no_hacinado, "10. Hacinamiento: Cantidad de NNA que NO habitan en condiciones de hacinamiento"),
    (nna_jefe_ocupado,    "11. Condición de actividad del jefe. # de NNA que viven en hogares con jefe de hogar ocupado"),
    (nna_jefe_desocupado, "11. Condición de actividad del jefe. # de NNA que viven en hogares con jefe de hogar desocupado"),
    (nna_jefe_inactivo,   "11. Condición de actividad del jefe. # de NNA que viven en hogares con jefe de hogar inactivo"),
    (nna_jefe_nsnc,       "11. Condición de actividad del jefe. # de NNA que viven en hogares con jefe de hogar NS/NC"),
    (con_seguro, "12. Acceso a seguro social: # NNA CON acceso al seguro social"),
    (sin_seguro, "12. Acceso a seguro social: # NNA SIN acceso al seguro social"),
    (con_transferencias, "13. Acceso a transferencias sociales: # NNA CON acceso TS"),
    (sin_transferencias, "13. Acceso a transferencias sociales: # NNA SIN acceso TS"),
    (con_beca, "14. Acceso a becas: # NNA CON acceso a becas"),
    (sin_beca, "14. Acceso a becas: # NNA SIN acceso a becas"),
    (con_internet, "15. Acceso a internet. #NNA CON acceso a internet"),
    (sin_internet, "15. Acceso a internet. #NNA SIN acceso a internet")
]

for df, nombre in indicadores:
    if "PROVINCIA" not in df.columns:
        print(f"Falta PROVINCIA en: {nombre}")
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