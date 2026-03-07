import pandas as pd
import pyreadstat

# ============================================================
# CARGA DE BASES
# ============================================================
df_persona,  meta_persona  = pyreadstat.read_sav("personas.sav")
df_hogar,    meta_hogar    = pyreadstat.read_sav("hogar.sav")
df_vivienda, meta_vivienda = pyreadstat.read_sav("vivienda.sav")


# ============================================================
# JOIN PERSONA + VIVIENDA
# Se hace una sola vez y se reutiliza para todos los indicadores
# que necesiten variables de vivienda.
# LLAVEVIV es única por vivienda, por lo que el join no duplica filas.
# ============================================================
df_persona_viv = df_persona.merge(
    df_vivienda[["LLAVEVIV", "V08_AGUA"]],  # agregar más columnas de vivienda si se necesitan
    on="LLAVEVIV",
    how="left"
)


# ============================================================
# MASKS REUTILIZABLES
# Condiciones que se usan en múltiples dataframes.
# ============================================================

# Identidad étnica
mask_indigena      = (df_persona["P08_INDIG"] != 11) & (df_persona["P09_AFROD"] == 8)
mask_afro          = (df_persona["P08_INDIG"] == 11) & (df_persona["P09_AFROD"] != 8)
mask_afro_indigena = (df_persona["P08_INDIG"] != 11) & (df_persona["P09_AFROD"] != 8)


# ============================================================
# DATAFRAMES
# ============================================================

# Total de NNA (0 a 17 años)
nna = df_persona[df_persona["P03_EDAD"] <= 17]

# NNA varones
ninos = df_persona[
    (df_persona["P03_EDAD"] <= 17) &
    (df_persona["P02_SEXO"] == 1)  # 1=varón, confirmar con meta.variable_value_labels
]

# NNA mujeres
ninas = df_persona[
    (df_persona["P03_EDAD"] <= 17) &
    (df_persona["P02_SEXO"] == 2)  # 2=mujer, confirmar con meta.variable_value_labels
]

# NNA indígenas (no afro)
indigena = df_persona[
    (df_persona["P03_EDAD"] <= 17) &
    mask_indigena
]

# NNA afrodescendientes (no indígenas)
afro = df_persona[
    (df_persona["P03_EDAD"] <= 17) &
    mask_afro
]

# NNA afrodescendientes e indígenas
afro_indigena = df_persona[
    (df_persona["P03_EDAD"] <= 17) &
    mask_afro_indigena
]

# NNA otros (ni indígenas ni afro, incluye no declarados)
otros = df_persona[
    (df_persona["P03_EDAD"] <= 17) &
    ~(mask_indigena | mask_afro | mask_afro_indigena)
]

# NNA que no asisten a educación preescolar (3 a 5 años)
no_asiste_preescolar = df_persona[
    (df_persona["P03_EDAD"].between(3, 5)) &
    (df_persona["P14_ESCU"] == 2)  # 2=No
]

# NNA que no asisten a educación escolar (6 a 17 años)
no_asiste_escolar = df_persona[
    (df_persona["P03_EDAD"].between(6, 17)) &
    (df_persona["P14_ESCU"] == 2)  # 2=No
]

# NNA con discapacidad
discapacidad = df_persona[
    (df_persona["P03_EDAD"] <= 17) &
    (df_persona["P11_DISCA"] == 1)  # 1=Sí
]

# Maternidad adolescente: mujeres de 10 a 19 años con hijos nacidos vivos
maternidad_adolescente = df_persona[
    (df_persona["P03_EDAD"].between(10, 19)) &
    (df_persona["P02_SEXO"] == 2) &
    (df_persona["P23_HIJOS"] > 0) &
    (df_persona["P23_HIJOS"] != 99)  # excluir no declarados
]

# Trabajo infantil: 10 a 14 años, trabaja y no estudia
trabajo_sin_estudio_10_14 = df_persona[
    (df_persona["P03_EDAD"].between(10, 14)) &
    (df_persona["P17_TRAB"] == 1) &
    (df_persona["P14_ESCU"] == 2)
]

# Trabajo infantil: 10 a 14 años, trabaja y estudia
trabajo_con_estudio_10_14 = df_persona[
    (df_persona["P03_EDAD"].between(10, 14)) &
    (df_persona["P17_TRAB"] == 1) &
    (df_persona["P14_ESCU"] == 1)
]

# Trabajo infantil: 15 a 17 años, trabaja y no estudia
trabajo_sin_estudio_15_17 = df_persona[
    (df_persona["P03_EDAD"].between(15, 17)) &
    (df_persona["P17_TRAB"] == 1) &
    (df_persona["P14_ESCU"] == 2)
]

# Trabajo infantil: 15 a 17 años, trabaja y estudia
trabajo_con_estudio_15_17 = df_persona[
    (df_persona["P03_EDAD"].between(15, 17)) &
    (df_persona["P17_TRAB"] == 1) &
    (df_persona["P14_ESCU"] == 1)
]

# NNA en hogares con acceso a agua segura
# (acueducto público IDAAN, comunitario, particular, carro cisterna o agua embotellada)
agua_segura = df_persona_viv[
    (df_persona_viv["P03_EDAD"] <= 17) &
    (df_persona_viv["V08_AGUA"].isin([1, 2, 3, 9, 10]))
]

# Total de hogares (sin filtro, cada fila es un hogar)
total_hogares = df_hogar