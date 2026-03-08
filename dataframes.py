import pandas as pd
import pyreadstat

# ============================================================
# CARGA DE BASES
# ============================================================
print("Cargando bases de datos...")
df_persona,  meta_persona  = pyreadstat.read_sav("ddbb/CEN2023_PERSONA.sav")
print("  [1/3] PERSONA cargada")
df_hogar,    meta_hogar    = pyreadstat.read_sav("ddbb/CEN2023_HOGAR.sav")
print("  [2/3] HOGAR cargada")
df_vivienda, meta_vivienda = pyreadstat.read_sav("ddbb/CEN2023_VIVIENDA.sav")
print("  [3/3] VIVIENDA cargada")


# ============================================================
# JOIN PERSONA + VIVIENDA
# Se hace una sola vez y se reutiliza para todos los indicadores
# que necesiten variables de vivienda.
# LLAVEVIV es única por vivienda, por lo que el join no duplica filas.
# V01_TIPO filtra viviendas válidas (1,2,3,4), excluyendo viviendas
# colectivas e institucionales que no se cuentan en los indicadores.
# ============================================================
print("\nRealizando joins...")
df_persona = df_persona.merge(
    df_vivienda[["LLAVEVIV", "V01_TIPO", "V08_AGUA", "V09_INST"]],
    on="LLAVEVIV",
    how="left"
)
df_persona = df_persona[df_persona["V01_TIPO"].isin([1, 2, 3, 4])]
print("  [1/1] PERSONA + VIVIENDA")


# ============================================================
# DATAFRAMES
# ============================================================
print("\nGenerando dataframes...")
_n = 0
_total = 57

# Total de NNA (0 a 17 años) - base para todos los demás df
nna = df_persona[df_persona["P03_EDAD"].between(0, 17)]
_n += 1; print(f"  [{_n}/{_total}] nna")

# NNA varones
ninos = nna[nna["P02_SEXO"] == 1]  # 1=varón
_n += 1; print(f"  [{_n}/{_total}] ninos")

# NNA mujeres
ninas = nna[nna["P02_SEXO"] == 2]  # 2=mujer
_n += 1; print(f"  [{_n}/{_total}] ninas")

# IDs de hogares que tienen al menos un NNA (drop duplicates evita contar 2 veces un hogar que tiene 2 niños)
hogares_con_nna_ids = nna[["LLAVEVIV", "HOGAR"]].drop_duplicates()

# Total hogares con NNA
hogares_con_nna = df_hogar.merge(hogares_con_nna_ids, on=["LLAVEVIV", "HOGAR"], how="inner")
_n += 1; print(f"  [{_n}/{_total}] hogares_con_nna")

# NNA de 0 a 6 años
nna_0_6 = nna[nna["P03_EDAD"].between(0, 6)]
_n += 1; print(f"  [{_n}/{_total}] nna_0_6")

# NNA de 7 a 12 años
nna_7_12 = nna[nna["P03_EDAD"].between(7, 12)]
_n += 1; print(f"  [{_n}/{_total}] nna_7_12")

# NNA de 13 a 17 años
nna_13_17 = nna[nna["P03_EDAD"].between(13, 17)]
_n += 1; print(f"  [{_n}/{_total}] nna_13_17")


# masks para etnias (serie de true/false para cada fila, despues se filtra con esto)
mask_indigena      = (nna["P08_INDIG"] != 11) & (nna["P09_AFROD"] == 8)
mask_afro          = (nna["P08_INDIG"] == 11) & (nna["P09_AFROD"] != 8)
mask_afro_indigena = (nna["P08_INDIG"] != 11) & (nna["P09_AFROD"] != 8) & (nna["P08_INDIG"] != 99) & (nna["P09_AFROD"] != 9)

# NNA indígenas (no afro)
indigena = nna[mask_indigena]
_n += 1; print(f"  [{_n}/{_total}] indigena")

# NNA afrodescendientes (no indígenas)
afro = nna[mask_afro]
_n += 1; print(f"  [{_n}/{_total}] afro")

# NNA afrodescendientes e indígenas
afro_indigena = nna[mask_afro_indigena]
_n += 1; print(f"  [{_n}/{_total}] afro_indigena")

# NNA otras etnias (ni indígenas ni afro, incluye no declarados)
otras_etnias = nna[~(mask_indigena | mask_afro | mask_afro_indigena)]
_n += 1; print(f"  [{_n}/{_total}] otras_etnias")

# NNA que no asisten a educación preescolar (3 a 5 años)
no_asiste_preescolar = nna[
    (nna["P03_EDAD"].between(3, 5)) &
    (nna["P14_ESCU"] == 2)  # 2=No
]
_n += 1; print(f"  [{_n}/{_total}] no_asiste_preescolar")

# NNA que no asisten a educación escolar (6 a 17 años)
no_asiste_escolar = nna[
    (nna["P03_EDAD"].between(6, 17)) &
    (nna["P14_ESCU"] == 2)  # 2=No
]
_n += 1; print(f"  [{_n}/{_total}] no_asiste_escolar")

# NNA que no asisten por edad simple (6 a 17 años) - lo hice loop para no escribir 1 por 1
no_asiste_por_edad = {}
for edad in range(6, 18):
    no_asiste_por_edad[edad] = nna[
        (nna["P03_EDAD"] == edad) &
        (nna["P14_ESCU"] == 2)
    ]
    _n += 1; print(f"  [{_n}/{_total}] no_asiste_{edad}")

no_asiste_por_edad_sexo = {}
for edad in range(6, 18):
    for sexo, label in [(1, "niño"), (2, "niña")]:
        no_asiste_por_edad_sexo[(edad, sexo)] = nna[
            (nna["P03_EDAD"] == edad) &
            (nna["P14_ESCU"] == 2) &
            (nna["P02_SEXO"] == sexo)
            ]
        _n += 1;
        print(f"  [{_n}/{_total}] no_asiste_{edad}_{label}")


    ################################################
    ########ACA FALTA SOBREEDAD ESCOLAR#############
    ################################################

# Maternidad adolescente: mujeres de 10 a 14 años con hijos nacidos vivos
maternidad_10_14 = nna[
    (nna["P03_EDAD"].between(10, 14)) &
    (nna["P02_SEXO"] == 2) &
    (nna["P23_HIJOS"] > 0) &
    (nna["P23_HIJOS"] != 99)  # excluir no declarados
    ]
_n += 1; print(f"  [{_n}/{_total}] maternidad_10_14")

# Maternidad adolescente: mujeres de 15 a 17 años con hijos nacidos vivos
maternidad_15_19 = df_persona[
    (df_persona["P03_EDAD"].between(15, 19)) &
    (df_persona["P02_SEXO"] == 2) &
    (df_persona["P23_HIJOS"] > 0) &
    (df_persona["P23_HIJOS"] != 99)  # excluir no declarados
    ]
_n += 1; print(f"  [{_n}/{_total}] maternidad_15_19")

# Maternidad adolescente: mujeres de 15 a 17 años con hijos nacidos vivos
maternidad_15_17 = df_persona[
    (df_persona["P03_EDAD"].between(15, 17)) &
    (df_persona["P02_SEXO"] == 2) &
    (df_persona["P23_HIJOS"] > 0) &
    (df_persona["P23_HIJOS"] != 99)  # excluir no declarados
    ]
_n += 1; print(f"  [{_n}/{_total}] maternidad_15_17")

# Trabajo infantil: 10 a 14 años, trabaja y no estudia
trabajo_sin_estudio_10_14 = nna[
    (nna["P03_EDAD"].between(10, 14)) &
    ((nna["P17_TRAB"] == 1) | (nna["P17A_TRABAUS"]==1) | (nna["P17B_ALGTRA"]==1) | (nna["P17C_ALGFAM"]==1)) &    #si
    (nna["P14_ESCU"] == 2)      #no
]
_n += 1; print(f"  [{_n}/{_total}] trabajo_sin_estudio_10_14")

# Trabajo infantil: 10 a 14 años, trabaja y estudia
trabajo_con_estudio_10_14 = nna[
    (nna["P03_EDAD"].between(10, 14)) &
    ((nna["P17_TRAB"] == 1) | (nna["P17A_TRABAUS"]==1) | (nna["P17B_ALGTRA"]==1) | (nna["P17C_ALGFAM"]==1)) &    #si
    (nna["P14_ESCU"] == 1)      #si
]
_n += 1; print(f"  [{_n}/{_total}] trabajo_con_estudio_10_14")

# Trabajo infantil: 15 a 17 años, trabaja y no estudia
trabajo_sin_estudio_15_17 = nna[
    (nna["P03_EDAD"].between(15, 17)) &
    ((nna["P17_TRAB"] == 1) | (nna["P17A_TRABAUS"]==1) | (nna["P17B_ALGTRA"]==1) | (nna["P17C_ALGFAM"]==1)) &    #si
    (nna["P14_ESCU"] == 2)      #no
]
_n += 1; print(f"  [{_n}/{_total}] trabajo_sin_estudio_15_17")

# Trabajo infantil: 15 a 17 años, trabaja y estudia
trabajo_con_estudio_15_17 = nna[
    (nna["P03_EDAD"].between(15, 17)) &
    ((nna["P17_TRAB"] == 1) | (nna["P17A_TRABAUS"]==1) | (nna["P17B_ALGTRA"]==1) | (nna["P17C_ALGFAM"]==1)) &    #si
    (nna["P14_ESCU"] == 1)      #si
]
_n += 1; print(f"  [{_n}/{_total}] trabajo_con_estudio_15_17")

# NNA con acceso a agua segura
# (acueducto público/comunitario/particular con instalación interna) o (carro cisterna o agua embotellada)
agua_segura = nna[
    ((nna["V08_AGUA"].isin([1, 2, 3])) & (nna["V09_INST"] == 1)) |
    (nna["V08_AGUA"].isin([9, 10]))
]
_n += 1; print(f"  [{_n}/{_total}] agua_segura")

print("\nDataframes listos.")












