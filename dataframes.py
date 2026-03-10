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
    df_vivienda[["LLAVEVIV", "V01_TIPO", "V07_CUAR","V08_AGUA", "V09_INST"]],
    on="LLAVEVIV",
    how="left"
)
df_persona = df_persona[df_persona["V01_TIPO"].isin([1, 2, 3, 4])]
print("  [1/2] PERSONA + VIVIENDA")

df_hogar = df_hogar.merge(
    df_vivienda[["LLAVEVIV", "V01_TIPO"]],
    on="LLAVEVIV",
    how="left"
)
df_hogar = df_hogar[df_hogar["V01_TIPO"].isin([1, 2, 3, 4])]

print("  [2/2] HOGAR + VIVIENDA")

# ============================================================
# DATAFRAMES
# ============================================================
print("\nGenerando dataframes...")
_n = 0
_total = 113 ##esto esta hardcodeado, es solo para los logs

# Total de NNA (0 a 17 años) - base para todos los demás df
nna = df_persona[df_persona["P03_EDAD"].between(0, 17)]
_n += 1; print(f"  [{_n}/{_total}] nna")

# NNA varones
ninos = nna[nna["P02_SEXO"] == 1]  # 1=varón
_n += 1; print(f"  [{_n}/{_total}] ninos")

# NNA mujeres
ninas = nna[nna["P02_SEXO"] == 2]  # 2=mujer
_n += 1; print(f"  [{_n}/{_total}] ninas")

nna_por_rango_sexo = {}
for rango, label_edad in [((0, 4), "0_4"), ((5, 11), "5_11"), ((12, 17), "12_17")]:
    for sexo, label_sexo in [(1, "niño"), (2, "niña")]:
        key = f"{label_edad}_{label_sexo}"
        nna_por_rango_sexo[key] = nna[
            (nna["P03_EDAD"].between(*rango)) &
            (nna["P02_SEXO"] == sexo)
        ]
        _n += 1; print(f"  [{_n}/{_total}] nna_{key}")

# IDs de hogares que tienen al menos un NNA (drop duplicates evita contar 2 veces un hogar que tiene 2 niños)
hogares_con_nna_ids = nna[["LLAVEVIV", "HOGAR"]].drop_duplicates()

# Total hogares con NNA
hogares_con_nna = df_hogar.merge(hogares_con_nna_ids, on=["LLAVEVIV", "HOGAR"], how="inner")
_n += 1; print(f"  [{_n}/{_total}] hogares_con_nna")


#### migración jefes
""" #COLUMNAS Q AL FINAL NO VAN
# Identificar jefes de hogar

jefes = df_persona[df_persona["P01_RELA"] == 1][["LLAVEVIV", "HOGAR", "P07_VIVIA2"]]

df_hogar_con_migracion = df_hogar.merge(jefes, on=["LLAVEVIV", "HOGAR"], how="left")

hogares_jefe = {}
for valor, label in [(3, "mig_internacional"), (2, "mig_nacional"), (1, "no_migrante"), (9, "nsnc")]:
    hogares_jefe[label] = df_hogar_con_migracion[
        (df_hogar_con_migracion["P07_VIVIA2"] == valor)
    ].merge(hogares_con_nna_ids, on=["LLAVEVIV", "HOGAR"], how="inner")
    _n += 1; print(f"  [{_n}/{_total}] hogares_jefe_{label}")

#(df_hogar_con_migracion.merge(hogares_con_nna_ids, on=["LLAVEVIV", "HOGAR"], how="inner")["P07_VIVIA2"].value_counts().sort_index())
"""
# NNA con jefes migrantes
jefes = df_persona[df_persona["P01_RELA"] == 1][["LLAVEVIV", "HOGAR", "P07_VIVIA2"]].rename(columns={"P07_VIVIA2": "P07_VIVIA2_JEFE"})

nna_con_migracion = nna.merge(jefes, on=["LLAVEVIV", "HOGAR"], how="left")

nna_jefe = {}
nna_jefe["mig_internacional"] = nna_con_migracion[nna_con_migracion["P07_VIVIA2_JEFE"] == 3]
_n += 1; print(f"  [{_n}/{_total}] nna_jefe_mig_internacional")

nna_jefe["mig_nacional"] = nna_con_migracion[nna_con_migracion["P07_VIVIA2_JEFE"] == 2]
_n += 1; print(f"  [{_n}/{_total}] nna_jefe_mig_nacional")

nna_jefe["no_mig"] = nna_con_migracion[nna_con_migracion["P07_VIVIA2_JEFE"].isin([1, 9]) | nna_con_migracion["P07_VIVIA2_JEFE"].isna()]
_n += 1; print(f"  [{_n}/{_total}] nna_jefe_no_mig")

###############################################################################################
#NNA con etnias jefes
jefes_etnia = df_persona[df_persona["P01_RELA"] == 1][["LLAVEVIV", "HOGAR", "P08_INDIG", "P09_AFROD"]].rename(columns={
    "P08_INDIG": "P08_INDIG_JEFE",
    "P09_AFROD": "P09_AFROD_JEFE"
})

nna_con_etnia = nna.merge(jefes_etnia, on=["LLAVEVIV", "HOGAR"], how="left")

mask_indigena_jefe      = (nna_con_etnia["P08_INDIG_JEFE"] != 11) & (nna_con_etnia["P09_AFROD_JEFE"] == 8)
mask_afro_jefe          = (nna_con_etnia["P08_INDIG_JEFE"] == 11) & (nna_con_etnia["P09_AFROD_JEFE"] != 8)
mask_afro_indigena_jefe = (nna_con_etnia["P08_INDIG_JEFE"] != 11) & (nna_con_etnia["P09_AFROD_JEFE"] != 8) & (nna_con_etnia["P08_INDIG_JEFE"] != 99) & (nna_con_etnia["P09_AFROD_JEFE"] != 9)

indigena      = nna_con_etnia[mask_indigena_jefe]
_n += 1; print(f"  [{_n}/{_total}] indigena")

afro          = nna_con_etnia[mask_afro_jefe]
_n += 1; print(f"  [{_n}/{_total}] afro")

afro_indigena = nna_con_etnia[mask_afro_indigena_jefe]
_n += 1; print(f"  [{_n}/{_total}] afro_indigena")

otras_etnias  = nna_con_etnia[~(mask_indigena_jefe | mask_afro_jefe | mask_afro_indigena_jefe)]
_n += 1; print(f"  [{_n}/{_total}] otras_etnias")

#asistencia NNA por edad/sexo
asistencia_por_edad_sexo = {}
for edad in range(4, 18):
    for asiste, label_asiste in [(1, "dentro"), (2, "fuera")]:
        for sexo, label_sexo in [(2, "nina"), (1, "nino")]:
            key = f"{edad}_{label_sexo}_{label_asiste}"
            if asiste == 1:
                filtro_asiste = nna["P14_ESCU"] == 1
            else:
                filtro_asiste = nna["P14_ESCU"].isin([2, 9])  # fuera + nsnc
            asistencia_por_edad_sexo[key] = nna[
                (nna["P03_EDAD"] == edad) &
                (nna["P02_SEXO"] == sexo) &
                filtro_asiste
            ]
            _n += 1; print(f"  [{_n}/{_total}] asistencia_{key}")


# Sobreedad: NNA (hasta 19) cuyo grado corresponde a una edad mayor a la esperada
sobreedad_map = {
    11: 8,  12: 9,  13: 10, 14: 11, 15: 12, 16: 13,
    31: 14, 32: 15, 33: 16,
    21: 14, 22: 15, 23: 16,
    34: 17, 35: 18, 36: 19
}

nna_8_19 = df_persona[df_persona["P03_EDAD"].between(8, 19)]

mask_sobreedad = pd.Series(False, index=nna_8_19.index)
for grado, edad_min in sobreedad_map.items():
    mask_sobreedad |= (nna_8_19["P15_GRADO"] == grado) & (nna_8_19["P03_EDAD"] == edad_min)

sobreedad     = nna_8_19[mask_sobreedad]
sin_sobreedad = nna_8_19[~mask_sobreedad]
_n += 1; print(f"  [{_n}/{_total}] sobreedad")
_n += 1; print(f"  [{_n}/{_total}] sin_sobreedad")

################################################################
# Maternidad adolescente

# Base de mujeres por rango
mujeres_10_14 = nna[(nna["P03_EDAD"].between(10, 14)) & (nna["P02_SEXO"] == 2)]
mujeres_15_19 = df_persona[(df_persona["P03_EDAD"].between(15, 19)) & (df_persona["P02_SEXO"] == 2)]
mujeres_15_17 = df_persona[(df_persona["P03_EDAD"].between(15, 17)) & (df_persona["P02_SEXO"] == 2)]

# Maternidad 10-14
maternidad_10_14     = mujeres_10_14[(mujeres_10_14["P23_HIJOS"] > 0) & (mujeres_10_14["P23_HIJOS"] != 99)]
no_maternidad_10_14  = mujeres_10_14[~((mujeres_10_14["P23_HIJOS"] > 0) & (mujeres_10_14["P23_HIJOS"] != 99))]
_n += 1; print(f"  [{_n}/{_total}] maternidad_10_14")
_n += 1; print(f"  [{_n}/{_total}] no_maternidad_10_14")

# Maternidad 15-19
maternidad_15_19     = mujeres_15_19[(mujeres_15_19["P23_HIJOS"] > 0) & (mujeres_15_19["P23_HIJOS"] != 99)]
no_maternidad_15_19  = mujeres_15_19[~((mujeres_15_19["P23_HIJOS"] > 0) & (mujeres_15_19["P23_HIJOS"] != 99))]
_n += 1; print(f"  [{_n}/{_total}] maternidad_15_19")
_n += 1; print(f"  [{_n}/{_total}] no_maternidad_15_19")

# Maternidad 15-17
maternidad_15_17     = mujeres_15_17[(mujeres_15_17["P23_HIJOS"] > 0) & (mujeres_15_17["P23_HIJOS"] != 99)]
no_maternidad_15_17  = mujeres_15_17[~((mujeres_15_17["P23_HIJOS"] > 0) & (mujeres_15_17["P23_HIJOS"] != 99))]
_n += 1; print(f"  [{_n}/{_total}] maternidad_15_17")
_n += 1; print(f"  [{_n}/{_total}] no_maternidad_15_17")

# Mask de trabajo (cualquiera de las 4 variables)
mask_trabaja = (
    (nna["P17_TRAB"] == 1) | (nna["P17A_TRABAUS"] == 1) |
    (nna["P17B_ALGTRA"] == 1) | (nna["P17C_ALGFAM"] == 1)
)

trabajo = {}
for rango, label_edad in [((10, 14), "10_14"), ((15, 17), "15_17")]:
    for trabaja, estudia, label in [
        (True,  2, "trabaja_sin_estudiar"),
        (True,  1, "trabaja_y_estudia"),
        (False, 2, "no_trabaja_ni_estudia"),
        (False, 1, "solo_estudia"),
    ]:
        for sexo, label_sexo in [(2, "Niñas"), (1, "Niños")]:
            key = f"{label_edad}_{label_sexo}_{label}"
            titulo = f"8. Trabajo infantil. #{label_sexo} {rango[0]}-{rango[1]} que {label.replace('_', ' ')}"
            mask_trabajo = mask_trabaja if trabaja else ~mask_trabaja
            trabajo[key] = (
                nna[
                    (nna["P03_EDAD"].between(*rango)) &
                    (nna["P02_SEXO"] == sexo) &
                    mask_trabajo &
                    (nna["P14_ESCU"] == estudia)
                ],
                titulo
            )
            _n += 1; print(f"  [{_n}/{_total}] trabajo_{key}")

# NNA con y sin acceso a agua segura
# (acueducto público/comunitario/particular con instalación interna) o (carro cisterna o agua embotellada)
agua_segura = nna[
    ((nna["V08_AGUA"].isin([1, 2, 3])) & (nna["V09_INST"] == 1)) |
    (nna["V08_AGUA"].isin([9, 10]))
]
_n += 1; print(f"  [{_n}/{_total}] agua_segura")

agua_no_segura = nna[
    ~(
        ((nna["V08_AGUA"].isin([1, 2, 3])) & (nna["V09_INST"] == 1)) |
        (nna["V08_AGUA"].isin([9, 10]))
    )
]
_n += 1; print(f"  [{_n}/{_total}] agua_no_segura")

# Hacinamiento: personas en vivienda / cuartos > 3
# H_PERSONAS viene de df_hogar, pero necesitamos personas por vivienda
personas_por_viv = df_hogar.groupby("LLAVEVIV")["H_PERSONAS"].sum().reset_index(name="PERSONAS_VIV")

hacinamiento_viv = df_vivienda[["LLAVEVIV", "V07_CUAR"]].merge(personas_por_viv, on="LLAVEVIV", how="left")
hacinamiento_viv["hacinado"] = hacinamiento_viv["PERSONAS_VIV"] / hacinamiento_viv["V07_CUAR"] > 3

nna_hacinamiento = nna.merge(hacinamiento_viv[["LLAVEVIV", "hacinado"]], on="LLAVEVIV", how="left")

hacinado     = nna_hacinamiento[nna_hacinamiento["hacinado"] == True]
no_hacinado  = nna_hacinamiento[nna_hacinamiento["hacinado"] == False]
_n += 1; print(f"  [{_n}/{_total}] hacinado")
_n += 1; print(f"  [{_n}/{_total}] no_hacinado")


#ESTADO ACTIVIDAD JEFE
jefes_actividad = df_persona[df_persona["P01_RELA"] == 1][["LLAVEVIV", "HOGAR", "PEA_NEA"]].rename(columns={"PEA_NEA": "PEA_NEA_JEFE"})

nna_con_actividad = nna.merge(jefes_actividad, on=["LLAVEVIV", "HOGAR"], how="left")

nna_jefe_ocupado    = nna_con_actividad[nna_con_actividad["PEA_NEA_JEFE"] == 1]
nna_jefe_desocupado = nna_con_actividad[nna_con_actividad["PEA_NEA_JEFE"] == 2]
nna_jefe_inactivo   = nna_con_actividad[nna_con_actividad["PEA_NEA_JEFE"] == 3]
nna_jefe_nsnc       = nna_con_actividad[nna_con_actividad["PEA_NEA_JEFE"] == 9]
_n += 1; print(f"  [{_n}/{_total}] nna_jefe_ocupado")
_n += 1; print(f"  [{_n}/{_total}] nna_jefe_desocupado")
_n += 1; print(f"  [{_n}/{_total}] nna_jefe_inactivo")
_n += 1; print(f"  [{_n}/{_total}] nna_jefe_nsnc")

#nna con seguro social
con_seguro = nna[nna["P10_SEGSOC"].isin([1, 2, 3, 4, 5])]
sin_seguro = nna[nna["P10_SEGSOC"].isin([6, 9])]
_n += 1; print(f"  [{_n}/{_total}] con_seguro")
_n += 1; print(f"  [{_n}/{_total}] sin_seguro")

#nna que viven en hogar donde reciben transferencias sociales
mask_trans = (
    (df_persona["P225A_REDOPOR"] == 1) |
    (df_persona["P225B_120A65"] == 2) |
    (df_persona["P225C_ANGELG"] == 3)
)

hogares_con_trans = df_persona[mask_trans][["LLAVEVIV", "HOGAR"]].drop_duplicates()

con_transferencias = nna.merge(hogares_con_trans, on=["LLAVEVIV", "HOGAR"], how="inner")
sin_transferencias = nna[~nna.set_index(["LLAVEVIV", "HOGAR"]).index.isin(
    hogares_con_trans.set_index(["LLAVEVIV", "HOGAR"]).index
)]
_n += 1; print(f"  [{_n}/{_total}] con_transferencias")
_n += 1; print(f"  [{_n}/{_total}] sin_transferencias")

# nna que viven en Hogares donde al menos un miembro recibe beca
hogares_con_beca = df_persona[
    df_persona["P224_BECA"].notna() & (df_persona["P224_BECA"] > 0)
][["LLAVEVIV", "HOGAR"]].drop_duplicates()

con_beca = nna.merge(hogares_con_beca, on=["LLAVEVIV", "HOGAR"], how="inner")
sin_beca = nna[~nna.set_index(["LLAVEVIV", "HOGAR"]).index.isin(
    hogares_con_beca.set_index(["LLAVEVIV", "HOGAR"]).index
)]
_n += 1; print(f"  [{_n}/{_total}] con_beca")
_n += 1; print(f"  [{_n}/{_total}] sin_beca")

#nna que viven en hogares con Internet
internet_hogar = df_hogar[["LLAVEVIV", "HOGAR", "H18L_INTER"]]

nna_con_internet_data = nna.merge(internet_hogar, on=["LLAVEVIV", "HOGAR"], how="left")

con_internet = nna_con_internet_data[nna_con_internet_data["H18L_INTER"] == 1]
sin_internet = nna_con_internet_data[nna_con_internet_data["H18L_INTER"] == 2]
_n += 1; print(f"  [{_n}/{_total}] con_internet")
_n += 1; print(f"  [{_n}/{_total}] sin_internet")

print("\nDataframes listos.")












