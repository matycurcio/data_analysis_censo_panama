import pandas as pd


# ============================================================
# FUNCIÓN PRINCIPAL
# Agrupa cualquier df por nivel geográfico (provincia, distrito
# o corregimiento) y devuelve una tabla jerárquica con todos
# los niveles intercalados ordenados por código.
# ============================================================
def tabla_jerarquica(df):
    prov = (df.groupby(["PROV"])
              .size()
              .reset_index(name="cantidad")
              .assign(nivel="provincia", codigo=lambda x: x["PROV"]))

    dist = (df.groupby(["PROV", "DIST"])
              .size()
              .reset_index(name="cantidad")
              .assign(nivel="distrito", codigo=lambda x: x["PROV"].astype(str) + x["DIST"].astype(str)))

    corr = (df.groupby(["PROV", "DIST", "CORRE"])
              .size()
              .reset_index(name="cantidad")
              .assign(nivel="corregimiento", codigo=lambda x: x["PROV"].astype(str) + x["DIST"].astype(str) + x["CORRE"].astype(str)))

    tabla = pd.concat([prov, dist, corr]).sort_values("codigo").reset_index(drop=True)
    return tabla