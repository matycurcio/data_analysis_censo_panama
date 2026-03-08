import pandas as pd


# ============================================================
# FUNCIÓN PRINCIPAL
# Agrupa cualquier df por nivel geográfico (provincia, distrito
# o corregimiento) y devuelve una tabla jerárquica con todos
# los niveles intercalados ordenados por código.
# ============================================================
def tabla_jerarquica(df, nombre="cantidad"):
    prov = (df.groupby(["PROVINCIA"])
              .size()
              .reset_index(name=nombre)
              .assign(nivel="provincia", codigo=lambda x: x["PROVINCIA"]))

    dist = (df.groupby(["PROVINCIA", "DISTRITO"])
              .size()
              .reset_index(name=nombre)
              .assign(nivel="distrito", codigo=lambda x: x["PROVINCIA"].astype(str) + x["DISTRITO"].astype(str)))

    corr = (df.groupby(["PROVINCIA", "DISTRITO", "CORREG"])
              .size()
              .reset_index(name=nombre)
              .assign(nivel="corregimiento", codigo=lambda x: x["PROVINCIA"].astype(str) + x["DISTRITO"].astype(str) + x["CORREG"].astype(str)))

    orden_nivel = {"provincia": 0, "distrito": 1, "corregimiento": 2}
    tabla = pd.concat([prov, dist, corr])
    tabla["orden_nivel"] = tabla["nivel"].map(orden_nivel)
    tabla = tabla.sort_values(["orden_nivel", "codigo"]).drop(columns="orden_nivel").reset_index(drop=True)
    tabla = tabla[["codigo", nombre, "nivel"]]
    return tabla