import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd

# ============================================================
# CONFIGURACIÓN DE NIVELES
# ============================================================
NIVEL_CONFIG = {
    "provincia":     {"archivo": "geo/pan_provincias.json",         "codigo_geo": "PROV_ID"},
    "distrito":      {"archivo": "geo/pan_districts_final.geojson", "codigo_geo": "ID_DIST"},
    "corregimiento": {"archivo": "geo/pan_corregs_final.geojson",   "codigo_geo": "ID_CORR"},
}

# ============================================================
# FUNCIÓN
# ============================================================
def dibujar_mapa(df_resultado, columna, nivel, codigo=None, cmap="Blues", etiquetas=True):
    """
    df_resultado -> df grande con todos los indicadores
    columna      -> nombre del indicador (ej. "0.#TotalNNA")
    nivel        -> "provincia", "distrito" o "corregimiento"
    codigo       -> codigo especifico a dibujar (ej. "01" para Bocas del Toro)
                    si es None, dibuja todos
    cmap         -> paleta de colores (default: Blues)
    """
    config = NIVEL_CONFIG[nivel]
    gdf = gpd.read_file(config["archivo"])

    datos = df_resultado[df_resultado["nivel"] == nivel][["codigo", "nombre", columna]].copy()
    gdf[config["codigo_geo"]] = gdf[config["codigo_geo"]].astype(str)
    datos["codigo"] = datos["codigo"].astype(str)

    gdf = gdf.merge(datos, left_on=config["codigo_geo"], right_on="codigo", how="left")

    if codigo:
        gdf = gdf[gdf[config["codigo_geo"]].str.startswith(codigo)]

    # Calcular porcentajes
    total = gdf[columna].sum()
    gdf["pct"] = gdf[columna] / total * 100

    vmin = gdf["pct"].min()
    vmax = gdf["pct"].max()
    norm = plt.Normalize(vmin=vmin, vmax=vmax)
    cmap_obj = plt.get_cmap(cmap)

    fig, ax = plt.subplots(figsize=(15, 15))
    fig.patch.set_facecolor("white")
    gdf.plot(column="pct", cmap=cmap_obj, legend=False, norm=norm, ax=ax, edgecolor="black")
    ax.set_axis_off()

    sm = plt.cm.ScalarMappable(cmap=cmap_obj, norm=norm)
    sm.set_array([])
    cbar = fig.colorbar(sm, ax=ax, shrink=0.5)
    default_ticks = cbar.get_ticks()
    ticks = [vmin] + [t for t in default_ticks if vmin <= t <= vmax] + [vmax]
    tick_labels = [f"{vmin:.1f}%"] + [f"{t:.1f}%" for t in default_ticks if vmin <= t <= vmax] + [f"{vmax:.1f}%"]
    cbar.set_ticks(ticks)
    cbar.set_ticklabels(tick_labels)

    if etiquetas:
        for idx, row in gdf.iterrows():
            centroid = row.geometry.centroid
            offset_y = -10000 if row["codigo"] == "0805" else 0
            pct = row["pct"] if pd.notna(row["pct"]) else 0
            ax.annotate(
                f"{row['nombre']}\n{pct:.1f}%",
                xy=(centroid.x, centroid.y + offset_y),
                ha="center", fontsize=6,
                fontweight="bold"
            )

    plt.show()


# ============================================================
# CARGA DEL RESULTADO Y LLAMADAS
# ============================================================
resultado = pd.read_excel("resultado.xlsx", dtype={"codigo": str})

dibujar_mapa(resultado, "9. Agua segura. #NNA sin acceso a agua segura", "corregimiento", "0301", etiquetas=False)