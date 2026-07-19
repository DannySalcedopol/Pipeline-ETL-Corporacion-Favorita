"""
05_eda_profundo.py
Tarea 5 del DAG: eda_profundo

Ejecuta el análisis estadístico completo sobre el DataFrame consolidado,
respondiendo a las preguntas de la sección 6.2 del enunciado:

- Ventas generales (por familia, por tienda, por ciudad/provincia, evolución
  temporal)
- Estacionalidad y feriados
- Promociones
- Petróleo y economía (incluye lag temporal 2015-2016)
- Transacciones (ticket promedio)

No incluye modelos predictivos ni forecasting: solo descripción y
correlación, tal como exige el enunciado.

Cada resultado se guarda como tabla separada en Parquet (REPORTS_DIR /
eda_tables/) para que la tarea 6 las escriba en PostgreSQL y Power BI las
consuma directamente, y además se resume todo en un JSON
(EDA_PROFUNDO_REPORT) para la documentación del README.
"""

import sys
import json

import polars as pl

from config import CONSOLIDATED_PARQUET, REPORTS_DIR, EDA_PROFUNDO_REPORT, get_logger

logger = get_logger("eda_profundo")

EDA_TABLES_DIR = REPORTS_DIR / "eda_tables"


def guardar_tabla(df: pl.DataFrame, nombre: str) -> None:
    EDA_TABLES_DIR.mkdir(parents=True, exist_ok=True)
    ruta = EDA_TABLES_DIR / f"{nombre}.parquet"
    df.write_parquet(ruta)
    logger.info(f"Tabla '{nombre}' guardada en {ruta} ({df.height} filas)")


def main():
    logger.info(f"Cargando consolidado desde {CONSOLIDATED_PARQUET} ...")
    df = pl.read_parquet(CONSOLIDATED_PARQUET)
    resumen = {}

    # -----------------------------------------------------------------
    # VENTAS GENERALES
    # -----------------------------------------------------------------

    # Distribución de ventas por familia de producto
    ventas_por_familia = (
        df.group_by("family")
        .agg(pl.col("sales").sum().alias("ventas_totales"))
        .sort("ventas_totales", descending=True)
    )
    guardar_tabla(ventas_por_familia, "ventas_por_familia")
    resumen["familia_mayor_volumen"] = ventas_por_familia.row(0, named=True)

    # Ventas totales por tienda + ranking top/bottom 10
    ventas_por_tienda = (
        df.group_by("store_nbr")
        .agg(pl.col("sales").sum().alias("ventas_totales"))
        .sort("ventas_totales", descending=True)
    )
    guardar_tabla(ventas_por_tienda, "ventas_por_tienda")
    resumen["top_10_tiendas"] = ventas_por_tienda.head(10).to_dicts()
    resumen["bottom_10_tiendas"] = ventas_por_tienda.tail(10).to_dicts()

    # Ventas promedio por ciudad y provincia
    ventas_por_ciudad = (
        df.group_by("city")
        .agg(pl.col("sales").mean().alias("ventas_promedio"))
        .sort("ventas_promedio", descending=True)
    )
    guardar_tabla(ventas_por_ciudad, "ventas_promedio_por_ciudad")

    ventas_por_provincia = (
        df.group_by("state")
        .agg(pl.col("sales").mean().alias("ventas_promedio"))
        .sort("ventas_promedio", descending=True)
    )
    guardar_tabla(ventas_por_provincia, "ventas_promedio_por_provincia")

    # Evolución temporal: tendencia mensual y anual 2013-2017
    df = df.with_columns([
        pl.col("date").dt.year().alias("anio"),
        pl.col("date").dt.month().alias("mes"),
    ])

    ventas_mensuales = (
        df.group_by(["anio", "mes"])
        .agg(pl.col("sales").sum().alias("ventas_totales"))
        .sort(["anio", "mes"])
    )
    guardar_tabla(ventas_mensuales, "evolucion_ventas_mensual")

    ventas_anuales = (
        df.group_by("anio")
        .agg(pl.col("sales").sum().alias("ventas_totales"))
        .sort("anio")
    )
    guardar_tabla(ventas_anuales, "evolucion_ventas_anual")

    # -----------------------------------------------------------------
    # ESTACIONALIDAD Y FERIADOS
    # -----------------------------------------------------------------

    ventas_feriado_vs_normal = (
        df.group_by("es_feriado")
        .agg([
            pl.col("sales").mean().alias("ventas_promedio"),
            pl.col("sales").sum().alias("ventas_totales"),
            pl.len().alias("num_registros"),
        ])
    )
    guardar_tabla(ventas_feriado_vs_normal, "ventas_feriado_vs_normal")
    resumen["impacto_feriados"] = ventas_feriado_vs_normal.to_dicts()

    # Ventas en +/-3 días alrededor de feriados nacionales, por familia
    fechas_feriado_nacional = (
        df.filter(pl.col("es_feriado"))
        .select("date")
        .unique()
        .to_series()
        .to_list()
    )

    ventanas = []
    for fecha in fechas_feriado_nacional:
        for offset in range(-3, 4):
            ventanas.append((fecha, offset))

    ventanas_df = pl.DataFrame(
        {
            "date_feriado": [v[0] for v in ventanas],
            "offset_dias": [v[1] for v in ventanas],
        }
    ).with_columns(
        (pl.col("date_feriado") + pl.duration(days=pl.col("offset_dias"))).alias("date")
    )

    ventas_alrededor_feriados = (
        ventanas_df.join(df.select(["date", "family", "sales"]), on="date", how="inner")
        .group_by(["offset_dias", "family"])
        .agg(pl.col("sales").mean().alias("ventas_promedio"))
        .sort(["family", "offset_dias"])
    )
    guardar_tabla(ventas_alrededor_feriados, "ventas_alrededor_feriados_por_familia")

    # Sensibilidad de familias a feriados: diferencia % vs promedio normal
    promedio_normal_familia = (
        df.filter(~pl.col("es_feriado"))
        .group_by("family")
        .agg(pl.col("sales").mean().alias("ventas_promedio_normal"))
    )
    promedio_feriado_familia = (
        df.filter(pl.col("es_feriado"))
        .group_by("family")
        .agg(pl.col("sales").mean().alias("ventas_promedio_feriado"))
    )
    sensibilidad_feriados = (
        promedio_normal_familia.join(promedio_feriado_familia, on="family", how="inner")
        .with_columns(
            (
                100
                * (pl.col("ventas_promedio_feriado") - pl.col("ventas_promedio_normal"))
                / pl.col("ventas_promedio_normal")
            ).alias("variacion_porcentual")
        )
        .sort("variacion_porcentual", descending=True)
    )
    guardar_tabla(sensibilidad_feriados, "sensibilidad_familias_feriados")
    resumen["familias_mas_sensibles_a_feriados"] = sensibilidad_feriados.head(5).to_dicts()

    # -----------------------------------------------------------------
    # PROMOCIONES
    # -----------------------------------------------------------------

    df = df.with_columns((pl.col("onpromotion") > 0).alias("tiene_promocion"))

    ventas_con_sin_promo = (
        df.group_by(["family", "tiene_promocion"])
        .agg(pl.col("sales").mean().alias("ventas_promedio"))
        .sort(["family", "tiene_promocion"])
    )
    guardar_tabla(ventas_con_sin_promo, "ventas_con_sin_promocion_por_familia")

    pivote_promo = ventas_con_sin_promo.pivot(
        index="family", on="tiene_promocion", values="ventas_promedio"
    )
    # Tras el pivot las columnas booleanas quedan como "true"/"false" (str)
    cols = pivote_promo.columns
    col_true = next((c for c in cols if c.lower() == "true"), None)
    col_false = next((c for c in cols if c.lower() == "false"), None)
    if col_true and col_false:
        pivote_promo = pivote_promo.with_columns(
            (
                100 * (pl.col(col_true) - pl.col(col_false)) / pl.col(col_false)
            ).alias("efecto_promocion_pct")
        ).sort("efecto_promocion_pct", descending=True)
    guardar_tabla(pivote_promo, "efecto_promocion_por_familia")
    resumen["familias_con_mayor_efecto_promocion"] = pivote_promo.head(5).to_dicts()

    # -----------------------------------------------------------------
    # PETRÓLEO Y ECONOMÍA
    # -----------------------------------------------------------------

    ventas_mensuales_petroleo = (
        df.group_by(["anio", "mes"])
        .agg([
            pl.col("sales").sum().alias("ventas_totales"),
            pl.col("dcoilwtico").mean().alias("precio_petroleo_promedio"),
        ])
        .sort(["anio", "mes"])
        .drop_nulls()
    )
    guardar_tabla(ventas_mensuales_petroleo, "ventas_vs_petroleo_mensual")

    correlacion_petroleo = ventas_mensuales_petroleo.select(
        pl.corr("ventas_totales", "precio_petroleo_promedio").alias("correlacion")
    ).item()
    resumen["correlacion_petroleo_ventas"] = correlacion_petroleo

    # Lag temporal 2015-2016: correlación de ventas con petróleo desplazado
    # 1, 2 y 3 meses (sirve para detectar si la caída de ventas ocurre con
    # retraso respecto a la caída del petróleo).
    periodo_caida = ventas_mensuales_petroleo.filter(
        (pl.col("anio") >= 2015) & (pl.col("anio") <= 2016)
    ).sort(["anio", "mes"])

    lags_resultado = {}
    serie_petroleo = periodo_caida["precio_petroleo_promedio"].to_list()
    serie_ventas = periodo_caida["ventas_totales"].to_list()
    for lag in (1, 2, 3):
        if len(serie_ventas) > lag:
            ventas_desplazadas = serie_ventas[lag:]
            petroleo_base = serie_petroleo[: len(ventas_desplazadas)]
            if len(petroleo_base) > 1:
                corr_lag = pl.DataFrame(
                    {"v": ventas_desplazadas, "p": petroleo_base}
                ).select(pl.corr("v", "p")).item()
                lags_resultado[f"lag_{lag}_meses"] = corr_lag
    resumen["correlacion_lag_petroleo_2015_2016"] = lags_resultado

    # Ciudades más sensibles a la caída del petróleo (2015-2016 vs resto)
    ventas_ciudad_periodo = (
        df.with_columns(
            pl.when((pl.col("anio") >= 2015) & (pl.col("anio") <= 2016))
            .then(pl.lit("caida_petroleo"))
            .otherwise(pl.lit("otro_periodo"))
            .alias("periodo")
        )
        .group_by(["city", "periodo"])
        .agg(pl.col("sales").mean().alias("ventas_promedio"))
    )
    guardar_tabla(ventas_ciudad_periodo, "ventas_ciudad_periodo_petroleo")

    pivote_ciudad = ventas_ciudad_periodo.pivot(
        index="city", on="periodo", values="ventas_promedio"
    )
    if "caida_petroleo" in pivote_ciudad.columns and "otro_periodo" in pivote_ciudad.columns:
        pivote_ciudad = pivote_ciudad.with_columns(
            (
                100
                * (pl.col("caida_petroleo") - pl.col("otro_periodo"))
                / pl.col("otro_periodo")
            ).alias("variacion_pct")
        ).sort("variacion_pct")
    guardar_tabla(pivote_ciudad, "sensibilidad_ciudades_petroleo")
    resumen["ciudades_mas_sensibles_caida_petroleo"] = pivote_ciudad.head(5).to_dicts()

    # -----------------------------------------------------------------
    # TRANSACCIONES
    # -----------------------------------------------------------------

    relacion_trans_ventas = (
        df.group_by("store_nbr")
        .agg([
            pl.col("transactions").mean().alias("transacciones_promedio"),
            pl.col("sales").sum().alias("ventas_totales"),
        ])
    )
    correlacion_trans_ventas = relacion_trans_ventas.select(
        pl.corr("transacciones_promedio", "ventas_totales").alias("correlacion")
    ).item()
    resumen["correlacion_transacciones_ventas"] = correlacion_trans_ventas

    # Ticket promedio = ventas / transacciones
    ticket_promedio = relacion_trans_ventas.with_columns(
        (pl.col("ventas_totales") / pl.col("transacciones_promedio")).alias("ticket_promedio")
    ).sort("ticket_promedio", descending=True)
    guardar_tabla(ticket_promedio, "ticket_promedio_por_tienda")

    resumen["tiendas_ticket_alto"] = ticket_promedio.head(5).to_dicts()
    resumen["tiendas_ticket_bajo"] = ticket_promedio.tail(5).to_dicts()

    # -----------------------------------------------------------------
    # Guardar resumen JSON
    # -----------------------------------------------------------------
    EDA_PROFUNDO_REPORT.parent.mkdir(parents=True, exist_ok=True)
    with open(EDA_PROFUNDO_REPORT, "w", encoding="utf-8") as f:
        json.dump(resumen, f, indent=2, ensure_ascii=False, default=str)

    logger.info(f"Resumen EDA profundo guardado en {EDA_PROFUNDO_REPORT}")
    logger.info("Tarea eda_profundo completada.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
