"""
04_consolidar.py
Tarea 4 del DAG: consolidar

Une mediante joins los cinco archivos limpios en un solo DataFrame, usando
los campos comunes: store_nbr, date, item_family (family).

Estrategia de joins:
1. train (base, grano: store_nbr + date + family) LEFT JOIN stores
   por store_nbr -> agrega city, state, type (tienda), cluster.
2. resultado LEFT JOIN transactions por (store_nbr, date)
   -> agrega número de transacciones del día en esa tienda.
3. resultado LEFT JOIN oil por date
   -> agrega precio del petróleo del día (ya interpolado en tarea 3).
4. resultado LEFT JOIN holidays por date
   -> agrega información de feriado si la fecha coincide (puede generar
      múltiples filas si hay más de un feriado registrado el mismo día
      a distintos niveles -nacional/regional/local-, por eso se agrega
      una bandera simplificada es_feriado_nacional además de conservar
      el detalle).

Se usa LEFT JOIN (no inner) porque no todas las fechas tienen feriado ni
todas las tiendas/fechas tienen registro de transacciones, y no queremos
perder filas de venta por ausencia de esos datos complementarios.

Salida: CONSOLIDATED_PARQUET, la estructura unificada que alimenta el
EDA profundo (tarea 5) y la persistencia en PostgreSQL (tarea 6).
"""

import sys

import polars as pl

from config import CLEANED_DIR, CONSOLIDATED_PARQUET, get_logger

logger = get_logger("consolidar")


def main():
    logger.info("Cargando archivos limpios ...")
    train = pl.read_parquet(CLEANED_DIR / "train_limpio.parquet")
    stores = pl.read_parquet(CLEANED_DIR / "stores_limpio.parquet")
    transactions = pl.read_parquet(CLEANED_DIR / "transactions_limpio.parquet")
    oil = pl.read_parquet(CLEANED_DIR / "oil_limpio.parquet")
    holidays = pl.read_parquet(CLEANED_DIR / "holidays_limpio.parquet")

    filas_train = train.height
    logger.info(f"train (base): {filas_train} filas")

    # 1) train + stores (por store_nbr)
    df = train.join(stores, on="store_nbr", how="left")
    logger.info(f"Tras join con stores: {df.height} filas")

    # 2) + transactions (por store_nbr, date)
    df = df.join(transactions, on=["store_nbr", "date"], how="left")
    logger.info(f"Tras join con transactions: {df.height} filas")

    # 3) + oil (por date)
    df = df.join(oil, on="date", how="left")
    logger.info(f"Tras join con oil: {df.height} filas")

    # 4) + holidays (por date). holidays puede tener múltiples registros
    # para la misma fecha (feriado nacional + local el mismo día), así que
    # primero se construye una vista resumida por fecha para no explotar
    # el número de filas de train con productos cartesianos.
    holidays_resumen = (
        holidays.group_by("date")
        .agg([
            (pl.col("type") == "Holiday").any().alias("es_feriado"),
            pl.col("transferred").any().alias("feriado_transferido"),
            pl.col("locale").first().alias("feriado_locale"),
            pl.col("locale_name").first().alias("feriado_locale_name"),
            pl.col("description").first().alias("feriado_descripcion"),
        ])
    )

    df = df.join(holidays_resumen, on="date", how="left")
    df = df.with_columns([
        pl.col("es_feriado").fill_null(False),
        pl.col("feriado_transferido").fill_null(False),
    ])
    logger.info(f"Tras join con holidays (resumido): {df.height} filas")

    if df.height != filas_train:
        logger.warning(
            f"El número de filas cambió respecto a train original "
            f"({filas_train} -> {df.height}). Verificar duplicados en "
            f"holidays_resumen o en stores/transactions por clave."
        )

    df.write_parquet(CONSOLIDATED_PARQUET)
    logger.info(
        f"Consolidado escrito en {CONSOLIDATED_PARQUET} "
        f"({df.height} filas, {df.width} columnas)."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
