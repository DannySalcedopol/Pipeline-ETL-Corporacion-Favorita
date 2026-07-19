# Pipeline ETL Corporación Favorita

## Descripción

Pipeline ETL desarrollado para el procesamiento y análisis de datos de ventas de Corporación Favorita.

El proyecto implementa un flujo automatizado utilizando Apache Airflow para la orquestación de tareas y Polars para la transformación eficiente de grandes volúmenes de datos.

El pipeline realiza la carga de datos, limpieza, consolidación, análisis exploratorio y exportación hacia PostgreSQL para su posterior visualización en herramientas de Business Intelligence como Power BI.

---

## Arquitectura del proyecto

```text
Dataset CSV
     |
     v
Apache Airflow
     |
     v
DAG favorita_pipeline
     |
     +----------------+
     |                |
     v                v
Carga datos     Limpieza Polars
     |                |
     +----------------+
             |
             v
     Consolidación Parquet
             |
             v
    Análisis Exploratorio
             |
             v
        PostgreSQL
             |
             v
          Power BI
```

---

## Tecnologías utilizadas

| Tecnología | Uso |
|---|---|
| Python | Desarrollo del pipeline |
| Apache Airflow 2.10.5 | Orquestación de procesos ETL |
| Polars | Transformación y limpieza de datos |
| Pandas / NumPy | Análisis de datos |
| PyArrow | Manejo de archivos Parquet |
| PostgreSQL | Almacenamiento final |
| Power BI | Visualización de resultados |

---

## Estructura del proyecto

```text
ProyectoAnalisis/
│
├── dags/
│   └── favorita_pipeline.py
│       DAG principal de Airflow
│
├── scripts/
│   ├── 01_cargar_datos.py
│   ├── 02_eda_inicial.py
│   ├── 03_limpiar_datos.py
│   ├── 04_consolidar.py
│   ├── 05_eda_profundo.py
│   ├── 06_exportar_postgres.py
│   └── config.py
│
├── data/
│   Datos originales del proyecto
│
├── favorita_data/
│   Resultados generados del pipeline
│   (excluidos del repositorio)
│
├── sql/
│   Scripts SQL
│
├── requirements.txt
├── manifest.json
└── .gitignore
```

---

## Flujo ETL

### 1. Carga de datos

Se cargan los archivos CSV:

- train.csv
- test.csv
- stores.csv
- transactions.csv
- oil.csv
- holidays_events.csv

Los datos son preparados para su procesamiento.

---

### 2. Análisis exploratorio inicial

Se generan métricas iniciales:

- cantidad de registros
- tipos de datos
- valores nulos
- estadísticas generales

---

### 3. Limpieza de datos

Proceso realizado con Polars:

- tratamiento de valores faltantes
- conversión de tipos
- eliminación de inconsistencias
- preparación de datasets limpios

---

### 4. Consolidación

Se integran las diferentes fuentes de información:

- ventas
- tiendas
- transacciones
- petróleo
- feriados

El resultado es almacenado en formato Parquet.

---

### 5. Análisis exploratorio profundo

Se generan tablas analíticas para estudiar:

- evolución de ventas
- comportamiento por tienda
- comportamiento por familia de productos
- impacto de promociones
- relación con feriados y precio del petróleo

---

### 6. Exportación

Los datos consolidados son enviados hacia PostgreSQL para consumo analítico y visualización.

---

## Ejecución del proyecto

Activar entorno virtual:

```bash
source venv/bin/activate
```

Ejecutar Airflow:

```bash
airflow webserver
```

En otra terminal:

```bash
airflow scheduler
```

Ejecutar el DAG:

```bash
airflow dags trigger favorita_pipeline
```

---

## Variables de configuración

Las conexiones y parámetros sensibles deben almacenarse mediante variables de entorno.

Archivo:

```text
.env
```

Este archivo no debe incluirse en Git.

---

## Control de versiones

El proyecto utiliza Git para mantener el historial de cambios.

Archivos excluidos:

- datasets originales
- archivos generados
- logs
- entornos virtuales

---

## Autores

- Damian Cadavid
- Danny Salcedo
- Dereck Ortiz

Proyecto académico - Pipeline ETL Corporación Favorita
