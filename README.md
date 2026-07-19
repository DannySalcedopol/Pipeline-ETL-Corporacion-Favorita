```markdown
# Pipeline ETL Corporación Favorita

## 1. Descripción del proyecto
Pipeline ETL desarrollado para el procesamiento y análisis de datos de ventas de Corporación Favorita. El proyecto implementa un flujo automatizado utilizando Apache Airflow como herramienta de orquestación y Polars como motor principal para la transformación eficiente de grandes volúmenes de datos. El pipeline permite realizar la extracción de datos desde archivos CSV, limpieza y transformación de información, consolidación en formato Parquet, generación de análisis exploratorio y exportación hacia PostgreSQL para su consumo mediante herramientas de Business Intelligence como Power BI.

---

## 2. Tecnologías utilizadas

| Tecnología | Uso |
|---|---|
| Python | Desarrollo del pipeline ETL |
| Apache Airflow 2.10.5 | Orquestación y ejecución automática |
| Polars | Limpieza y transformación de datos |
| Pandas / NumPy | Análisis complementario |
| PyArrow | Manejo de archivos Parquet |
| PostgreSQL | Almacenamiento analítico |
| Power BI | Visualización de resultados |

---

## 3. Arquitectura de la solución
```text
   Dataset CSV
        |
        v
  Apache Airflow
        |
        v
DAG favorita_pipeline
        |
        +------------------+
        |                  |
        v                  v
   Carga datos      Limpieza Polars
        |                  |
        +------------------+
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

## 4. Descripción del DAG

* **Archivo principal:** `dags/favorita_pipeline.py`
* **Nombre del DAG:** `favorita_pipeline`

### Tareas ejecutadas:

| Tarea | Descripción |
| --- | --- |
| cargar_datos | Lectura de archivos CSV |
| eda_inicial | Análisis inicial de calidad |
| limpiar_datos | Limpieza y transformación |
| consolidar | Unión y generación Parquet |
| eda_profundo | Creación de tablas analíticas |
| exportar_postgres | Carga hacia PostgreSQL |

**Flujo:** `cargar_datos -> eda_inicial -> limpiar_datos -> consolidar -> eda_profundo -> exportar_postgres`

---

## 5. Proceso del pipeline

* **Etapa 1 (Carga):** Archivos `train.csv`, `test.csv`, `stores.csv`, `transactions.csv`, `oil.csv` y `holidays_events.csv`.
* **Etapa 2 (Análisis Exploratorio Inicial):** Reportes de cantidad de registros, tipos de datos, nulos y estadísticas generales. Ver captura en: `capturas/01_graph.png`.
* **Etapa 3 (Limpieza):** Con Polars se tratan nulos, conversión de tipos y eliminación de inconsistencias.
* **Etapa 4 (Consolidación):** Integración de todos los datasets en un archivo maestro en formato Parquet.
* **Etapa 5 (Análisis Profundo):** Tablas analíticas de evolución de ventas, tiendas, familias, impacto de promociones, feriados y relación con el precio del petróleo.
* **Etapa 6 (Exportación):** Envío de los datos consolidados hacia PostgreSQL. Ver capturas en `capturas/03_tablas_postgres.png` y `capturas/04_exportacion.png`.

---

## 6. Métricas del pipeline

### Registros procesados:

| Dataset | Registros |
| --- | --- |
| train.csv | 3,000,888 |
| stores.csv | 54 |
| transactions.csv | 83,488 |
| oil.csv | 1,218 |
| holidays_events.csv | 350 |

*Monitoreo de tiempo en Airflow disponible en:* `capturas/02_gantt.png`

---

## 7. Dashboard Power BI

El resultado del pipeline se usará para dashboards interactivos con indicadores de evolución de ventas globales, impacto de promociones y análisis económico relacionado con el petróleo. *(Capturas del dashboard serán agregadas posteriormente)*

---

## 8. Despliegue del proyecto

* **Requisitos:** Python 3.12, Apache Airflow 2.10.5, PostgreSQL y entorno virtual.
* **Instalación:**
1. Crear entorno: `python3 -m venv venv`
2. Activar: `source venv/bin/activate`
3. Instalar: `pip install -r requirements.txt`
4. Ejecutar Airflow: Terminal 1 (`airflow webserver`) y Terminal 2 (`airflow scheduler`)
5. Ejecutar DAG: `airflow dags trigger favorita_pipeline`



---

## 9. Estructura del proyecto

```text
ProyectoAnalisis/
├── dags/
│   └── favorita_pipeline.py
├── scripts/
│   ├── 01_cargar_datos.py
│   ├── 02_eda_inicial.py
│   ├── 03_limpiar_datos.py
│   ├── 04_consolidar.py
│   ├── 05_eda_profundo.py
│   ├── 06_exportar_postgres.py
│   └── config.py
├── capturas/
│   ├── 01_graph.png
│   ├── 02_gantt.png
│   ├── 03_tablas_postgres.png
│   └── 04_exportacion.png
├── requirements.txt
├── manifest.json
└── README.md

```

---

## Autores

* Damian Cadavid
* Danny Salcedo
* Dereck Ortiz

*Proyecto académico - Pipeline ETL Corporación Favorita*

```

```
