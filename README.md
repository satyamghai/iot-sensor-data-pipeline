# IoT Sensor Data Pipeline & Risk Analytics

An end-to-end IoT sensor data pipeline built using **Databricks, PySpark, Delta Lake, Auto Loader, Medallion Architecture, and Power BI**. The project processes sensor readings, applies data-quality checks, generates device-level risk analytics, and visualizes monitoring insights through a dashboard.

## Project Overview

This project simulates an IoT monitoring workflow using CSV sensor data batches. Data is ingested into a Bronze layer, validated and cleaned in Silver, and transformed into analytical Gold tables for reporting.

## Architecture

**CSV Sensor Data → Bronze → Silver → Gold → Power BI**

- **Bronze:** Ingests raw CSV batches using Databricks Auto Loader and Delta tables, with ingestion metadata.
- **Silver:** Casts data types, validates sensor readings, quarantines invalid records, and deduplicates valid data.
- **Gold:** Provides device-level metrics, hourly trends, daily alert summaries, and high-risk device analytics.
- **Power BI:** Visualizes sensor activity, error rates, alert trends, and device risk distribution.

## Technologies Used

- Databricks
- PySpark and Spark SQL
- Delta Lake
- Databricks Auto Loader
- Unity Catalog
- Medallion Architecture
- Power BI
- SQL and DAX

## Data Quality Results

The pipeline processed **172,800 raw sensor records** and produced the following Silver-layer results:

| Metric | Result |
|---|---:|
| Raw records ingested | 172,800 |
| Valid records | 155,305 |
| Quarantined records | 17,495 |
| Silver pass rate | 89.88% |

Invalid records are quarantined rather than included in the validated Silver dataset.

## Gold Analytics

The Gold layer provides analytical tables for:

- Device-level readings, temperature, humidity, errors, and alerts
- Hourly sensor trends
- Daily alert summaries
- High-risk device details
- Device risk distribution

The risk model combines error rate, alert rate, and average temperature to assign device risk categories.

## Power BI Dashboard

The **IoT Sensor Monitoring & Risk Analytics** dashboard includes:

- Total readings and active readings
- Overall error rate and alert events
- Active versus error readings by device
- Top five devices by error count
- Device risk distribution
- High-risk device details
- Daily alert trends

![IoT Sensor Monitoring & Risk Analytics](IOT_Dashboard_Screenshot.png)

## Repository Structure

```text
iot-sensor-data-pipeline/
├── Bronze_Ingestion.py
├── Silver_Processing.py
├── Gold_Analytics.py
├── Project_Setup.py
├── IOT_Dashboard.pbix
├── IOT_Dashboard_Screenshot.png
└── README.md
```

## How to Explore

1. Review the notebooks in Bronze → Silver → Gold order.
2. Examine the data-quality validation and quarantine logic in Silver.
3. Explore the analytical tables generated in Gold.
4. Open `IOT_Dashboard.pbix` in Power BI Desktop to explore the dashboard.

The notebooks are designed for Databricks. Catalogs, schemas, volumes, source paths, and checkpoints may need to be configured for another workspace.

## Project Highlights

- Developed a multi-layer data pipeline using Bronze, Silver, and Gold architecture.
- Implemented data-quality validation and quarantine handling for invalid sensor records.
- Created Gold-layer analytics for device performance, alerts, and risk classification.
- Built a Power BI dashboard to monitor sensor activity, errors, and device risk distribution.

---

**Author:** Satyam  
**Project:** IoT Sensor Data Pipeline & Risk Analytics
