# DW_Design Dockerized ETL

This repository provisions a complete environment to restore the supplied SAP B1 backup, build the dimensional model from the shared diagram, and load every source (backup, FX Excel, aggregated JSON) into SQL Server.

## Repository Layout

- `docker-compose.yml` - orchestration for the three containers (source SQL Server, DW SQL Server, Python ETL).
- `docker/etl/Dockerfile` - Python 3.11 image with ODBC Driver 18 plus libraries (pandas, pyodbc, SQLAlchemy, openpyxl).
- `docker/etl/load_dw.py` - main ETL script: restore, schema creation, data integration, and load.
- `docker/mssql/backups/DB_SALES.bak` - source backup restored into the origin SQL instance.
- `docker/etl/data/TiposCambio_USD_CRC_2024_2025.xlsx` - CRC to USD exchange rates for Dim_ExchangeRate.
- `docker/etl/data/ventas_resumen_2024_2025.json` - monthly aggregated sales (JSON channel).
- `DB_SALES_Explanation.sql` - notes for navigating the original SAP tables.

## Component Overview

1. **sqlsource container**
   - Based on mcr.microsoft.com/mssql/server:2022-latest.
   - Restores DB_SALES from the backup mounted in /var/opt/mssql/backup.
   - Exposes port 14333 so you can inspect the OLTP data (OINV, INV1, ORIN, etc.).

2. **sqldw container**
   - Another SQL Server instance (port 14334) that hosts the dimensional model DW_DESIGN.
   - Receives Dim_Date, Dim_Customer, Dim_Product, Dim_Salesperson, Dim_Warehouse, Dim_Invoice, Dim_ExchangeRate, and Fact_Sales.

3. **etl container**
   - Custom Python image installing the Microsoft ODBC driver and analytical libraries.
   - Runs docker/etl/load_dw.py, which:
     - Restores DB_SALES if it is not already online (always selects the newest backup set).
     - Rebuilds the dimensional schema in DW_DESIGN, truncating via DELETE plus DBCC CHECKIDENT to respect foreign keys.
     - Extracts data from DB_SALES and applies project rules (shift DocDate +4 years, invert credit notes, standardise USD/CRC).
     - Loads exchange rates from the Excel file, adding a USD->USD row to allow lookups.
     - Conforms the JSON monthly channel by creating dedicated customer/salesperson/warehouse members.
     - Inserts all dimensions and Fact_Sales with consistent surrogate keys.

4. **Volumes**
   - docker/mssql/source/* and docker/mssql/dw/* keep data and log files between restarts.
   - The backup and flat files are versioned inside the repo for reproducibility.

## Prerequisites

- Docker Desktop or Docker Engine with Compose v2.
- Free ports 14333 and 14334 (edit docker-compose.yml if needed).
- At least 4 GB of disk space for restored data files.

## Quick Start

```bash
docker compose up --build
```

What happens automatically:

1. sqlsource restores DB_SALES from docker/mssql/backups/DB_SALES.bak.
2. sqldw starts an empty instance ready for the DW.
3. etl runs load_dw.py: rebuilds the schema, cleans tables, loads dimensions and facts by integrating the backup, Excel, and JSON. The process ends with ETL process completed successfully. when everything loads correctly.

Default connection info:

| Server  | Host/Port        | Database   | User | Password       |
|---------|------------------|------------|------|----------------|
| Source  | localhost,14333  | DB_SALES   | sa   | StrongPwd123!  |
| DW      | localhost,14334  | DW_DESIGN  | sa   | StrongPwd123!  |

Example query:
```powershell
sqlcmd -S localhost,14334 -U sa -P StrongPwd123! -Q "SELECT TOP 10 * FROM DW_DESIGN.dbo.Fact_Sales"
```

## Customisation Tips

- Update passwords or port mappings in docker-compose.yml to fit your environment.
- Rename the DW database by editing DW_CONN (for example Database=DW_ANALYTICS).
- Replace the Excel or JSON sources in docker/etl/data/; the ETL will pick the first `TiposCambio*.xlsx` and `ventas*.json`.
- Adjust the dimensional schema in `build_dw_schema` (inside `load_dw.py`) if the data model evolves.

## By Allan, Alejandro, Santiago, Brian
Thx 4 watch <3