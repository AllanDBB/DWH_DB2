import json
import logging
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import pyodbc
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)


def _switch_database(conn_str: str, database: str) -> str:
    pattern = re.compile(r"(Database|Initial Catalog)=([^;]+)", re.IGNORECASE)
    if pattern.search(conn_str):
        return pattern.sub(f"\\1={database}", conn_str)
    separator = "" if conn_str.endswith(";") else ";"
    return f"{conn_str}{separator}Database={database};"


def _extract_database_name(conn_str: str) -> Optional[str]:
    pattern = re.compile(r"(Database|Initial Catalog)=([^;]+)", re.IGNORECASE)
    match = pattern.search(conn_str)
    return match.group(2) if match else None


def _quoted(value: str) -> str:
    escaped = value.replace("'", "''")
    return f"N'{escaped}'"


def _get_filename(physical_name: str) -> str:
    normalized = physical_name.replace("\\", "/")
    return normalized.split("/")[-1]


def _connect(conn_str: str, autocommit: bool = True) -> pyodbc.Connection:
    logging.debug("Opening connection: %s", conn_str)
    return pyodbc.connect(conn_str, autocommit=autocommit)


def ensure_source_database(restored_conn_str: str, backup_path: str) -> None:
    db_name = _extract_database_name(restored_conn_str)
    if not db_name:
        raise RuntimeError("SOURCE_CONN must include a Database element.")

    master_conn_str = _switch_database(restored_conn_str, "master")
    with _connect(master_conn_str) as conn:
        cursor = conn.cursor()
        existing = cursor.execute(
            "SELECT state_desc FROM sys.databases WHERE name = ?", db_name
        ).fetchone()
        if existing and existing[0] == "ONLINE":
            logging.info(
                "Source database %s already online. Skipping restore.", db_name
            )
            return

        logging.info("Restoring source database %s from %s", db_name, backup_path)
        header_rows = cursor.execute(
            f"RESTORE HEADERONLY FROM DISK = {_quoted(backup_path)}"
        ).fetchall()
        if not header_rows:
            raise RuntimeError("Unable to read header metadata from backup file.")
        latest_header = max(header_rows, key=lambda row: row.BackupFinishDate)
        file_number = getattr(latest_header, "Position", None)
        if file_number is None:
            raise RuntimeError("Backup metadata did not include Position information.")
        logging.info(
            "Using backup set position %s with finish date %s.",
            file_number,
            latest_header.BackupFinishDate,
        )

        file_list = cursor.execute(
            f"RESTORE FILELISTONLY FROM DISK = {_quoted(backup_path)} WITH FILE = {file_number}"
        ).fetchall()

        if not file_list:
            raise RuntimeError("Unable to read metadata from backup file.")

        move_clauses = []
        for row in file_list:
            logical_name = row.LogicalName
            file_type = row.Type.strip()
            original_name = _get_filename(row.PhysicalName)
            target_dir = "/var/opt/mssql/data" if file_type == "D" else "/var/opt/mssql/log"
            target_path = f"{target_dir}/{original_name}"
            move_clauses.append(f"MOVE {_quoted(logical_name)} TO {_quoted(target_path)}")

        restore_options = [f"FILE = {file_number}", *move_clauses, "REPLACE", "RECOVERY"]

        restore_sql = f"""
RESTORE DATABASE [{db_name}]
FROM DISK = {_quoted(backup_path)}
WITH {', '.join(restore_options)}
"""
        cursor.execute(restore_sql)
        logging.info("Restore of %s completed.", db_name)

        # Ensure the database is online before continuing
        start = time.time()
        while True:
            row = cursor.execute(
                "SELECT state_desc FROM sys.databases WHERE name = ?", db_name
            ).fetchone()
            state = row[0] if row else None
            if state == "ONLINE":
                break
            logging.info(
                "Waiting for database %s to be ONLINE (current state: %s).",
                db_name,
                state,
            )
            if time.time() - start > 300:
                raise TimeoutError(f"Database {db_name} did not come online within timeout.")
            time.sleep(2)
        logging.info("Source database %s is ONLINE.", db_name)


def ensure_dw_database(dw_conn_str: str) -> None:
    db_name = _extract_database_name(dw_conn_str)
    if not db_name:
        raise RuntimeError("DW_CONN must include a Database element.")
    master_conn_str = _switch_database(dw_conn_str, "master")
    with _connect(master_conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute(
            f"""
IF DB_ID({_quoted(db_name)}) IS NULL
BEGIN
    EXEC('CREATE DATABASE [{db_name}]');
END
"""
        )
        logging.info("Ensured DW database %s exists.", db_name)


def _to_sqlalchemy_url(conn_str: str) -> str:
    return f"mssql+pyodbc:///?odbc_connect={quote_plus(conn_str)}"


def build_dw_schema(dw_conn_str: str) -> None:
    statements = [
        """
IF OBJECT_ID('dbo.Dim_Date','U') IS NULL
BEGIN
    CREATE TABLE dbo.Dim_Date (
        DateKey INT NOT NULL PRIMARY KEY,
        DateValue DATE NOT NULL,
        [Day] INT NOT NULL,
        [Month] INT NOT NULL,
        [Year] INT NOT NULL
    );
END
""",
        """
IF OBJECT_ID('dbo.Dim_Customer','U') IS NULL
BEGIN
    CREATE TABLE dbo.Dim_Customer (
        CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
        CardCode NVARCHAR(50) NOT NULL,
        CardName NVARCHAR(255) NOT NULL,
        Country NVARCHAR(100) NULL,
        Zone NVARCHAR(100) NULL,
        CONSTRAINT UX_DimCustomer_CardCode UNIQUE (CardCode)
    );
END
""",
        """
IF OBJECT_ID('dbo.Dim_Product','U') IS NULL
BEGIN
    CREATE TABLE dbo.Dim_Product (
        ProductKey INT IDENTITY(1,1) PRIMARY KEY,
        ItemCode NVARCHAR(50) NOT NULL,
        ItemName NVARCHAR(255) NOT NULL,
        Brand NVARCHAR(100) NULL,
        CONSTRAINT UX_DimProduct_ItemCode UNIQUE (ItemCode)
    );
END
""",
        """
IF OBJECT_ID('dbo.Dim_Salesperson','U') IS NULL
BEGIN
    CREATE TABLE dbo.Dim_Salesperson (
        SalespersonKey INT IDENTITY(1,1) PRIMARY KEY,
        SlpCode NVARCHAR(50) NOT NULL,
        SlpName NVARCHAR(255) NOT NULL,
        Active BIT NOT NULL,
        CONSTRAINT UX_DimSalesperson_SlpCode UNIQUE (SlpCode)
    );
END
""",
        """
IF OBJECT_ID('dbo.Dim_Invoice','U') IS NULL
BEGIN
    CREATE TABLE dbo.Dim_Invoice (
        InvoiceKey INT IDENTITY(1,1) PRIMARY KEY,
        DocEntry INT NOT NULL,
        DocNum INT NULL,
        DocType NVARCHAR(20) NULL,
        DocStatus NVARCHAR(20) NULL,
        DocCur NVARCHAR(10) NULL,
        IsCredit BIT NOT NULL,
        CONSTRAINT UX_DimInvoice_DocEntry UNIQUE (DocEntry)
    );
END
""",
        """
IF OBJECT_ID('dbo.Dim_ExchangeRate','U') IS NULL
BEGIN
    CREATE TABLE dbo.Dim_ExchangeRate (
        ExchangeRateKey INT IDENTITY(1,1) PRIMARY KEY,
        RateDate DATE NOT NULL,
        FromCurrency NVARCHAR(10) NOT NULL,
        ToCurrency NVARCHAR(10) NOT NULL,
        Rate DECIMAL(18,6) NOT NULL,
        CONSTRAINT UX_DimExchangeRate UNIQUE (RateDate, FromCurrency, ToCurrency)
    );
END
""",
        """
IF OBJECT_ID('dbo.Dim_Warehouse','U') IS NULL
BEGIN
    CREATE TABLE dbo.Dim_Warehouse (
        WarehouseKey INT IDENTITY(1,1) PRIMARY KEY,
        WhsCode NVARCHAR(50) NOT NULL,
        WhsName NVARCHAR(255) NOT NULL,
        CONSTRAINT UX_DimWarehouse_WhsCode UNIQUE (WhsCode)
    );
END
""",
        """
IF OBJECT_ID('dbo.Fact_Sales','U') IS NULL
BEGIN
    CREATE TABLE dbo.Fact_Sales (
        FactSalesID INT IDENTITY(1,1) PRIMARY KEY,
        WarehouseKey INT NOT NULL,
        DateKey INT NOT NULL,
        CustomerKey INT NOT NULL,
        ProductKey INT NOT NULL,
        SalespersonKey INT NOT NULL,
        InvoiceKey INT NOT NULL,
        ExchangeRateKey INT NOT NULL,
        Quantity DECIMAL(18,4) NOT NULL,
        UnitPriceUSD DECIMAL(18,4) NOT NULL,
        LineTotalUSD DECIMAL(18,4) NOT NULL,
        CONSTRAINT FK_FactSales_Date FOREIGN KEY (DateKey) REFERENCES dbo.Dim_Date (DateKey),
        CONSTRAINT FK_FactSales_Customer FOREIGN KEY (CustomerKey) REFERENCES dbo.Dim_Customer (CustomerKey),
        CONSTRAINT FK_FactSales_Product FOREIGN KEY (ProductKey) REFERENCES dbo.Dim_Product (ProductKey),
        CONSTRAINT FK_FactSales_Salesperson FOREIGN KEY (SalespersonKey) REFERENCES dbo.Dim_Salesperson (SalespersonKey),
        CONSTRAINT FK_FactSales_Invoice FOREIGN KEY (InvoiceKey) REFERENCES dbo.Dim_Invoice (InvoiceKey),
        CONSTRAINT FK_FactSales_ExchangeRate FOREIGN KEY (ExchangeRateKey) REFERENCES dbo.Dim_ExchangeRate (ExchangeRateKey),
        CONSTRAINT FK_FactSales_Warehouse FOREIGN KEY (WarehouseKey) REFERENCES dbo.Dim_Warehouse (WarehouseKey)
    );
END
""",
    ]

    with _connect(dw_conn_str) as conn:
        cursor = conn.cursor()
        for stmt in statements:
            cursor.execute(stmt)
        logging.info("DW schema verified/created.")


def _truncate_tables(dw_conn_str: str) -> None:
    with _connect(dw_conn_str) as conn:
        cursor = conn.cursor()
        cursor.execute("IF OBJECT_ID('dbo.Fact_Sales','U') IS NOT NULL DELETE FROM dbo.Fact_Sales;")
        for table in [
            "Dim_Invoice",
            "Dim_ExchangeRate",
            "Dim_Warehouse",
            "Dim_Salesperson",
            "Dim_Product",
            "Dim_Customer",
            "Dim_Date",
        ]:
            cursor.execute(f"IF OBJECT_ID('dbo.{table}','U') IS NOT NULL DELETE FROM dbo.{table};")

        # Reset identity seeds to keep deterministic surrogate keys on reruns
        for table in [
            "Dim_Invoice",
            "Dim_ExchangeRate",
            "Dim_Warehouse",
            "Dim_Salesperson",
            "Dim_Product",
            "Dim_Customer",
        ]:
            cursor.execute(
                f"IF OBJECT_ID('dbo.{table}','U') IS NOT NULL DBCC CHECKIDENT('dbo.{table}', RESEED, 0);"
            )
        cursor.execute(
            "IF OBJECT_ID('dbo.Fact_Sales','U') IS NOT NULL DBCC CHECKIDENT('dbo.Fact_Sales', RESEED, 0);"
        )
        logging.info("DW tables truncated.")


def _clean_dataframe(df: pd.DataFrame, columns: Sequence[str]) -> List[Tuple]:
    subset = df.loc[:, columns].where(pd.notnull(df), None)
    return [tuple(row) for row in subset.to_numpy()]


def _bulk_insert(conn_str: str, table: str, columns: Sequence[str], df: pd.DataFrame) -> int:
    rows = _clean_dataframe(df, columns)
    if not rows:
        logging.warning("No rows to insert into %s.", table)
        return 0

    placeholders = ",".join(["?"] * len(columns))
    sql = f"INSERT INTO dbo.{table} ({', '.join(columns)}) VALUES ({placeholders})"
    with _connect(conn_str) as conn:
        cursor = conn.cursor()
        cursor.fast_executemany = True
        cursor.executemany(sql, rows)
        logging.info("Inserted %d rows into %s.", len(rows), table)
    return len(rows)


def _find_file(path: Path, extension: str, keyword: str) -> Path:
    matches = list(path.glob(f"**/*{keyword}*.{extension}"))
    if not matches:
        raise FileNotFoundError(f"No {extension} file containing '{keyword}' found under {path}")
    return matches[0]


def _load_exchange_rates(data_dir: Path) -> pd.DataFrame:
    xlsx_path = _find_file(data_dir, "xlsx", "Tipo")
    df = pd.read_excel(xlsx_path)
    df.columns = [col.strip() for col in df.columns]
    date_col = next((c for c in df.columns if "fecha" in c.lower()), None)
    rate_col = next((c for c in df.columns if "tipo" in c.lower()), None)
    if not date_col or not rate_col:
        raise RuntimeError("Exchange rate file must contain 'Fecha' and rate columns.")
    df = df.rename(columns={date_col: "RateDate", rate_col: "CRCperUSD"})
    df["RateDate"] = pd.to_datetime(df["RateDate"]).dt.normalize()
    df["FromCurrency"] = "CRC"
    df["ToCurrency"] = "USD"
    df["Rate"] = df["CRCperUSD"].astype(float)
    df = df[["RateDate", "FromCurrency", "ToCurrency", "Rate"]]

    usd_reference = df["RateDate"].min()
    usd_row = pd.DataFrame(
        [{
            "RateDate": usd_reference,
            "FromCurrency": "USD",
            "ToCurrency": "USD",
            "Rate": 1.0,
        }]
    )
    combined = pd.concat([df, usd_row], ignore_index=True)
    combined = combined.drop_duplicates(subset=["RateDate", "FromCurrency", "ToCurrency"])
    return combined.sort_values("RateDate")


def _load_json_sales(data_dir: Path) -> pd.DataFrame:
    json_path = _find_file(data_dir, "json", "venta")
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    records: List[Dict] = []
    doc_counter = -1
    for entry in payload:
        year = entry.get("anio")
        month = entry.get("mes")
        ventas = entry.get("ventas", [])
        if year is None or month is None:
            continue
        doc_date = datetime(year=int(year), month=int(month), day=1)
        for venta in ventas:
            doc_counter -= 1
            quantity = float(venta.get("cantidad", 0))
            price = float(venta.get("precio", 0))
            item_code = venta.get("item")
            line_total = quantity * price
            records.append(
                {
                    "Source": "JSON",
                    "DocEntry": doc_counter,
                    "DocNum": doc_counter,
                    "DocDate": doc_date,
                    "CardCode": "JSON_CUST",
                    "SlpCode": "JSON",
                    "WhsCode": "JSON",
                    "ItemCode": item_code,
                    "Quantity": quantity,
                    "Currency": "USD",
                    "LineTotal": line_total,
                    "IsCredit": False,
                    "DocType": "JSON",
                    "DocStatus": "Closed",
                    "DocCur": "USD",
                }
            )
    return pd.DataFrame.from_records(records)


def _load_source_tables(src_conn_str: str) -> Dict[str, pd.DataFrame]:
    engine = create_engine(_to_sqlalchemy_url(src_conn_str))

    def read(query: str) -> pd.DataFrame:
        return pd.read_sql(text(query), engine)

    oinv = read("SELECT DocEntry, DocNum, DocType, DocStatus, DocCur, DocDate, CardCode, SlpCode, DocTotal, DocTotalFC FROM OINV")
    orins = read("SELECT DocEntry, DocNum, DocType, DocStatus, DocCur, DocDate, CardCode, SlpCode, DocTotal, DocTotalFC FROM ORIN")
    inv1 = read(
        """
SELECT
    DocEntry,
    LineNum,
    ItemCode,
    Quantity,
    Price,
    LineTotal
FROM INV1
"""
    )
    rin1 = read(
        """
SELECT
    DocEntry,
    LineNum,
    ItemCode,
    Quantity,
    Price,
    LineTotal
FROM RIN1
"""
    )
    ocrd = read("SELECT CardCode, CardName, Country, ISNULL(U_Zona, '') AS U_Zona FROM OCRD WHERE CardType = 'C'")
    zonas = read("SELECT * FROM ZONAS")
    ocry = read("SELECT Country AS Code, Name FROM OCRY")
    oitm = read("SELECT ItemCode, ItemName, ISNULL(U_Marca, '') AS U_Marca FROM OITM")
    marcas = read("SELECT * FROM MARCAS")
    oslp = read("SELECT SlpCode, SlpName, Active FROM OSLP")
    owhs = read("SELECT WhsCode, WhsName FROM OWHS")

    return {
        "OINV": oinv,
        "ORIN": orins,
        "INV1": inv1,
        "RIN1": rin1,
        "OCRD": ocrd,
        "ZONAS": zonas,
        "OCRY": ocry,
        "OITM": oitm,
        "MARCAS": marcas,
        "OSLP": oslp,
        "OWHS": owhs,
    }


def _normalize_invoice_data(source_tables: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    oinv = source_tables["OINV"].copy()
    orins = source_tables["ORIN"].copy()
    inv1 = source_tables["INV1"].copy()
    rin1 = source_tables["RIN1"].copy()

    if not len(oinv):
        raise RuntimeError("Source table OINV is empty.")

    oinv["IsCredit"] = False
    orins["IsCredit"] = True

    orins["DocTotal"] = orins["DocTotal"].fillna(0) * -1
    orins["DocTotalFC"] = orins["DocTotalFC"].fillna(0) * -1

    invoice_header = pd.concat([oinv, orins], ignore_index=True, sort=False)
    if "DocDate" in invoice_header.columns:
        invoice_header["DocDate"] = (
            pd.to_datetime(invoice_header["DocDate"]) + pd.DateOffset(years=4)
        ).dt.date

    rin1[["Quantity", "Price", "LineTotal"]] = rin1[["Quantity", "Price", "LineTotal"]].fillna(0) * -1
    if "TotalFrgn" in rin1.columns:
        rin1["TotalFrgn"] = rin1["TotalFrgn"].fillna(0) * -1
    if "Currency" in rin1.columns:
        rin1["Currency"] = rin1["Currency"].astype(str)

    line_items = pd.concat([inv1, rin1], ignore_index=True, sort=False)
    line_items = line_items.merge(
        invoice_header,
        on="DocEntry",
        suffixes=("", "_header"),
        how="left",
    )

    for column in ["DocDate", "DocType", "DocStatus", "DocCur", "CardCode", "SlpCode", "DocNum", "IsCredit"]:
        header_column = f"{column}_header"
        if header_column in line_items.columns and column not in line_items.columns:
            line_items[column] = line_items[header_column]

    header_cols = [col for col in line_items.columns if col.endswith("_header")]
    line_items = line_items.drop(columns=header_cols)

    line_items["DocDate"] = pd.to_datetime(line_items["DocDate"])

    if "Currency" not in line_items.columns:
        line_items["Currency"] = None
    if "WhsCode" not in line_items.columns:
        line_items["WhsCode"] = None
    if "TotalFrgn" not in line_items.columns:
        line_items["TotalFrgn"] = None

    line_items["DocCur"] = line_items["DocCur"].fillna(line_items["Currency"])
    line_items["Currency"] = line_items["Currency"].fillna(line_items["DocCur"]).fillna("CRC")
    line_items["WhsCode"] = line_items["WhsCode"].fillna("UNKNOWN")

    return line_items


def _build_dimensions(source_tables: Dict[str, pd.DataFrame], exchange_rates: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    ocrd = source_tables["OCRD"].copy()
    zonas = source_tables["ZONAS"].copy()
    ocry = source_tables["OCRY"].copy()
    oitm = source_tables["OITM"].copy()
    marcas = source_tables["MARCAS"].copy()
    oslp = source_tables["OSLP"].copy()
    owhs = source_tables["OWHS"].copy()

    zonas.columns = [c.strip() for c in zonas.columns]
    zonas_code = next((c for c in zonas.columns if c.lower() in ("code", "codzona", "cod_zona")), zonas.columns[0])
    zonas_name = next((c for c in zonas.columns if "name" in c.lower()), zonas.columns[-1])

    zonas_lookup = dict(zip(zonas[zonas_code], zonas[zonas_name]))
    country_lookup = dict(zip(ocry["Code"], ocry["Name"]))

    ocrd["CountryName"] = ocrd["Country"].map(country_lookup).fillna(ocrd["Country"])
    ocrd["Zone"] = ocrd["U_Zona"].map(zonas_lookup).fillna(ocrd["U_Zona"])
    dim_customer = ocrd[["CardCode", "CardName", "CountryName", "Zone"]].rename(
        columns={"CountryName": "Country"}
    )

    dim_customer = pd.concat(
        [
            pd.DataFrame(
                [
                    {"CardCode": "UNKNOWN", "CardName": "Cliente Desconocido", "Country": None, "Zone": None},
                    {"CardCode": "JSON_CUST", "CardName": "Canal JSON", "Country": "Desconocido", "Zone": "JSON"},
                ]
            ),
            dim_customer,
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["CardCode"])

    marcas.columns = [c.strip() for c in marcas.columns]
    marca_code = next((c for c in marcas.columns if c.lower() in ("code", "codmarca")), marcas.columns[0])
    marca_name = next((c for c in marcas.columns if "name" in c.lower()), marcas.columns[-1])
    marca_lookup = dict(zip(marcas[marca_code], marcas[marca_name]))
    oitm["Brand"] = oitm["U_Marca"].map(marca_lookup).fillna(oitm["U_Marca"])
    dim_product = oitm[["ItemCode", "ItemName", "Brand"]]
    dim_product = pd.concat(
        [
            pd.DataFrame(
                [
                    {"ItemCode": "UNKNOWN", "ItemName": "Producto Desconocido", "Brand": None},
                ]
            ),
            dim_product,
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["ItemCode"])

    oslp["Active"] = oslp["Active"].apply(lambda value: bool(int(value)) if str(value).isdigit() else str(value).upper() in ("Y", "YES", "TRUE", "T"))
    dim_salesperson = oslp[["SlpCode", "SlpName", "Active"]]
    dim_salesperson = pd.concat(
        [
            pd.DataFrame(
                [
                    {"SlpCode": "UNKNOWN", "SlpName": "Vendedor Desconocido", "Active": False},
                    {"SlpCode": "JSON", "SlpName": "Canal JSON", "Active": False},
                ]
            ),
            dim_salesperson,
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["SlpCode"])

    dim_warehouse = pd.concat(
        [
            pd.DataFrame(
                [
                    {"WhsCode": "UNKNOWN", "WhsName": "Almacen Desconocido"},
                    {"WhsCode": "JSON", "WhsName": "Canal JSON"},
                ]
            ),
            owhs[["WhsCode", "WhsName"]],
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["WhsCode"])

    dim_exchange = exchange_rates.copy()

    return {
        "Dim_Customer": dim_customer,
        "Dim_Product": dim_product,
        "Dim_Salesperson": dim_salesperson,
        "Dim_Warehouse": dim_warehouse,
        "Dim_ExchangeRate": dim_exchange,
    }


def _prepare_fact(
    normalized_lines: pd.DataFrame,
    json_sales: pd.DataFrame,
    exchange_rates: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    exchange_rates = exchange_rates.sort_values("RateDate")
    normalized_lines["Currency"] = normalized_lines["Currency"].fillna(normalized_lines["DocCur"])
    normalized_lines["CurrencyNorm"] = normalized_lines["Currency"].str.upper().str.strip()

    transactional = normalized_lines.rename(
        columns={
            "DocDate": "Date",
        }
    )
    transactional["Date"] = pd.to_datetime(transactional["Date"])
    transactional = transactional.sort_values("Date")

    exchange_rates_for_merge = exchange_rates.rename(
        columns={
            "RateDate": "RateReferenceDate",
            "Rate": "RateValue",
            "FromCurrency": "RateFromCurrency",
            "ToCurrency": "RateToCurrency",
        }
    )
    exchange_rates_for_merge["RateReferenceDate"] = pd.to_datetime(
        exchange_rates_for_merge["RateReferenceDate"]
    )
    transactional = pd.merge_asof(
        transactional.sort_values("Date"),
        exchange_rates_for_merge.sort_values("RateReferenceDate"),
        left_on="Date",
        right_on="RateReferenceDate",
        direction="backward",
    )

    transactional["RateValue"] = transactional["RateValue"].fillna(method="ffill").fillna(method="bfill")
    transactional["RateReferenceDate"] = transactional["RateReferenceDate"].fillna(transactional["Date"])
    transactional["Rate"] = transactional.apply(
        lambda row: 1.0 if row["CurrencyNorm"] == "USD" else float(row["RateValue"])
        if pd.notnull(row["RateValue"])
        else 1.0,
        axis=1,
    )
    transactional["FromCurrency"] = transactional.apply(
        lambda row: "USD" if row["CurrencyNorm"] == "USD" else "CRC",
        axis=1,
    )
    transactional["LineTotal"] = transactional["LineTotal"].fillna(0).astype(float)
    if "TotalFrgn" in transactional.columns:
        transactional["TotalFrgn"] = transactional["TotalFrgn"].fillna(0).astype(float)

    def compute_line_total_usd(row: pd.Series) -> float:
        if row["CurrencyNorm"] == "USD":
            if "TotalFrgn" in row.index and row["TotalFrgn"]:
                return float(row["TotalFrgn"])
            return float(row["LineTotal"])
        rate_value = row["Rate"] if row["Rate"] else 1.0
        return float(row["LineTotal"]) / rate_value if rate_value else float(row["LineTotal"])

    transactional["LineTotalUSD"] = transactional.apply(compute_line_total_usd, axis=1)
    transactional["Quantity"] = transactional["Quantity"].fillna(0).astype(float)
    transactional["UnitPriceUSD"] = transactional.apply(
        lambda row: row["LineTotalUSD"] / row["Quantity"] if row["Quantity"] else 0.0,
        axis=1,
    )

    transactional = transactional.drop(columns=["RateFromCurrency", "RateToCurrency"], errors="ignore")

    transactional["DocDate"] = transactional["Date"]
    transactional["DocEntry"] = transactional["DocEntry"].astype(int)
    transactional["DocNum"] = transactional["DocNum"].fillna(transactional["DocEntry"]).astype(int)
    transactional["IsCredit"] = transactional["IsCredit"].fillna(False)

    json_sales = json_sales.copy()
    if not json_sales.empty:
        json_sales["Rate"] = 1.0
        json_sales["LineTotalUSD"] = json_sales["LineTotal"]
        json_sales["UnitPriceUSD"] = json_sales.apply(
            lambda row: row["LineTotal"] / row["Quantity"] if row["Quantity"] else row["LineTotal"],
            axis=1,
        )
        json_sales["Date"] = pd.to_datetime(json_sales["DocDate"])
        json_sales["RateDate"] = json_sales["Date"]
        json_sales["RateReferenceDate"] = json_sales["Date"]
        json_sales["RateValue"] = 1.0

    fact_df = pd.concat(
        [transactional, json_sales],
        ignore_index=True,
        sort=False,
    )
    fact_df["DocDate"] = pd.to_datetime(fact_df["DocDate"])
    fact_df["RateDate"] = pd.to_datetime(
        fact_df["RateReferenceDate"] if "RateReferenceDate" in fact_df.columns else fact_df["DocDate"]
    )
    fact_df = fact_df.drop(columns=["RateReferenceDate"], errors="ignore")
    fact_df["DateKey"] = fact_df["DocDate"].dt.strftime("%Y%m%d").astype(int)
    fact_df["DocDate"] = fact_df["DocDate"].dt.date
    usd_reference_date = pd.to_datetime(exchange_rates["RateDate"].min())
    fact_df.loc[fact_df["FromCurrency"] == "USD", "RateDate"] = usd_reference_date
    fact_df["FromCurrency"] = fact_df["FromCurrency"].fillna("USD")
    fact_df["ToCurrency"] = "USD"
    fact_df["CardCode"] = fact_df["CardCode"].fillna("UNKNOWN")
    fact_df["ItemCode"] = fact_df["ItemCode"].fillna("UNKNOWN")
    fact_df["SlpCode"] = fact_df["SlpCode"].fillna("UNKNOWN")
    fact_df["WhsCode"] = fact_df["WhsCode"].fillna("UNKNOWN")
    fact_df["DocType"] = fact_df["DocType"].fillna("UNKNOWN")
    fact_df["DocStatus"] = fact_df["DocStatus"].fillna("Closed")
    fact_df["DocCur"] = fact_df["DocCur"].fillna(fact_df["Currency"]).fillna("CRC")
    fact_df["Currency"] = fact_df["Currency"].fillna("USD")

    invoice_dim = fact_df[
        [
            "DocEntry",
            "DocNum",
            "DocType",
            "DocStatus",
            "DocCur",
            "IsCredit",
        ]
    ].drop_duplicates(subset=["DocEntry"])
    invoice_dim = pd.concat(
        [
            pd.DataFrame(
                [
                    {
                        "DocEntry": 0,
                        "DocNum": None,
                        "DocType": "UNKNOWN",
                        "DocStatus": None,
                        "DocCur": None,
                        "IsCredit": False,
                    }
                ]
            ),
            invoice_dim,
        ],
        ignore_index=True,
    ).drop_duplicates(subset=["DocEntry"])

    return fact_df, invoice_dim


def _build_dim_date(fact_df: pd.DataFrame) -> pd.DataFrame:
    if fact_df.empty:
        raise RuntimeError("Fact dataset is empty; cannot build Dim_Date.")
    unique_dates = sorted(fact_df["DocDate"].unique())
    date_records = []
    for value in unique_dates:
        date_val = pd.to_datetime(value).date()
        date_records.append(
            {
                "DateKey": int(date_val.strftime("%Y%m%d")),
                "DateValue": date_val,
                "Day": date_val.day,
                "Month": date_val.month,
                "Year": date_val.year,
            }
        )
    return pd.DataFrame(date_records)


def _load_dimensions(dw_conn_str: str, dimensions: Dict[str, pd.DataFrame], dim_date: pd.DataFrame, dim_invoice: pd.DataFrame) -> None:
    _bulk_insert(dw_conn_str, "Dim_Date", ["DateKey", "DateValue", "Day", "Month", "Year"], dim_date)
    customer_cols = ["CardCode", "CardName", "Country", "Zone"]
    product_cols = ["ItemCode", "ItemName", "Brand"]
    salesperson_cols = ["SlpCode", "SlpName", "Active"]
    invoice_cols = ["DocEntry", "DocNum", "DocType", "DocStatus", "DocCur", "IsCredit"]
    exchange_cols = ["RateDate", "FromCurrency", "ToCurrency", "Rate"]
    warehouse_cols = ["WhsCode", "WhsName"]

    _bulk_insert(dw_conn_str, "Dim_Customer", customer_cols, dimensions["Dim_Customer"])
    _bulk_insert(dw_conn_str, "Dim_Product", product_cols, dimensions["Dim_Product"])
    _bulk_insert(dw_conn_str, "Dim_Salesperson", salesperson_cols, dimensions["Dim_Salesperson"])
    _bulk_insert(dw_conn_str, "Dim_Warehouse", warehouse_cols, dimensions["Dim_Warehouse"])
    _bulk_insert(dw_conn_str, "Dim_ExchangeRate", exchange_cols, dimensions["Dim_ExchangeRate"])
    _bulk_insert(dw_conn_str, "Dim_Invoice", invoice_cols, dim_invoice)


def _fetch_lookup(engine, table: str, key_column: str, natural_columns: Sequence[str]) -> Dict[Tuple, int]:
    cols = ", ".join([key_column] + list(natural_columns))
    df = pd.read_sql(text(f"SELECT {cols} FROM dbo.{table}"), engine)
    for col in natural_columns:
        if "date" in col.lower():
            df[col] = pd.to_datetime(df[col])
    lookup = {}
    for _, row in df.iterrows():
        key = tuple(row[col] for col in natural_columns)
        lookup[key] = row[key_column]
    return lookup


def _assign_dimension_keys(dw_conn_str: str, fact_df: pd.DataFrame) -> pd.DataFrame:
    engine = create_engine(_to_sqlalchemy_url(dw_conn_str))

    customer_lookup = _fetch_lookup(engine, "Dim_Customer", "CustomerKey", ["CardCode"])
    product_lookup = _fetch_lookup(engine, "Dim_Product", "ProductKey", ["ItemCode"])
    salesperson_lookup = _fetch_lookup(engine, "Dim_Salesperson", "SalespersonKey", ["SlpCode"])
    invoice_lookup = _fetch_lookup(engine, "Dim_Invoice", "InvoiceKey", ["DocEntry"])
    warehouse_lookup = _fetch_lookup(engine, "Dim_Warehouse", "WarehouseKey", ["WhsCode"])
    exchange_lookup = _fetch_lookup(
        engine,
        "Dim_ExchangeRate",
        "ExchangeRateKey",
        ["RateDate", "FromCurrency", "ToCurrency"],
    )

    def map_value(row, lookup: Dict[Tuple, int], columns: Iterable[str], fallback_key: Tuple) -> int:
        key = tuple(row[col] for col in columns)
        if key in lookup:
            return lookup[key]
        if fallback_key in lookup:
            return lookup[fallback_key]
        raise KeyError(f"No dimension key for {columns}={key}")

    fact_df["CustomerKey"] = fact_df.apply(
        lambda row: map_value(row, customer_lookup, ["CardCode"], ("UNKNOWN",)),
        axis=1,
    )
    fact_df["ProductKey"] = fact_df.apply(
        lambda row: map_value(row, product_lookup, ["ItemCode"], ("UNKNOWN",)),
        axis=1,
    )
    fact_df["SalespersonKey"] = fact_df.apply(
        lambda row: map_value(row, salesperson_lookup, ["SlpCode"], ("UNKNOWN",)),
        axis=1,
    )
    fact_df["WarehouseKey"] = fact_df.apply(
        lambda row: map_value(row, warehouse_lookup, ["WhsCode"], ("UNKNOWN",)),
        axis=1,
    )
    fact_df["InvoiceKey"] = fact_df.apply(
        lambda row: map_value(row, invoice_lookup, ["DocEntry"], (0,)),
        axis=1,
    )
    usd_fallback_key = (fact_df["RateDate"].min(), "USD", "USD")
    fact_df["ExchangeRateKey"] = fact_df.apply(
        lambda row: map_value(
            row,
            exchange_lookup,
            ["RateDate", "FromCurrency", "ToCurrency"],
            usd_fallback_key,
        ),
        axis=1,
    )
    return fact_df


def _load_fact(dw_conn_str: str, fact_df: pd.DataFrame) -> None:
    columns = [
        "WarehouseKey",
        "DateKey",
        "CustomerKey",
        "ProductKey",
        "SalespersonKey",
        "InvoiceKey",
        "ExchangeRateKey",
        "Quantity",
        "UnitPriceUSD",
        "LineTotalUSD",
    ]
    _bulk_insert(dw_conn_str, "Fact_Sales", columns, fact_df[columns])


def main() -> None:
    source_conn = os.getenv("SOURCE_CONN")
    dw_conn = os.getenv("DW_CONN")
    backup_path = os.getenv("SOURCE_BACKUP_PATH", "/var/opt/mssql/backup/DB_SALES.bak")
    data_dir = Path(os.getenv("DATA_DIR", "/workspace/docker/etl/data"))

    if not source_conn or not dw_conn:
        raise RuntimeError("Environment variables SOURCE_CONN and DW_CONN must be set.")

    ensure_source_database(source_conn, backup_path)
    ensure_dw_database(dw_conn)
    build_dw_schema(dw_conn)
    _truncate_tables(dw_conn)

    exchange_rates = _load_exchange_rates(data_dir)
    json_sales = _load_json_sales(data_dir)
    source_tables = _load_source_tables(source_conn)
    normalized_lines = _normalize_invoice_data(source_tables)
    fact_df, dim_invoice = _prepare_fact(normalized_lines, json_sales, exchange_rates)
    dim_date = _build_dim_date(fact_df)
    dimensions = _build_dimensions(source_tables, exchange_rates)

    _load_dimensions(dw_conn, dimensions, dim_date, dim_invoice)
    fact_with_keys = _assign_dimension_keys(dw_conn, fact_df)
    _load_fact(dw_conn, fact_with_keys)
    logging.info("ETL process completed successfully.")


if __name__ == "__main__":
    main()
