FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    DEBIAN_FRONTEND=noninteractive \
    ACCEPT_EULA=Y

# Install Microsoft ODBC Driver 18 for SQL Server and build prerequisites
RUN apt-get update \
    && apt-get install -y curl apt-transport-https gnupg ca-certificates \
    && mkdir -p /etc/apt/keyrings /usr/share/keyrings \
    && curl -sSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /etc/apt/keyrings/microsoft.gpg \
    && cp /etc/apt/keyrings/microsoft.gpg /usr/share/keyrings/microsoft-prod.gpg \
    && chmod 644 /etc/apt/keyrings/microsoft.gpg /usr/share/keyrings/microsoft-prod.gpg \
    && curl -sSL https://packages.microsoft.com/config/debian/12/prod.list -o /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && apt-get install -y msodbcsql18 unixodbc-dev build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
