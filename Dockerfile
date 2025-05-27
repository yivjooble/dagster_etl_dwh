# Start with a Python base image based on Debian Bullseye
FROM python:3.11.4-slim

# Update the package lists, install necessary packages and cleanup in one step
RUN apt-get update && apt-get install -y \
    software-properties-common \
    gnupg \
    apt-transport-https \
    curl \
#    # Add the official MariaDB repository for the MariaDB Connector/C
#    # and the GPG key to verify the packages
    && apt-key adv --fetch-keys 'https://mariadb.org/mariadb_release_signing_key.asc' \
    && curl -LsS https://downloads.mariadb.com/MariaDB/mariadb_repo_setup | bash \
#    # Download and add Microsoft repository GPG keys
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
#    # Register the Microsoft SQL Server Ubuntu repository for Debian 10
    && curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y \
        libmariadb3 libmariadb-dev \
        libpq-dev \
        gcc \
        unixodbc-dev \
        msodbcsql17 \
#    # Cleanup
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    && apt-get autoremove

WORKDIR /app

RUN mkdir /app/dagster_home

RUN pip install "cython<3.0.0" wheel && pip install pyyaml==5.4.1 --no-build-isolation

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright and browsers
RUN playwright install \
    && playwright install-deps

# Set variables for the container
ENV DAGSTER_HOME=/app/dagster_home/
ENV DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1

COPY ./openssl.cnf /etc/ssl/openssl.cnf

COPY . .

# Expose the port your app runs on
EXPOSE 3007

# Run your Dagster project with the specified command
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3007"]