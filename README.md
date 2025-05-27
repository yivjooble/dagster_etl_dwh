# Dagster-ETL via Docker

### Setup Docker

1. **Build the Docker Image**
   - Navigate to the project's directory and build the Docker image:
     ```
     docker build -t dwh_dagster_prod_img .
     ```

2. **Run the Docker Container**
   - To run the Docker container for the DWH Dagster ETL, execute the following command:
     ```shell
     docker run -d -p 3007:3007 \
     --name dwh_dagster_prod \
     --env-file /path/to/env/.env \
     -v /path/to/dagster_home:/path/to/dagster_home \
     dwh_dagster_prod_img
     ```
   - This command runs the `dwh_dagster_prod` container in detached mode, maps port 3007 of the host to the container, sets the environment variables, and mounts the necessary volume.

### Additional Step for Mac M-Processors in case local setup:
   - Re-install pyodbc:
     ```
     pip uninstall pyodbc
     pip install --no-binary :all: pyodbc
     ```