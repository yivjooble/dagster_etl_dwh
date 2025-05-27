import requests
from datetime import datetime
import psycopg2
import time
from dagster import op, job
from utility_hub.core_tools import get_creds_from_vault

PROMETHEUS_URL = "http://10.0.1.164:8428"


class PrometheusClient:
    def __init__(self, base_url):
        self.base_url = base_url.rstrip('/')

    def query(self, query_str, eval_time=None):
        params = {'query': query_str}
        if eval_time:
            params['time'] = eval_time

        api_url = f"{self.base_url}/api/v1/query"
        response = requests.get(api_url, params=params)
        if response.status_code != 200:
            raise Exception(f"Prometheus API error {response.status_code}: {response.text}")
        return response.json()

    def get_metric_value(self, query_str, eval_time):
        result = self.query(query_str, eval_time)
        data = result.get("data", {}).get("result", [])
        if not data:
            raise Exception(f"No data returned for query: {query_str}")
        # Assume the value is in the first result
        value_str = data[0].get("value", [None, None])[1]
        try:
            return float(value_str)
        except Exception as e:
            raise Exception(f"Error converting value '{value_str}' to float: {e}")

    @staticmethod
    def bytes_to_human_readable(num_bytes):
        step_unit = 1024
        units = ['bytes', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']
        idx = 0
        while num_bytes >= step_unit and idx < len(units) - 1:
            num_bytes /= step_unit
            idx += 1
        return f"{num_bytes:.2f} {units[idx]}"


class PostgresManager:
    def __init__(self, db_config, context):
        self.db_config = db_config
        self.context = context

    def _connect(self):
        conn = psycopg2.connect(**self.db_config)
        conn.autocommit = True
        return conn

    def manage_connections(self):
        try:
            conn = self._connect()
            cur = conn.cursor()

            # Get current connection PID
            cur.execute("SELECT pg_backend_pid();")
            current_pid = cur.fetchone()[0]

            # Retrieve PIDs of all processes not owned by 'dbAdmin' and not current
            query = """
                SELECT pid, usename
                FROM pg_stat_activity
                WHERE usename != 'dbAdministrator'
                  AND pid <> %s;
            """
            cur.execute(query, (current_pid,))
            rows = cur.fetchall()
            pids = [row[0] for row in rows]

            if not pids:
                self.context.log.info("No PostgreSQL processes to manage.")
            else:
                self.context.log.info(f"Processes to manage (PIDs): {pids}")
                # Cancel processes gracefully
                for pid in pids:
                    self.context.log.info(f"Cancelling process {pid}...")
                    cur.execute("SELECT pg_cancel_backend(%s);", (pid,))
                time.sleep(2)  # Wait for cancellation to take effect
                # Force termination if necessary
                for pid in pids:
                    self.context.log.info(f"Terminating process {pid}...")
                    cur.execute("SELECT pg_terminate_backend(%s);", (pid,))

            cur.close()
            conn.close()
            self.context.log.info("PostgreSQL process management complete.")
        except Exception as e:
            self.context.log.info(f"Error managing PostgreSQL connections: {e}")


class RamMonitor:
    def __init__(self, prometheus_client, postgres_manager, instance, job_name, context, usage_threshold=70.0):
        self.prometheus_client = prometheus_client
        self.postgres_manager = postgres_manager
        self.instance = instance
        self.job_name = job_name
        self.context = context
        self.usage_threshold = usage_threshold

    def construct_queries(self):
        query_ram_total = (
            f"node_memory_MemTotal_bytes{{instance='{self.instance}', job='{self.job_name}'}}"
        )
        query_ram_used = (
            f"node_memory_MemTotal_bytes{{instance='{self.instance}', job='{self.job_name}'}} - "
            f"node_memory_MemFree_bytes{{instance='{self.instance}', job='{self.job_name}'}} - "
            f"(node_memory_Cached_bytes{{instance='{self.instance}', job='{self.job_name}'}} + "
            f"node_memory_Buffers_bytes{{instance='{self.instance}', job='{self.job_name}'}} + "
            f"node_memory_SReclaimable_bytes{{instance='{self.instance}', job='{self.job_name}'}})"
        )
        return query_ram_total, query_ram_used

    def run(self):
        eval_time = datetime.utcnow().isoformat("T") + "Z"
        query_ram_total, query_ram_used = self.construct_queries()

        self.context.log.info(f"--> {self.job_name}\n")

        try:
            ram_total = self.prometheus_client.get_metric_value(query_ram_total, eval_time)
            ram_used = self.prometheus_client.get_metric_value(query_ram_used, eval_time)

            self.context.log.info("\nHuman-Readable Metrics:")
            self.context.log.info(f"RAM Total: {self.prometheus_client.bytes_to_human_readable(ram_total)}")
            self.context.log.info(f"RAM Used: {self.prometheus_client.bytes_to_human_readable(ram_used)}")

            usage_pct = (ram_used / ram_total) * 100
            self.context.log.info(f"\nRAM Usage Percentage: {usage_pct:.2f}%")

            if usage_pct > self.usage_threshold:
                self.context.log.info(f"\nWARNING: RAM usage exceeds {self.usage_threshold}%. Initiating PostgreSQL process management.")
                self.postgres_manager.manage_connections()
            else:
                self.context.log.info("\nRAM usage is within acceptable limits (70%). No action taken on PostgreSQL processes.")

        except Exception as e:
            self.context.log.info(f"An error occurred during monitoring: {e}")


@op
def execute_rpl_nl(context):
    # --- Configuration ---
    instance = "nl-pgsql-statistic.jooble.com:9100"
    job_name = "nl_cluster"

    db_config = {
        "host": "10.0.1.65",
        "port": 5432,
        "dbname": "at",
        "user": get_creds_from_vault("REPLICA_USER"),
        "password": get_creds_from_vault("REPLICA_PASSWORD")
    }

    # Instantiate our clients and monitor.
    prom_client = PrometheusClient(PROMETHEUS_URL)
    pg_manager = PostgresManager(db_config, context)
    ram_monitor = RamMonitor(prom_client, pg_manager, instance, job_name, context, usage_threshold=70.0)

    # Run the monitoring and remedial actions.
    ram_monitor.run()


@op
def execute_rpl_us(context):
    # --- Configuration ---
    instance = "us-pgsql-statistic.jooble.com:9100"
    job_name = "us_cluster"

    db_config = {
        "host": "192.168.1.207",
        "port": 5432,
        "dbname": "br",
        "user": get_creds_from_vault("REPLICA_USER"),
        "password": get_creds_from_vault("REPLICA_PASSWORD")
    }

    # Instantiate our clients and monitor.
    prom_client = PrometheusClient(PROMETHEUS_URL)
    pg_manager = PostgresManager(db_config, context)
    ram_monitor = RamMonitor(prom_client, pg_manager, instance, job_name, context, usage_threshold=70.0)

    # Run the monitoring and remedial actions.
    ram_monitor.run()


@job(
    name='service__' + 'grafana_rpl_nl',
    description='Terminate process if RAM Used is higher than 70% of RAM Total.'
)
def grafana_monitor_rpl_nl():
    execute_rpl_nl()


@job(
    name='service__' + 'grafana_rpl_us',
    description='Terminate process if RAM Used is higher than 70% of RAM Total.'
)
def grafana_monitor_rpl_us():
    execute_rpl_us()
