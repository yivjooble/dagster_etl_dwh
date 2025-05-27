from pathlib import Path
from dagster import RetryPolicy, Backoff, Jitter


def get_io_manager_path():
    # Define your base directory
    current_dir = Path(__file__).resolve().parent

    # Go four directories up
    base_dir = current_dir.parents[0]

    # Define a directory for your IO manager
    io_manager_dir = base_dir / "storage"

    return str(io_manager_dir)


def job_prefix():
    return 'freshdesk__'


retry_policy = RetryPolicy(
    max_retries=3,
    delay=150,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS
)
