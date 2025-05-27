from dagster import RetryPolicy, Backoff

retry_policy=RetryPolicy(
        max_retries = 5,
        delay = 5,
        backoff=Backoff.LINEAR
    )


job_config = {
    "execution": {
        "config": {
            "multiprocess": {
                "max_concurrent": 14,
            }
        }
    }
}