from dagster import RetryPolicy, Backoff, Jitter

"""
    Full Jitter: Random delay between 0 and the exponential backoff delay.
    Equal Jitter: Half of the exponential backoff delay plus a random delay between 0 and half of the exponential backoff delay.
    Decorrelated Jitter: Random delay based on the previous delay, aiming to avoid clustering.
    No Jitter: No randomization, just using the exponential backoff delay.
    Plus-Minus Jitter: Exponential backoff delay plus or minus a random small fraction to spread out retries.
"""

ch_countries = [
    'at',
    # 'ch',
    # 'be',
    # 'cz',
    # 'ua',
    # 'hu',
    # 'ro',
    'fr',
    'pl',
    'de',
    'uk'
]

retry_policy = RetryPolicy(
    max_retries=5,
    delay=5,
    backoff=Backoff.EXPONENTIAL,
    jitter=Jitter.PLUS_MINUS
)

job_config = {
    "execution": {
        "config": {
            "multiprocess": {
                "max_concurrent": 24,
            }
        }
    }
}
