from dagster import RetryPolicy, Backoff, Jitter

"""
    Full Jitter: Random delay between 0 and the exponential backoff delay.
    Equal Jitter: Half of the exponential backoff delay plus a random delay between 0 and half of the exponential backoff delay.
    Decorrelated Jitter: Random delay based on the previous delay, aiming to avoid clustering.
    No Jitter: No randomization, just using the exponential backoff delay.
    Plus-Minus Jitter: Exponential backoff delay plus or minus a random small fraction to spread out retries.
"""

retry_policy = RetryPolicy(
    max_retries=5, delay=5, backoff=Backoff.EXPONENTIAL, jitter=Jitter.PLUS_MINUS
)

job_config = {
    "execution": {
        "config": {
            "multiprocess": {
                "max_concurrent": 10,
            }
        }
    }
}

# даний COUNTRY_LIST не включає в себе "de" за запитом від аналітиків.
web_bigquery_statistic_country_list = [
    "ua",
    "uk",
    "fr",
    "ca",
    "us",
    "id",
    "pl",
    "hu",
    "ro",
    "es",
    "at",
    "be",
    "br",
    "ch",
    "cz",
    "in",
    "it",
    "nl",
    "tr",
    "cl",
    "co",
    "gr",
    "sk",
    "th",
    "tw",
    "bg",
    "hr",
    "kz",
    "no_",
    "rs",
    "se",
    "nz",
    "ng",
    "ar",
    "mx",
    "pe",
    "cn",
    "hk",
    "kr",
    "ph",
    "pk",
    "jp",
    "pr",
    "sv",
    "cr",
    "au",
    "do",
    "uy",
    "ec",
    "sg",
    "az",
    "fi",
    "ba",
    "pt",
    "dk",
    "ie",
    "my",
    "za",
    "ae",
    "qa",
    "sa",
    "kw",
    "bh",
    "eg",
    "ma",
    "uz",
]

country_list = [
    "ua",
    "uk",
    "fr",
    "ca",
    "us",
    "id",
    "pl",
    "hu",
    "ro",
    "es",
    "at",
    "be",
    "br",
    "ch",
    "cz",
    "in",
    "it",
    "nl",
    "tr",
    "cl",
    "co",
    "gr",
    "sk",
    "th",
    "tw",
    "bg",
    "hr",
    "kz",
    "no_",
    "rs",
    "se",
    "nz",
    "ng",
    "ar",
    "mx",
    "pe",
    "cn",
    "hk",
    "kr",
    "ph",
    "pk",
    "jp",
    "pr",
    "sv",
    "cr",
    "au",
    "do",
    "uy",
    "ec",
    "sg",
    "az",
    "fi",
    "ba",
    "pt",
    "dk",
    "ie",
    "my",
    "za",
    "ae",
    "qa",
    "sa",
    "kw",
    "bh",
    "eg",
    "ma",
    "uz",
    "de"
]


map_country_code_to_id = {
    'ua': 1,
    'de': 2,
    'uk': 3,
    'fr': 4,
    'ca': 5,
    'us': 6,
    'id': 7,
    'pl': 9,
    'hu': 10,
    'ro': 11,
    'es': 12,
    'at': 13,
    'be': 14,
    'br': 15,
    'ch': 16,
    'cz': 17,
    'in': 18,
    'it': 19,
    'nl': 20,
    'tr': 21,
    'cl': 23,
    'co': 24,
    'gr': 25,
    'sk': 26,
    'th': 27,
    'tw': 28,
    'bg': 30,
    'hr': 31,
    'kz': 32,
    'no': 33,
    'rs': 34,
    'se': 35,
    'nz': 36,
    'ng': 37,
    'ar': 38,
    'mx': 39,
    'pe': 40,
    'cn': 41,
    'hk': 42,
    'kr': 43,
    'ph': 44,
    'pk': 45,
    'jp': 46,
    'pr': 48,
    'sv': 49,
    'cr': 50,
    'au': 51,
    'do': 52,
    'uy': 53,
    'ec': 54,
    'sg': 55,
    'az': 56,
    'fi': 57,
    'ba': 58,
    'pt': 59,
    'dk': 60,
    'ie': 61,
    'my': 62,
    'za': 63,
    'ae': 64,
    'qa': 65,
    'sa': 66,
    'kw': 67,
    'bh': 68,
    'eg': 69,
    'ma': 70,
    'uz': 71,
}
