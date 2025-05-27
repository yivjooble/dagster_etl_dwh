from datetime import datetime, timedelta


def get_datediff(date: str) -> int:
    current_date = datetime.strptime(f"{date}", "%Y-%m-%d").date()
    reference_date = datetime.strptime("1900-01-01", "%Y-%m-%d").date()
    return (current_date - reference_date).days


def date_diff_now():
    current_date = datetime.now().date()
    reference_date = datetime.strptime("1900-01-01", "%Y-%m-%d").date()
    return (current_date - reference_date).days


def date_diff_to_date(date_diff: int) -> str:
    reference_date = datetime.strptime("1900-01-01", "%Y-%m-%d").date()
    return (reference_date + timedelta(days=int(date_diff))).strftime("%Y-%m-%d")
