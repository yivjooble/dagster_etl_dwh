from datetime import datetime


def get_datediff(date: str) -> int:
    current_date = datetime.strptime(f'{date}', '%Y-%m-%d').date()
    reference_date = datetime.strptime('1900-01-01', '%Y-%m-%d').date()
    return (current_date - reference_date).days


def get_first_day_of_month_as_int():
    # Get the current date
    today = datetime.now()

    # Extract the year and month, set the day to 1
    first_day_of_month = datetime(today.year, today.month, 1)

    # Calculate the difference from the base date (1900-01-01)
    base_date = datetime(1900, 1, 1)
    diff = (first_day_of_month - base_date).days

    return diff
