from datetime import datetime, timedelta

def get_datediff(date: str) -> int:
    current_date = datetime.strptime(f'{date}', '%Y-%m-%d').date()
    reference_date = datetime.strptime('1900-01-01', '%Y-%m-%d').date()
    return (current_date - reference_date).days


def date_diff_now():
    current_date = datetime.now().date()
    reference_date = datetime.strptime('1900-01-01', '%Y-%m-%d').date()
    return (current_date - reference_date).days


def get_previous_month_dates() -> tuple:
    now = datetime.now()
    current_month_start = now.replace(day=1)
    previous_month_end = current_month_start - timedelta(days=1)
    previous_month_start = previous_month_end.replace(day=1)
    
    return previous_month_start.strftime('%Y-%m-%d'), previous_month_end.strftime('%Y-%m-%d')