from datetime import datetime

def get_datediff(date: str) -> int:
    current_date = datetime.strptime(f'{date}', '%Y-%m-%d').date()
    reference_date = datetime.strptime('1900-01-01', '%Y-%m-%d').date()
    return (current_date - reference_date).days