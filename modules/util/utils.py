import datetime

def date_generator(start_year, end_year):
    start_date = datetime.date(start_year, 1, 1)
    end_date = datetime.date(end_year, 12, 31)
    
    date_list = []
    
    current_date = start_date
    while current_date <= end_date:
        date_list.append(current_date.strftime('%Y%m'))
        current_date = current_date.replace(day=1) + datetime.timedelta(days=32)
        current_date = current_date.replace(day=1)
        
    return date_list