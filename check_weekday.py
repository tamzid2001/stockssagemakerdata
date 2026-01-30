from datetime import datetime, timedelta
from typing import Tuple
import sys


def is_weekday_date(date: datetime) -> Tuple[bool, str, str]:
    """
    Check if a given date is a weekday or weekend.
    
    Args:
        date: datetime object
        
    Returns:
        Tuple of (is_weekday: bool, day_name: str, date_str: str)
        is_weekday: True if date is Monday-Friday, False if Saturday/Sunday
        day_name: Name of the day (e.g., 'Monday', 'Saturday')
        date_str: Date as string in format 'YYYY-MM-DD'
    """
    day_of_week = date.weekday()
    day_name = date.strftime('%A')
    is_weekday = day_of_week < 5
    
    return is_weekday, day_name, date.strftime('%Y-%m-%d')


def is_prediction_on_weekday(start_date: str, prediction_length: int) -> Tuple[bool, str, str]:
    """
    Determine if a prediction date lands on a weekday or weekend.
    
    Args:
        start_date: Start date as string in format 'YYYY-MM-DD'
        prediction_length: Number of days to add to start date
        
    Returns:
        Tuple of (is_weekday: bool, day_name: str, date_str: str)
        is_weekday: True if prediction date is Monday-Friday, False if Saturday/Sunday
        day_name: Name of the day (e.g., 'Monday', 'Saturday')
        date_str: Date as string in format 'YYYY-MM-DD'
    """
    # Parse the start date
    date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    
    # Calculate prediction date
    prediction_date = date_obj + timedelta(days=prediction_length)
    
    return is_weekday_date(prediction_date)


if __name__ == "__main__":
    if len(sys.argv) == 2:
        # Check today or yesterday
        arg = sys.argv[1].lower()
        
        if arg == "today":
            date_to_check = datetime.now()
            is_weekday, day_name, date_str = is_weekday_date(date_to_check)
            print(f"Date: {date_str} (today)")
            print(f"Day: {day_name}")
            print(f"Is weekday: {is_weekday}")
        elif arg == "yesterday":
            date_to_check = datetime.now() - timedelta(days=1)
            is_weekday, day_name, date_str = is_weekday_date(date_to_check)
            print(f"Date: {date_str} (yesterday)")
            print(f"Day: {day_name}")
            print(f"Is weekday: {is_weekday}")
        else:
            print(f"Usage: python check_weekday.py [today|yesterday] or")
            print(f"       python check_weekday.py START_DATE DAYS")
            sys.exit(1)
    elif len(sys.argv) == 3:
        # Command-line usage with start date and days
        start_date = sys.argv[1]
        prediction_length = int(sys.argv[2])
        
        is_weekday, day_name, end_date = is_prediction_on_weekday(start_date, prediction_length)
        
        print(f"Start date: {start_date}")
        print(f"Prediction length: {prediction_length} days")
        print(f"End date: {end_date}")
        print(f"Day: {day_name}")
        print(f"Is weekday: {is_weekday}")
    else:
        # Example usage
        start = "2024-01-08"
        days = 30
        
        is_weekday, day_name, end_date = is_prediction_on_weekday(start, days)
        
        print(f"Start date: {start}")
        print(f"Prediction length: {days} days")
        print(f"End date: {end_date}")
        print(f"Day: {day_name}")
        print(f"Is weekday: {is_weekday}")
        
        # Additional example
        print("\n--- Another example ---")
        start2 = "2024-01-01"
        days2 = 7
        
        is_weekday2, day_name2, end_date2 = is_prediction_on_weekday(start2, days2)
        
        print(f"Start date: {start2}")
        print(f"Prediction length: {days2} days")
        print(f"End date: {end_date2}")
        print(f"Day: {day_name2}")
        print(f"Is weekday: {is_weekday2}")
