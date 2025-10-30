# dashboard/timezone_converter.py

from datetime import datetime
import pytz

def timezone_converstion(location):
    """
    Converts a timezone string (e.g., 'Asia/Tokyo') to the current local time.
    Returns the time as a string in 'HH:MM AM/PM' format.
    """
    try:
        tz = pytz.timezone(location)
        return datetime.now(tz).strftime("%I:%M %p")
    except Exception as e:
        return "Invalid TZ"