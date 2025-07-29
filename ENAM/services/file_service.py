import datetime
import os

def set_last_updated(file_path):
    """Set the last updated timestamp in a file"""
    with open(file_path, 'w') as f:
        f.write(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

def get_last_updated(file_path):
    """Get the last updated timestamp from a file"""
    if not os.path.exists(file_path):
        return None
    with open(file_path, 'r') as f:
        return f.read().strip()