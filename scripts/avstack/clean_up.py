# # scripts/avstack/clean_up.py

# import os, glob

# def delete_json_files():
#     target_dir = '/Users/bengrigsby/Desktop/Jobs/flightweather/data/kafka_logs/avstack'
#     files = glob.glob(os.path.join(target_dir, "*.json"))

#     if not files:
#         print(f"No files found in {target_dir}")
#         return

#     for f in files:
#         os.remove(f)
#         print(f"Deleted: {f}")
    

# if __name__ == '__main__':
#     delete_json_files()

# scripts/avstack/clean_up.py

import os
import glob

def delete_json_files():
    target_dir = '/opt/airflow/data/kafka_logs/avstack'
    files = glob.glob(os.path.join(target_dir, "*.json"))

    if not files:
        print(f"No files found in {target_dir}")
        return

    for f in files:
        os.remove(f)
        print(f"Deleted: {f}")

if __name__ == '__main__':
    delete_json_files()