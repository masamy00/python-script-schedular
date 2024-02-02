import os
import schedule
import time
import json
import subprocess
import logging
from datetime import datetime

MAX_TIMEOUT = 21600  # 6 hours in seconds
LOGS_FOLDER = 'logs'
SCRIPTS_FOLDER = 'scripts'
SCHEDULERS_FOLDER = 'schedulers'


def configure_logging():
    current_month = datetime.now().strftime("%Y%m")
    log_file = os.path.join(LOGS_FOLDER, f"{current_month}.log")

    try:
        if not os.path.exists(LOGS_FOLDER):
            os.makedirs(LOGS_FOLDER)
    except OSError as e:
        print(f"Error creating logs folder: {e}")
        raise

    logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')


def run_script(script_file, timeout):
    # Enforce maximum timeout
    timeout = min(timeout, MAX_TIMEOUT) if timeout is not None else MAX_TIMEOUT
    logging.info(f"Running script {script_file}")

    try:
        subprocess.run(['python', script_file], timeout=timeout, check=True)
    except subprocess.CalledProcessError as e:
        logging.error(f"Error running script {script_file}: {e}")
    except subprocess.TimeoutExpired:
        logging.warning(f"Script {script_file} timed out after {timeout} seconds.")
    except Exception as e:
        logging.error(f"An unexpected error occurred while running script {script_file}: {e}")


def read_schedule_json(json_file):
    with open(json_file, 'r') as file:
        data = json.load(file)
    return data


def job(script_file, schedule_data):
    try:
        logging.info(f"Running {script_file} at {schedule_data}")
        run_script(script_file, schedule_data.get("timeout", None))
    except Exception as e:
        logging.error(f"An unexpected error occurred while running script {script_file}: {e}")


def schedule_scripts_in_folder(scripts_folder, schedulers_folder):
    for script_file in os.listdir(scripts_folder):
        if script_file.endswith(".py"):
            script_path = os.path.join(scripts_folder, script_file)
            json_file = os.path.join(schedulers_folder, f"{script_file.split('.')[0]}_schedule.json")

            if os.path.exists(json_file):
                schedule_data = read_schedule_json(json_file)

                if schedule_data['frequency'] == 'daily':
                    if 'clocks' in schedule_data:
                        for time_data in schedule_data['clocks']:
                            hour = time_data.get('hour', 0)
                            minute = time_data.get('minute', 0)
                            print(f"Scheduling {script_path} daily at {hour:02d}:{minute:02d}")
                            schedule.every().day.at(f"{hour:02d}:{minute:02d}").do(job, script_path, schedule_data)

                    elif 'days' in schedule_data:
                        for day in schedule_data['days']:
                            for time_data in schedule_data.get('times', []):
                                hour = time_data.get('hour', 0)
                                minute = time_data.get('minute', 0)
                                print(f"Scheduling {script_path} on {day} at {hour:02d}:{minute:02d}")
                                schedule.every().day.at(f"{hour:02d}:{minute:02d}").do(job, script_path, schedule_data)

                elif schedule_data['frequency'] == 'hourly':
                    print(f"Scheduling {script_path} hourly at :{schedule_data.get('minute', 0)}")
                    schedule.every().hour.at(f":{schedule_data.get('minute', 0)}").do(job, script_path, schedule_data)

                elif schedule_data['frequency'] == 'weekly':
                    for day in schedule_data.get('days', []):
                        hour = schedule_data.get('hour', 0)
                        minute = schedule_data.get('minute', 0)
                        print(f"Scheduling {script_path} weekly on {day} at {hour:02d}:{minute:02d}")
                        schedule.every().day.at(f"{hour:02d}:{minute:02d}").do(job, script_path, schedule_data)

    logging.info("Scripts scheduled.")

    logging.info("Scripts scheduled.")


def main():
    configure_logging()

    scripts_folder = 'scripts'
    schedulers_folder = 'schedulers'
    schedule_scripts_in_folder(scripts_folder, schedulers_folder)

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
