from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import XCom
from airflow.utils.dates import datetime, timedelta
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import logging
import os
import requests
import json
import time
from dotenv import load_dotenv

load_dotenv()

class FlightChecker:
    def __init__(self):
        try:
            self.api_key = os.getenv("FLIGHTS_API_KEY")
            self.airports = os.getenv("AIRPORTS")
            self.airlines = os.getenv("AIRLINES")
            self.delay_threshold = int(os.getenv("DELAY_THRESHOLD", "0"))
            self.time_to_departure_threshold = int(os.getenv("TIME_TO_DEPARTURE_THRESHOLD", "0"))
            self.cancelled_flight_time_window_start = int(os.getenv("CANCELLED_FLIGHT_TIME_WINDOW_START", "0"))
            self.cancelled_flight_time_window_end = int(os.getenv("CANCELLED_FLIGHT_TIME_WINDOW_END", "0"))
            self.api_host = os.getenv("API_HOST", "https://airlabs.co/api/v9")
            self.api_endpoint = os.getenv("API_ENDPOINT", "schedules")
            self.ignored_destinations_bcn = os.getenv("IGNORED_DESTINATIONS_BCN", "").split(",") if os.getenv("IGNORED_DESTINATIONS_BCN") else []
            self.ignored_destinations_ams = os.getenv("IGNORED_DESTINATIONS_AMS", "").split(",") if os.getenv("IGNORED_DESTINATIONS_AMS") else []
            self.last_delay_print_time = {}  # Stores the last delay print time for each airport

            self.validate_environment_variables()
        except Exception as e:
            logging.error(f"Error initializing FlightChecker: {str(e)}")
            raise

    def validate_environment_variables(self):
        """
        Validates the presence and validity of required environment variables.
        Raises a ValueError if any of the required variables are missing or empty.
        """
        required_variables = [
            "FLIGHTS_API_KEY", "AIRPORTS", "AIRLINES", "DELAY_THRESHOLD",
            "TIME_TO_DEPARTURE_THRESHOLD", "CANCELLED_FLIGHT_TIME_WINDOW_START",
            "CANCELLED_FLIGHT_TIME_WINDOW_END"
        ]
        for var in required_variables:
            if not os.getenv(var):
                raise ValueError(f"Environment variable {var} is missing or empty")

    def load_flight_data(self, **context):
        """
        Loads flight data from the API and stores it in XCom.
        Retries the API request with exponential backoff in case of failures.
        Args:
            context (dict): The task context dictionary
        """
        try:
            url = f"{self.api_host}/{self.api_endpoint}?dep_iata={self.airports}&api_key={self.api_key}"
            retry_count = 0
            max_retries = 5
            while retry_count < max_retries:
                try:
                    response = requests.get(url)
                    response.raise_for_status()
                    flight_data = response.json()
                    # Store flight data in XCom
                    context['ti'].xcom_push(key='flight_data', value=flight_data)
                    break
                except requests.exceptions.RequestException as e:
                    logging.error(f"Failed to load flight data: {str(e)}")
                    logging.info(f"Retrying in {2 ** retry_count} seconds...")
                    time.sleep(2 ** retry_count)
                    retry_count += 1
            else:
                raise RuntimeError("Failed to load flight data after multiple retries")
        except Exception as e:
            logging.error(f"Error loading flight data: {str(e)}")
            raise

    def analyze_delays(self, **context):
        """
        Analyzes flight delays for each airport and performs appropriate actions.
        Args:
            context (dict): The task context dictionary
        """
        try:
            # Retrieve flight data from XCom
            flight_data = context['ti'].xcom_pull(key='flight_data')
            if flight_data is None:
                logging.warning("Flight data is not loaded")
                return

            ongoing_delays = False

            for flight in flight_data:
                airport = flight[0]
                if airport == "BCN":
                    ignored_destinations = self.ignored_destinations_bcn
                elif airport == "AMS":
                    ignored_destinations = self.ignored_destinations_ams
                else:
                    continue

                if flight[1] in self.airlines.split(",") and flight[7] not in ignored_destinations:
                    dep_time = datetime.strptime(flight[2], "%Y-%m-%d %H:%M")
                    dep_delayed = int(flight[3])
                    status = flight[4]

                    if dep_delayed > self.delay_threshold and dep_time > datetime.now() + timedelta(
                            minutes=self.time_to_departure_threshold):
                        ongoing_delays = True

                        flight_iata = flight[5]

                        if airport in self.last_delay_print_time and flight_iata in self.last_delay_print_time[airport]:
                            continue  # Skip already processed delays

                        logging.info(f"Flight {flight_iata} is delayed for airport {airport}.")
                        self.notify_plugin("Delayed", flight, airport=airport, flight_iata=flight_iata)

                        # Update last delay print time
                        if airport in self.last_delay_print_time:
                            self.last_delay_print_time[airport].append(flight_iata)
                        else:
                            self.last_delay_print_time[airport] = [flight_iata]

                        # Only acknowledge a cancelled flight if a delay has been printed for the same airport
                        if status == "cancelled":
                            time_since_last_delay = (
                                    datetime.now() - self.last_delay_print_time[airport][-1]).total_seconds() / 60
                            if self.cancelled_flight_time_window_start < time_since_last_delay < self.cancelled_flight_time_window_end:
                                logging.info(f"Flight {flight_iata} is cancelled for airport {airport}.")
                                self.notify_plugin("Cancelled", flight, airport=airport, flight_iata=flight_iata)

            context['ti'].xcom_push(key='ongoing_delays', value=ongoing_delays)
        except Exception as e:
            logging.error(f"Error analyzing delays: {str(e)}")
            raise

    def get_next_call_time(self, ongoing_delays, current_time):
        """
        Determine the next API call time based on the current state of delays and time of day.
        Args:
            ongoing_delays (bool): If there are any ongoing flight delays
            current_time (datetime): Current datetime
        Returns:
            next_call_time (datetime): The next API call time
        """
        if ongoing_delays:
            # If there are ongoing delays, check every hour
            next_call_time = current_time + timedelta(hours=1)
        elif current_time.hour < 7:
            # If it's before 7 AM, check at 10 AM
            next_call_time = current_time.replace(hour=10, minute=0, second=0)
        else:
            # Otherwise, check every 3 hours
            next_call_time = current_time + timedelta(hours=3)

        return next_call_time

    def notify_plugin(self, status, flight_info, airport=None, flight_iata=None):
        """
        Notifies a plugin or external service about a flight status change.
        Customize this method to implement the desired notification functionality.
        Args:
            status (str): The flight status (e.g., "Delayed", "Cancelled")
            flight_info (list): Information about the flight
            airport (str): The airport code
            flight_iata (str): The flight IATA code
        """
        # Example implementation: Log the notification details
        logging.info(f"Notifying plugin: Status='{status}', Flight Info='{flight_info}', Airport='{airport}', Flight IATA='{flight_iata}'")


class DelayedApiCallSensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DelayedApiCallSensor, self).__init__(*args, **kwargs)

    def poke(self, context):
        flight_checker = FlightChecker()
        ongoing_delays = context['ti'].xcom_pull(task_ids='analyze_delays_task', key='ongoing_delays')

        current_time = datetime.now()

        next_call_time = flight_checker.get_next_call_time(ongoing_delays, current_time)

        if current_time >= next_call_time:
            return True
        else:
            return False


default_args = {
    'start_date': datetime(2023, 5, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
        'flight_checker',
        default_args=default_args,
        description='Flight Checker DAG',
        schedule_interval=timedelta(minutes=1),
        catchup=False
) as dag:
    flight_checker = FlightChecker()

    load_flight_data_task = PythonOperator(
        task_id='load_flight_data_task',
        python_callable=flight_checker.load_flight_data,
        provide_context=True,
        dag=dag,
    )

    analyze_delays_task = PythonOperator(
        task_id='analyze_delays_task',
        python_callable=flight_checker.analyze_delays,
        provide_context=True,
        dag=dag,
    )

    delayed_api_call_sensor = DelayedApiCallSensor(
        task_id='delayed_api_call_sensor',
        poke_interval=60,  # Check the condition every 60 seconds
        dag=dag,
    )

    load_flight_data_task >> analyze_delays_task >> delayed_api_call_sensor





