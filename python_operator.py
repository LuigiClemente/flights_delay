import os
import requests
import json
import logging
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.jenkins.operators.jenkins_job_trigger import JenkinsJobTriggerOperator

load_dotenv()


class FlightChecker:
    def __init__(self):
        try:
            self.api_key = os.getenv("FLIGHTS_API_KEY")
            self.airports = os.getenv("AIRPORTS")
            self.airlines = os.getenv("AIRLINES")
            self.check_interval = int(os.getenv("CHECK_INTERVAL", "60")) * 60
            self.delay_threshold = int(os.getenv("DELAY_THRESHOLD", "0"))
            self.time_to_departure_threshold = int(os.getenv("TIME_TO_DEPARTURE_THRESHOLD", "0"))
            self.cancelled_flight_time_window_start = int(os.getenv("CANCELLED_FLIGHT_TIME_WINDOW_START", "0"))
            self.cancelled_flight_time_window_end = int(os.getenv("CANCELLED_FLIGHT_TIME_WINDOW_END", "0"))
            self.api_host = "https://airlabs.co/api/v9"
            self.api_endpoint = "schedules"
            self.airport_hours = self.parse_airport_hours(os.getenv("AIRPORT_HOURS", ""))
            self.last_delay_print_time = {}
            self.jenkins_job_name = os.getenv("JENKINS_JOB_NAME")
            self.jenkins_connection_id = os.getenv("JENKINS_CONNECTION_ID")
            self.ignored_destinations = {
                airport: os.getenv(f"IGNORED_DESTINATIONS_{airport}", "").split(',')
                for airport in self.airports.split(',')
            }
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
            "FLIGHTS_API_KEY", "AIRPORTS", "AIRLINES", "CHECK_INTERVAL", "DELAY_THRESHOLD",
            "TIME_TO_DEPARTURE_THRESHOLD", "CANCELLED_FLIGHT_TIME_WINDOW_START",
            "CANCELLED_FLIGHT_TIME_WINDOW_END", "AIRPORT_HOURS", "JENKINS_JOB_NAME", "JENKINS_CONNECTION_ID"
        ]
        for var in required_variables:
            if not os.getenv(var):
                raise ValueError(f"Environment variable {var} is missing or empty")

        for airport in self.airports.split(','):
            if f"IGNORED_DESTINATIONS_{airport}" not in os.environ:
                logging.warning(f"Environment variable IGNORED_DESTINATIONS_{airport} is not set, "
                                "assuming no destinations are ignored.")

    def parse_airport_hours(self, env_str):
        """
        Parses the environment variable string for airport hours.
        The format is 'AIRPORT1:OPEN1-CLOSE1,AIRPORT2:OPEN2-CLOSE2,...'
        For example: 'BCN:6-23,AMS:3-23'
        Args:
            env_str (str): The environment variable string
        Returns:
            dict: A dictionary with airport codes as keys and tuples with open and close hours as values
        """
        airport_hours = {}
        pairs = env_str.split(',')
        for pair in pairs:
            if ':' not in pair:
                continue
            airport, hours = pair.split(':')
            open_hour, close_hour = map(int, hours.split('-'))
            airport_hours[airport] = (open_hour, close_hour)
        return airport_hours

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
                    # Print flight data for analysis
                    self.print_flight_data(flight_data)
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
            logging.info(f"Received flight data: {flight_data}")
            
            if flight_data is None:
                logging.warning("Flight data is not loaded")
                return

            for flight in flight_data:
                origin = flight[0]
                destination = flight[1]
                if destination in self.ignored_destinations.get(origin, []):
                    continue
                # Perform delay analysis and notify if necessary
                try:
                    dep_time = datetime.strptime(flight[2], "%Y-%m-%d %H:%M")
                except ValueError as ve:
                    logging.error(f"Invalid departure time format in flight data: {flight}, error: {ve}")
                    continue
                dep_delayed = int(flight[3])
                status = flight[4]

                if dep_delayed > self.delay_threshold and dep_time > datetime.now() + timedelta(
                        hours=self.time_to_departure_threshold):
                    flight_iata = flight[5]
                    airport = flight[0]
                    if airport in self.last_delay_print_time and flight_iata in self.last_delay_print_time[airport]:
                        continue  # Skip already processed delays

                    logging.info(f"Flight {flight_iata} is delayed for airport {airport}.")
                    self.notify_plugin("Delayed", flight, airport=airport, flight_iata=flight_iata, **context)

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
                            self.notify_plugin("Cancelled", flight, airport=airport, flight_iata=flight_iata, **context)
        except Exception as e:
            logging.error(f"Error analyzing delays: {str(e)}")
            raise

    def notify_plugin(self, status, flight_info, airport=None, flight_iata=None, **context):
        """
        Notifies a plugin or external service about a flight status change.
        Customize this method to implement the desired notification functionality.
        Args:
            status (str): The flight status (e.g., "Delayed", "Cancelled")
            flight_info (list): Information about the flight
            airport (str): The airport code
            flight_iata (str): The flight IATA code
            context (dict): The task context dictionary
        """
        # Modify this method to implement the desired notification functionality
        print(f"Flight {flight_iata} is {status} for airport {airport}")

    def print_flight_data(self, flight_data):
        """
        Prints the flight data for analysis.
        Customize this method to implement the desired printing functionality.
        Args:
            flight_data (list): The flight data
        """
        print("Flight data:")
        for flight in flight_data:
            print(flight)


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

    load_flight_data_task >> analyze_delays_task

