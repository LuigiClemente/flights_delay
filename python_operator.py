import os
import requests
import json
import logging
import time
from datetime import datetime, timedelta
from pytz import timezone
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.jenkins.operators.jenkins import JenkinsJobTriggerOperator

load_dotenv()

class FlightChecker:
    def __init__(self):
        try:
            # Load environment variables
            self.api_key = os.getenv("FLIGHTS_API_KEY")
            self.airports = os.getenv("AIRPORTS")
            self.airlines = os.getenv("AIRLINES")
            self.delay_threshold = int(os.getenv("DELAY_THRESHOLD", "0"))
            self.time_to_departure_threshold = int(os.getenv("TIME_TO_DEPARTURE_THRESHOLD", "0"))
            self.cancelled_flight_time_window_start = int(os.getenv("CANCELLED_FLIGHT_TIME_WINDOW_START", "0"))
            self.cancelled_flight_time_window_end = int(os.getenv("CANCELLED_FLIGHT_TIME_WINDOW_END", "0"))
            self.api_host = os.getenv("API_HOST", "https://airlabs.co")
            self.api_endpoint = os.getenv("API_ENDPOINT", "schedules")
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
            "FLIGHTS_API_KEY", "AIRPORTS", "AIRLINES", "DELAY_THRESHOLD",
            "TIME_TO_DEPARTURE_THRESHOLD", "CANCELLED_FLIGHT_TIME_WINDOW_START",
            "CANCELLED_FLIGHT_TIME_WINDOW_END", "API_HOST", "API_ENDPOINT",
            "JENKINS_JOB_NAME", "JENKINS_CONNECTION_ID"
        ]
        for var in required_variables:
            if not os.getenv(var):
                raise ValueError(f"Environment variable {var} is missing or empty")

        for airport in self.airports.split(','):
            if f"IGNORED_DESTINATIONS_{airport}" not in os.environ:
                logging.warning(f"Environment variable IGNORED_DESTINATIONS_{airport} is not set, "
                                "assuming no destinations are ignored.")

    def load_flight_data(self, **context):
        """
        Loads flight data from the API and stores it in XCom.
        Retries the API request with exponential backoff in case of failures.
        Args:
            context (dict): The task context dictionary
        """
        try:
            config = self.load_config_file()
            if not config or not self.is_flight_data_valid(config):
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
                        self.update_config_file(flight_data)
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

                if dep_delayed > self.delay_threshold and dep_time > datetime.now(tz=timezone('UTC')) + timedelta(
                        minutes=self.time_to_departure_threshold):
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
                                datetime.now(tz=timezone('UTC')) - self.last_delay_print_time[airport][-1]).total_seconds() / 60
                        if self.cancelled_flight_time_window_start < time_since_last_delay < self.cancelled_flight_time_window_end:
                            logging.info(f"Flight {flight_iata} is cancelled for airport {airport}.")
                            self.notify_plugin("Cancelled", flight, airport=airport, flight_iata=flight_iata, **context)
        except Exception as e:
            logging.error(f"Error analyzing delays: {str(e)}")
            raise

    def update_config_file(self, flight_data):
        """
        Updates the config.json file with the next potential delayed flight information.
        Args:
            flight_data (list): The flight data
        """
        next_flight = self.get_next_potential_delayed_flight(flight_data)
        config = {
            "next_flight": next_flight
        }
        with open("config.json", "w") as config_file:
            json.dump(config, config_file)

    def get_next_potential_delayed_flight(self, flight_data):
        """
        Determines the next potential delayed flight based on the current time and flight data.
        Args:
            flight_data (list): The flight data
        Returns:
            dict: Information about the next potential delayed flight
        """
        now = datetime.now(tz=timezone('UTC'))
        next_flight = None
        for flight in flight_data:
            origin = flight[0]
            destination = flight[1]
            if destination in self.ignored_destinations.get(origin, []):
                continue
            dep_time = datetime.strptime(flight[2], "%Y-%m-%d %H:%M").replace(tzinfo=timezone('UTC'))
            if dep_time > now and (dep_time - now) > timedelta(minutes=self.delay_threshold):
                flight_iata = flight[5]
                airline = flight[6]
                next_flight = {
                    "origin": origin,
                    "destination": destination,
                    "departure_time": dep_time.isoformat(),
                    "airline": airline,
                    "flight_number": flight_iata
                }
                break
        return next_flight

    def load_config_file(self):
        """
        Loads the config.json file and returns its contents as a dictionary.
        Returns:
            dict: The contents of the config.json file
        """
        try:
            with open("config.json", "r") as config_file:
                config = json.load(config_file)
            return config
        except FileNotFoundError:
            logging.error("config.json file not found")
            return {}

    def is_flight_delayed(self, flight):
        """
        Checks if a flight is delayed based on the current time and the departure time of the flight.
        Args:
            flight (dict): Information about the flight
        Returns:
            bool: True if the flight is delayed, False otherwise
        """
        departure_time = flight.get("departure_time")
        if not departure_time:
            logging.error("Departure time missing from flight data")
            return False
        try:
            departure_time = datetime.fromisoformat(departure_time).replace(tzinfo=timezone('UTC'))
        except ValueError:
            logging.error(f"Invalid isoformat string: '{departure_time}'")
            return False
        return departure_time + timedelta(minutes=self.delay_threshold) < datetime.now(tz=timezone('UTC'))

    def is_flight_data_valid(self, config):
        """
        Checks if the flight data stored in the config is valid and within the time threshold.
        Args:
            config (dict): The contents of the config.json file
        Returns:
            bool: True if the flight data is valid and within the time threshold, False otherwise
        """
        if "next_flight" in config:
            next_flight = config["next_flight"]
            if self.is_flight_delayed(next_flight):
                return True
        return False

    def check_flight_status(self):
        """
        Checks the flight status and performs appropriate actions.
        """
        try:
            config = self.load_config_file()
            if self.is_flight_data_valid(config):
                next_flight = config["next_flight"]
                logging.info(f"Flight {next_flight['flight_number']} is delayed for airport {next_flight['origin']}.")
                self.notify_plugin("Delayed", next_flight)
            else:
                logging.info("No delayed flights found.")
        except Exception as e:
            logging.error(f"Error checking flight status: {str(e)}")
            raise

    def notify_plugin(self, status, flight_info, **context):
        """
        Notifies a plugin or external service about a flight status change.
        Customize this method to implement the desired notification functionality.
        Args:
            status (str): The flight status (e.g., "Delayed", "Cancelled")
            flight_info (dict): Information about the flight
            context (dict): The task context dictionary
        """
        # Modify this method to implement the desired notification functionality
        print(f"Flight {flight_info['flight_number']} is {status} for airport {flight_info['origin']}")
        self.trigger_jenkins_job(flight_info, **context)

    def trigger_jenkins_job(self, flight_info, **context):
        """
        Triggers a Jenkins job for the delayed flight.
        Args:
            flight_info (dict): Information about the delayed flight
            context (dict): The task context dictionary
        """
        if self.jenkins_job_name and self.jenkins_connection_id:
            jenkins_operator = JenkinsJobTriggerOperator(
                task_id='jenkins_trigger',
                job_name=self.jenkins_job_name,
                connection_id=self.jenkins_connection_id,
                parameters={
                    'flight_number': flight_info['flight_number'],
                    'origin': flight_info['origin'],
                    'destination': flight_info['destination'],
                    'departure_time': flight_info['departure_time'],
                    'airline': flight_info['airline']
                },
                **context
            )
            jenkins_operator.execute(context)

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

# Define the default arguments for the DAG
default_args = {
    'start_date': datetime(2023, 5, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Create the DAG
with DAG(
        'flight_checker',
        default_args=default_args,
        description='Flight Checker DAG',
        schedule_interval=timedelta(minutes=1),
        catchup=False
) as dag:
    # Instantiate the FlightChecker class
    flight_checker = FlightChecker()

    # Define the tasks
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

    update_config_file_task = PythonOperator(
        task_id='update_config_file_task',
        python_callable=flight_checker.update_config_file,
        op_kwargs={'flight_data': '{{ ti.xcom_pull(key="flight_data") }}'},
        dag=dag,
    )

    check_flight_status_task = PythonOperator(
        task_id='check_flight_status_task',
        python_callable=flight_checker.check_flight_status,
        dag=dag,
    )

    # Define the task dependencies
    load_flight_data_task >> analyze_delays_task >> update_config_file_task >> check_flight_status_task



