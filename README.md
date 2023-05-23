```mermaid
graph TD
    A(Load Flight Data Task) --> B(Analyze Delays Task)
    B --> C{Delay Threshold Met?}
    C -->|Yes| D(Notify Plugin Task)
    C -->|No| E(Skip Notification Task)
    style A fill:#FFD700, stroke:#000, stroke-width:2px
    style B fill:#87CEEB, stroke:#000, stroke-width:2px
    style C fill:#FFA500, stroke:#000, stroke-width:2px
    style D fill:#90EE90, stroke:#000, stroke-width:2px
    style E fill:#FF0000, stroke:#000, stroke-width:2px
```





Flight Checker DAG
==================

This is a DAG (Directed Acyclic Graph) implemented using Airflow. The purpose of this DAG is to periodically check for flight delays and perform appropriate actions based on the delay status.

Workflow Overview
-----------------

The workflow consists of two tasks:

1.  **load\_flight\_data\_task**: This task retrieves flight data from an API and stores it in XCom.
2.  **analyze\_delays\_task**: This task analyzes the flight delays based on the retrieved data and performs necessary actions.

The workflow is scheduled to run every minute, allowing for near real-time monitoring of flight delays.

FlightChecker Class
-------------------

The `FlightChecker` class encapsulates the logic for loading flight data, analyzing delays, and notifying external services about flight status changes. Let's go through the key components of this class.

### Initialization

The class is initialized with the required environment variables and sets up various configuration parameters. These include:

*   `api_key`: The API key used to access the flights data API.
*   `airports`: The IATA codes of the airports to monitor for flight delays.
*   `airlines`: The airlines to filter the flight data. If set to 'all', no airline filter is applied.
*   `check_interval`: The interval (in seconds) between each check for flight delays.
*   `delay_threshold`: The minimum delay duration (in minutes) for a flight to be considered delayed.
*   `time_to_departure_threshold`: The minimum time (in hours) before departure for a flight to be considered for delay analysis.
*   `cancelled_flight_time_window_start` and `cancelled_flight_time_window_end`: The time window (in minutes) after a delay for a cancelled flight to be acknowledged.
*   `api_host` and `api_endpoint`: The URL endpoints for the flights data API.
*   `airport_hours`: A dictionary mapping airport codes to tuples of opening and closing hours.
*   `last_delay_print_time`: A dictionary to keep track of the last delay print time for each airport.

### Environment Variable Validation

The `validate_environment_variables` method ensures that all required environment variables are present and have valid values. If any of the variables are missing or empty, a `ValueError` is raised.

### Airport Hours Parsing

The `parse_airport_hours` method parses the environment variable string for airport hours. The format of the string is `AIRPORT1:OPEN1-CLOSE1,AIRPORT2:OPEN2-CLOSE2,...`. For example, `BCN:6-23,AMS:3-23` represents Barcelona airport opening from 6 AM to 11 PM and Amsterdam airport opening from 3 AM to 11 PM. The method extracts the opening and closing hours for each airport and stores them in a dictionary.

### Loading Flight Data

The `load_flight_data` method retrieves flight data from the API and stores it in XCom. It makes an API request using the configured API key and airport codes. The method handles potential API request failures by retrying with exponential backoff. If the maximum number of retries is exceeded, a `RuntimeError` is raised.

### Analyzing Delays

The `analyze_delays` method analyzes flight delays for each airport based on the retrieved flight data. It filters flights based on the configured airlines and checks for delays that meet the delay threshold and time to departure threshold criteria. The method also handles cancelled flights within a specific time window after a delay is acknowledged. When a delay or cancellation is detected, the method calls the `notify_plugin` method to notify external services (customizable implementation).

### Notifying Plugin

The `notify_plugin` method is a placeholder for notifying a plugin or external service about flight status changes. The method is currently implemented to log the notification details. This implementation can be customized to integrate with specific notification services.

DAG Definition
--------------

The DAG definition follows the Airflow DAG structure. Here's an overview of the main components:

### Default Arguments

The `default_args` dictionary specifies the default arguments for the DAG. It includes the start date, the number of retries for each task, and the retry delay.

### DAG Initialization

The `DAG` class is used to define the DAG. The DAG is named "flight\_checker" and has a description. The schedule interval is set to run the DAG every minute. The `catchup` parameter is set to False to prevent backfilling for missed intervals.

### Task Definition

*   **load\_flight\_data\_task**: This task is defined using the `PythonOperator`. It is associated with the `load_flight_data` method of the `flight_checker` instance. The `provide_context=True` parameter ensures that the execution context is passed to the method, allowing access to XCom. This task is responsible for loading flight data from the API and storing it in XCom.
    
*   **analyze\_delays\_task**: This task is defined using the `PythonOperator`. It is associated with the `analyze_delays` method of the `flight_checker` instance. The `provide_context=True` parameter ensures that the execution context is passed to the method, allowing access to XCom. This task analyzes the flight delays based on the retrieved data and performs appropriate actions.
    

### Task Dependencies

The `>>` operator is used to define the dependency between tasks. In this case, `load_flight_data_task` is set as the upstream task for `analyze_delays_task`. This means that `analyze_delays_task` will only execute if `load_flight_data_task` completes successfully.

Summary
-------

The Flight Checker DAG periodically loads flight data, analyzes delays for specified airports and airlines, and performs actions based on the delay status. It utilizes the FlightChecker class to encapsulate the logic and interacts with external services through the `notify_plugin` method. The DAG is designed to run every minute, providing near real-time monitoring of flight delays.
