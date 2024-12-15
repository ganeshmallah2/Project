
# Crash Analysis Application

## Overview
This application performs detailed analysis on crash-related data using Python and PySpark. The results for each analysis are stored in the `data/output` folder as specified. The application is designed to process multiple CSV files containing crash-related data and produce insights for further examination.

## Directory Structure
```
project_root
|-- data
|   |-- output
|       |-- result.txt
|   |-- input
|       |-- Charges_use.csv
|       |-- Damages_use.csv
|       |-- Endorse_use.csv
|       |-- Primary_Person_use.csv
|       |-- Restrict_use.csv
|       |-- Units_use.csv
|-- logs
|   |-- app.log
|-- src
|   |-- __init__.py
|   |-- app.py
|   |-- config.py
|   |-- Crash_analysis.py
|   |-- data_loader.py
|   |-- etl.py
|   |-- utils.py
|-- test
|   |-- test_Crash_analysis.py
|   |-- test_data_loader.py
|-- venv (virtual environment folder)
|-- .gitignore
|-- app.py
|-- README.md
```

## Analysis Performed
The application performs the following analyses:
```
|-- Analysis 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?
|-- Analysis 2: How many two wheelers are booked for crashes?
|-- Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
|-- Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run?
|-- Analysis 5: Which state has highest number of accidents in which females are not involved?
|-- Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
|-- Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
|-- Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
|-- Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
|-- Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)
```

## Getting Started

### Prerequisites
1. Python 3.8+
2. Apache Spark 3.4.2
3. java 1.8.0_25
   
## Virtual environment tools (e.g., `venv`)
Setup Instructions
1. Set up a virtual environment and activate it:
    ```
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```
3. Install dependencies:
    ```
    pip install -r requirements.txt
    ```
4. Run the application:
    ```
    spark-submit app.py
    ```

## Logging
Logs are stored in the `logs/app.log` file for debugging and monitoring purposes.

## Testing
Unit tests are included in the `tests` directory. To run tests:
```
pytest tests
```

## Contributing
1. Fork the repository.
2. Create a new branch for your feature:
    ```
    git checkout -b feature-name
    ```
3. Push the branch and create a pull request.
     ```
    git .add
    ```
4. Commit your changes:
    ```
    git commit -m "Crash Analysis"
    ```
    
## License
This project is not licensed under the MIT License. See the LICENSE file for details.
