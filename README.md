
# Crash Analysis Application

## Overview
This application performs detailed analysis on crash-related data using Python and PySpark. The results for each analysis are stored in the `data/output` folder as specified. The application is designed to process multiple CSV files containing crash-related data and produce insights for further examination.

## Directory Structure
```
project_root
|-- data
|   |-- output
|   |-- result.txt
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

### 1. Analytics 1: Find the number of crashes (accidents) in which number of males killed are greater than 2?
### 2. Analysis 2: How many two wheelers are booked for crashes?
### 3. Analysis 3: Determine the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.
### 4. Analysis 4: Determine number of Vehicles with driver having valid licences involved in hit and run?
### 5. Analysis 5: Which state has highest number of accidents in which females are not involved?
### 6. Analysis 6: Which are the Top 3rd to 5th VEH_MAKE_IDs that contribute to a largest number of injuries including death
### 7. Analysis 7: For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
### 8. Analysis 8: Among the crashed cars, what are the Top 5 Zip Codes with highest number crashes with alcohols as the contributing factor to a crash (Use Driver Zip Code)
### 9. Analysis 9: Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4 and car avails Insurance
### 10. Analysis 10: Determine the Top 5 Vehicle Makes where drivers are charged with speeding related offences, has licensed Drivers, used top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of offences (to be deduced from the data)


## Getting Started

### Prerequisites
1. Python 3.8+
2. Apache Spark
3. Virtual environment tools (e.g., `venv`)

### Setup Instructions
1. Set up a virtual environment and activate it:
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```
3. Install dependencies:
    ```
    pip install -r requirements.txt
    ```
4. Run the application:
    ```
    spark-python app.py
    ```

## Logging
Logs are stored in the `logs/app.log` file for debugging and monitoring purposes.

## Testing
Unit tests are included in the `tests` directory. To run tests:
```bash
pytest tests
```

## Contributing
1. Fork the repository.
2. Create a new branch for your feature:
    ```bash
    git checkout -b feature-name
    ```
3. Commit your changes:
    ```bash
    git commit -m "Add feature description"
    ```
4. Push the branch and create a pull request.
     ```
    git push
    ```

## License
This project is licensed under the MIT License. See the LICENSE file for details.
