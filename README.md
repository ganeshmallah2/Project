
# Crash Analysis Application

## Overview
This application performs detailed analysis on crash-related data using Python and PySpark. The results for each analysis are stored in the `data/output` folder as specified. The application is designed to process multiple CSV files containing crash-related data and produce insights for further examination.

## Directory Structure
```
project_root
|-- data
|   |-- output
|   |-- results.csv
|   |-- raw
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
|-- tests
|   |-- __init__.py
|   |-- config.py
|   |-- test_Crash_analysis.py
|   |-- test_data_loader.py
|   |-- test_etl.py
|   |-- test_utils.py
|-- venv (virtual environment folder)
|-- .gitignore
|-- Crash_analysis.ipynb
|-- README.md
```

## Analysis Performed
The application performs the following analyses:

### 1. Number of crashes with more than 2 male fatalities
- **Description**: Counts crashes where the number of males killed exceeds 2.
- **Output File**: `data/output/analysis_1.csv`

### 2. Two-wheelers booked for crashes
- **Description**: Determines how many two-wheelers were involved in crashes.
- **Output File**: `data/output/analysis_2.csv`

### 3. Top 5 vehicle makes where airbags didn’t deploy, and the driver died
- **Description**: Identifies top vehicle makes in accidents with non-deployed airbags and driver fatalities.
- **Output File**: `data/output/analysis_3.csv`

### 4. Vehicles with valid licenses in hit-and-run cases
- **Description**: Counts vehicles where the driver had a valid license and was involved in hit-and-run incidents.
- **Output File**: `data/output/analysis_4.csv`

### 5. State with the highest number of accidents not involving females
- **Description**: Finds the state with the highest crashes where no females were involved.
- **Output File**: `data/output/analysis_5.csv`

### 6. 3rd to 5th top vehicle makes contributing to injuries and deaths
- **Description**: Lists the 3rd to 5th top vehicle makes contributing to the highest injuries and deaths.
- **Output File**: `data/output/analysis_6.csv`

### 7. Top ethnic group for each body style involved in crashes
- **Description**: Determines the most frequent ethnic group for each unique vehicle body style.
- **Output File**: `data/output/analysis_7.csv`

### 8. Top 5 ZIP codes with alcohol-related crashes
- **Description**: Identifies ZIP codes with the highest number of crashes where alcohol was a contributing factor.
- **Output File**: `data/output/analysis_8.csv`

### 9. Crashes with no damaged property and high damage levels
- **Description**: Counts distinct crashes with no observed property damage, a damage level above 4, and insured vehicles.
- **Output File**: `data/output/analysis_9.csv`

### 10. Top 5 vehicle makes with speeding offences
- **Description**: Finds the top 5 vehicle makes where drivers:
  - Were charged with speeding-related offenses.
  - Had licensed drivers.
  - Used one of the top 10 vehicle colors.
  - Were registered in the top 25 states with the highest offenses.
- **Output File**: `data/output/analysis_10.csv`

## Getting Started

### Prerequisites
1. Python 3.8+
2. Apache Spark
3. Virtual environment tools (e.g., `venv` or `conda`)

### Setup Instructions
1. Clone the repository:
    ```bash
    git clone https://github.com/your-repo/crash-analysis.git
    cd crash-analysis
    ```
2. Set up a virtual environment and activate it:
    ```bash
    python3 -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```
3. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
4. Run the application:
    ```bash
    python src/app.py
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

## License
This project is licensed under the MIT License. See the LICENSE file for details.
