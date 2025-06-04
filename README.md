# Data Analytics project CS 360: Automotive Service & Accident Data Warehouse

## Project Description
This project aims to build an analytics solution for a hypothetical automotive service center. It involves integrating data from two distinct sources: a CSV file containing US accident data and a simulated operational relational database for a car service center. The goal is to create a data warehouse with a star schema to enable reporting and analysis on service trends, parts usage, and potential correlations with accident data.

## Technology Stack
* **Programming Language:** Python 3.x
* **Data Manipulation:** Pandas, NumPy
* **Database Interaction:** SQLAlchemy, mysql-connector-python
* **Databases:** MySQL (for both operational source `CarAnalyticsDB` and data warehouse `CarAnalyticsDWH`)
* **Data Generation (Operational Source):** Faker library
* **Environment Management:** python-dotenv
* **Version Control:** Git & GitHub

## Data Sources
1.  **US Accidents Data:** A CSV file (`datasets/US_Accidents_March23.csv`) containing records of traffic accidents in the US. (This is one of the two distinct data sources as required). **Note:** Due to its size, the raw CSV dataset is not included in this repository and is listed in `.gitignore`. A placeholder or a subset of the data might be included for structural reference, or the data can be obtained from its original [source](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents?resource=download).
2.  **Simulated Operational Car Service Database (`CarAnalyticsDB`):** A MySQL database populated with synthetic data (customers, vehicles, employees, parts inventory, service appointments, service details) generated using `source_data_generator.py`. (This is the relational database source as required)

## Features Implemented (Initial Load Phase)
* Setup of two MySQL databases:
    * `CarAnalyticsDB`: Simulates an operational OLTP database for a car service center.
    * `CarAnalyticsDWH`: Serves as the data warehouse with a star schema for OLAP.
* **Star Schema Design:** A comprehensive star schema designed and implemented in `CarAnalyticsDWH`, including dimension tables for date, location, weather, road features, daylight (from CSV), customer, vehicle, employee, part, and service type (from operational DB), and fact tables for accidents, service appointments, and service parts usage.
* **Source Data Generation:** `source_data_generator.py` script to populate `CarAnalyticsDB` with realistic sample data.
* **Initial ETL Pipeline (`initial_load.py`):**
    * Extracts data from the US Accidents CSV file.
    * Extracts data from the operational `CarAnalyticsDB`.
    * Performs transformations (data cleaning, type conversion, derived attributes).
    * Loads data into all dimension and fact tables of the `CarAnalyticsDWH` star schema, handling initial population and shared dimension updates.
* **Secure Credential Management:** Database credentials managed via a `.env` file (not committed to the repository).

## Setup and How to Run

1.  **Prerequisites:**
    * Python 3.13.3
    * MySQL Server
2.  **Clone the Repository:**
    ```bash
    git clone https://github.com/BakirSubic/DataAnalytics-CarServiceProject.git
    cd DataAnalytics
    ```
3.  **Create and Activate Virtual Environment:**
    ```bash
    python -m venv .venv
    # On Windows
    .venv\Scripts\activate
    # On macOS/Linux
    source .venv/bin/activate
    ```
4.  **Install Dependencies:**
    ```bash
    pip install -r requirements.txt
    ```
5. **Database Setup:**

   * Ensure your MySQL server is running.

   * Create two empty databases (e.g., `CarAnalyticsDB` and `CarAnalyticsDWH`).

   * To set up the operational database schema, execute the DDL statements from `sql/schema_operational.sql` against `CarAnalyticsDB`.

   * To set up the data warehouse schema, execute the DDL statements from `sql/schema_dwh.sql` against `CarAnalyticsDWH`.

   * **Note:** The current SQL files (`sql/schema_operational.sql` and `sql/schema_dwh.sql`) are deprecated as they need to be updated once the database and DWH edits are finalized.

6. **Run the Data Generation for Operational DB (if empty):**
    ```bash
    python source_data_generator.py
    ```
7. **Run the Initial ETL Load to Populate DWH:**
    ```bash
    python initial_load.py
    ```

## Future Work (Next Steps)
* **Implement Incremental Load Logic:** Develop ETL processes to handle new and updated data from source systems to keep the DWH current.
* **Visualization and Reporting:** Connect a BI tool to `CarAnalyticsDWH` to create interactive reports and dashboards.
* **ETL Orchestration:** Implement a workflow orchestration tool to schedule and manage ETL pipeline runs.
* **Advanced Features:** Explore potential bonus features like CI/CD, notifications, or more advanced analytics.
* **Comprehensive Documentation:** Finalize technical documentation.