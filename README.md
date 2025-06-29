# Automotive Service & Accident Data Warehouse

## Project Overview

This project provides a complete data analytics solution for a hypothetical automotive service center. It successfully integrates data from two primary sources: a large CSV file of US traffic accidents and a simulated operational database for a car service center.

The core of the project is a robust ETL (Extract, Transform, Load) pipeline that feeds a data warehouse built on a **galaxy schema**. This enables powerful analytics and reporting on key business areas like service trends, parts inventory, employee performance, and even potential correlations with broader accident data.

The system is fully automated, from generating sample source data to processing daily incremental updates, and culminates in a Power BI dashboard for visualization.

## Technology Stack

* **Programming Language:** Python 3.13.3
* **Data Manipulation:** Pandas
* **Database Interaction:** SQLAlchemy, `mysql-connector-python`
* **Databases:** MySQL (for both the operational `CarAnalyticsDB` and the `CarAnalyticsDWH`)
* **Data Generation:** Faker library for creating realistic source data
* **ETL Orchestration:** Windows Task Scheduler for daily automated runs
* **Visualization:** Microsoft Power BI
* **Environment Management:** `python-dotenv` for secure credential handling

## Data Sources

1.  **US Accidents Data:** A CSV file (`datasets/US_Accidents_March23.csv`) containing millions of records of traffic accidents across the United States. This provides a rich source for external analysis.  **Note:** Due to its size, the raw CSV dataset is not included in this repository and is listed in `.gitignore`. A placeholder or a subset of the data might be included for structural reference, or the data can be obtained from its original [source](https://www.kaggle.com/datasets/sobhanmoosavi/us-accidents?resource=download).
2.  **Simulated Car Service Database (`CarAnalyticsDB`):** A relational MySQL database that mimics a real-world transactional system (OLTP). It is populated with synthetic data for customers, vehicles, employees, parts, and service appointments using a dedicated Python script.

## Key Features

* **Dual Database Architecture:**
    * `CarAnalyticsDB`: A normalized OLTP database designed to simulate day-to-day business operations.
    ![caranalyticsdb.png](images/caranalyticsdb.png)
    * `CarAnalyticsDWH`: A dimensional data warehouse with a galaxy schema, optimized for analytical queries (OLAP).
    ![caranalyticsdb.png](images/caranalyticsdwh.png)

* **Comprehensive Galaxy Schema:** The DWH features a clean and intuitive galaxy schema. This includes fact tables for service appointments, parts usage, and accidents, linked to shared dimension tables like `dim_date`, `dim_customer`, `dim_vehicle`, and `dim_location`.

* **Robust ETL Pipelines:**
    * **Initial Load (`initial_load.py`):** A script designed for a one-time, full data migration from the source systems into the data warehouse.
    * **Incremental Load (`incremental_load.py`):** A sophisticated daily process that efficiently identifies and applies only the changes (new, updated, or deleted records) from the source database. This ensures the DWH stays current without needing a full refresh.

* **Slowly Changing Dimensions (SCD Type 2):** Implemented for key dimensions like `dim_customer`, `dim_vehicle`, `dim_employee`, and `dim_part`. This preserves historical data by creating new versions of records when changes occur, allowing for accurate point-in-time analysis.

* **Automated Data Simulation & ETL:**
    * The `source_data_generator.py` script can simulate a day's worth of new business activity (new customers, appointments, etc.).
    * Batch scripts (`setup.bat`, `run_incremental.bat`) streamline the setup and daily execution.
    * The entire incremental pipeline is automated to run daily at midnight using **Windows Task Scheduler**.

* **Data Validation:** A suite of unit tests (`dwh_population_test.py`) ensures data integrity, checks for null foreign keys, and verifies uniqueness constraints in the DWH after each load.

* **Interactive Dashboard:** A **Power BI** report is connected to the `CarAnalyticsDWH`, providing an interactive way to explore the data, visualize trends, and gain business insights.

## Setup and Execution Guide

Follow these steps to get the project up and running on your local machine.

### 1. Prerequisites

* Python (tested with 3.13.3)
* MySQL Server
* Microsoft Power BI Desktop (to view the report)

### 2. Clone the Repository
```bash
  git clone [https://github.com/BakirSubic/DataAnalytics-CarServiceProject.git](https://github.com/BakirSubic/DataAnalytics-CarServiceProject.git)
  cd DataAnalytics-CarServiceProject
```
### 3. Set Up the Environment
Create and activate a Python virtual environment.
```bash
  # Create the virtual environment
  python -m venv .venv

  # Activate it (Windows)
  .venv\Scripts\activate

  # Activate it (macOS/Linux)
  source .venv/bin/activate
```

### 4. Install Dependencies

After activating your virtual environment, install the required packages.

```bash
  pip install -r requirements.txt
```

### 5. Configure Your Enviroment
Create a .env file in the root directory of the project. This file will store your database credentials securely.

1. Copy the contents of .env.example to .env.

2. Fill in your actual connection details:

#### .env file
````bash
    SOURCE_DB_HOST=localhost
    SOURCE_DB_USER=your_user
    SOURCE_DB_PASSWORD=your_password
    SOURCE_DB_NAME=CarAnalyticsDB
    SOURCE_DB_PORT=3306

    DWH_DB_HOST=localhost   
    DWH_DB_USER=your_user
    DWH_DB_PASSWORD=your_password
    DWH_DB_NAME=CarAnalyticsDWH
    DWH_DB_PORT=3306
````
### 6. Configure Your Enviroment
Ensure your MySQL server is running. Then, using a tool like DBeaver, MySQL Workbench, or the MySQL CLI:
1. Create two databases: `CarAnalyticsDB` and `CarAnalyticsDWH`.

2. Run the following SQL scripts:
   * `sql/schema_operational.sql` → creates tables in `CarAnalyticsDB`
   * `sql/schema_dwh.sql` → creates the galaxy schema in `CarAnalyticsDWH`

### 6.5 Configure the Orchestration Scripts

For the automation to work outside of an IDE like PyCharm, the `.bat` orchestration scripts need to know exactly where to find the Python interpreter associated with your project's virtual environment.

**1. Find Your Python Interpreter Path**

   *   Open your project in PyCharm.
   *   Go to **File** -> **Settings**.
   *   In the Settings window, navigate to **Project: [Your Project Name]** -> **Python Interpreter**.
   *   The path to the interpreter is displayed at the top. It will look something like this:
       `C:\Users\YourName\PyCharmProjects\YourProject\.venv\Scripts\python.exe`
   *   **Copy this full path** to your clipboard.

**2. Edit the Batch Files**

   You will need to paste the copied path into two files: `run_once_setup.bat` and `run_daily_incremental.bat`.

   *   **Open `run_once_setup.bat`** in a text editor (like Notepad or VS Code).
   *   Find the line that starts with `SET PYTHON_EXE=`.
   *   Replace the placeholder path with the path you just copied. Ensure it is enclosed in double quotes.

     **Example:**
     ```batch
     REM --- Define the EXACT path to the Python executable from your PyCharm project's interpreter settings ---
     REM !! PASTE THE PATH YOU COPIED FROM PYCHARM SETTINGS BELOW !!
     SET PYTHON_EXE="C:\Users\Bakir Subic\PyCharmMiscProject\.venv\Scripts\python.exe"
     ```

   *   **Save and close** the file.

   *   **Open `run_daily_incremental.bat`** and repeat the exact same process: find the `SET PYTHON_EXE=` line and paste your correct path.

   *   **Save and close** the file.

Your orchestration scripts are now correctly configured and ready to be executed.

### 7. Run the One-Time Initial Setup
To populate the source database with synthetic data and perform a full initial ETL load:
````bash
  setup.bat
````
> ⚠️ **This script should only be run once. It will:**
> 
> - Generate initial source data  
> - Load that data into the data warehouse

### 8. Run a Daily Incremental Load
To simulate and process a day’s worth of new data:
````bash
  run_incremental.bat
````
This script does two things:

1. Adds new and updated records to the `CarAnalyticsDB` using `source_data_generator.py`

2. Runs `incremental_load.py` to apply only the new/changed data into `CarAnalyticsDWH`

> This process is fully automated via Windows Task Scheduler to run at midnight daily, but it can also be run manually at any time.

### 9. Automate the Daily Incremental Load

The final step is to use the Windows Task Scheduler to run your `run_daily_incremental.bat` script automatically every day. This completes the project's orchestration requirement.

**1. Open Task Scheduler**

   *   Press the **Windows Key**, type **Task Scheduler**, and open the application.

**2. Create a New Task**

   *   In the **Actions** pane on the right side, click **Create Basic Task...**.

   

**3. Name Your Task**

   *   **Name:** `Car Analytics Daily ETL`
   *   **Description:** `Runs the daily incremental ETL process for the car analytics data warehouse.`
   *   Click **Next**.

**4. Set the Trigger (The Schedule)**

   *   Choose **Daily** for when you want the task to start.
   *   Click **Next**.
   *   Set a time for the task to run. A common time for ETL jobs is late at night, such as **2:00:00 AM**.
   *   Ensure it is set to recur every **1** day.
   *   Click **Next**.

   

**5. Define the Action**

   *   Select **Start a program**.
   *   Click **Next**.
   *   In the "Program/script" field, click the **Browse...** button.
   *   Navigate to your project's root directory and select the **`run_daily_incremental.bat`** file.
   *   Click **Open**.

   

**6. Finalize the Task**

   *   Review the summary on the final screen to ensure everything is correct.
   *   Click **Finish**.