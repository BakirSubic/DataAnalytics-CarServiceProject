CREATE DATABASE CarAnalyticsDWH;

USE CarAnalyticsDWH;

CREATE TABLE archive_accidents_csv (
    ID VARCHAR(255) NOT NULL,
    Source VARCHAR(255),
    Severity INT,
    Start_Time DATETIME,
    End_Time DATETIME,
    Start_Lat DECIMAL(9, 6),
    Start_Lng DECIMAL(10, 6),
    End_Lat DECIMAL(9, 6),
    End_Lng DECIMAL(10, 6),
    Distance_mi DECIMAL(10, 2),
    Description TEXT,
    Street VARCHAR(255),
    City VARCHAR(100),
    County VARCHAR(100),
    State VARCHAR(50),
    Zipcode VARCHAR(20),
    Country VARCHAR(50),
    Timezone VARCHAR(100),
    Airport_Code VARCHAR(50),
    Weather_Timestamp DATETIME,
    Temperature_F DECIMAL(5, 2),
    Wind_Chill_F DECIMAL(5, 2),
    Humidity_pct DECIMAL(5, 2),
    Pressure_in DECIMAL(5, 2),
    Visibility_mi DECIMAL(5, 2),
    Wind_Direction VARCHAR(50),
    Wind_Speed_mph DECIMAL(6, 2),
    Precipitation_in DECIMAL(6, 2),
    Weather_Condition VARCHAR(255),
    Amenity BOOLEAN,
    Bump BOOLEAN,
    Crossing BOOLEAN,
    Give_Way BOOLEAN,
    Junction BOOLEAN,
    No_Exit BOOLEAN,
    Railway BOOLEAN,
    Roundabout BOOLEAN,
    Station BOOLEAN,
    Stop BOOLEAN,
    Traffic_Calming BOOLEAN,
    Traffic_Signal BOOLEAN,
    Turning_Loop BOOLEAN,
    Sunrise_Sunset VARCHAR(50),
    Civil_Twilight VARCHAR(50),
    Nautical_Twilight VARCHAR(50),
    Astronomical_Twilight VARCHAR(50),
    load_timestamp DATETIME NOT NULL,
    valid_from_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to_timestamp DATETIME NULL,
    PRIMARY KEY (ID, load_timestamp)
);

CREATE TABLE archive_customers (
    original_customer_id INT NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone_number VARCHAR(50),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    registration_date DATE,
    source_last_updated_timestamp DATETIME,
    load_timestamp DATETIME NOT NULL,
    valid_from_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to_timestamp DATETIME NULL,
    PRIMARY KEY (original_customer_id, load_timestamp)
);

CREATE TABLE archive_vehicles (
    original_vehicle_id INT NOT NULL,
    customer_id INT,
    VIN VARCHAR(17),
    make VARCHAR(100),
    model VARCHAR(100),
    year INT,
    mileage INT,
    fuel_type VARCHAR(50),
    transmission_type VARCHAR(50),
    source_last_updated_timestamp DATETIME,
    load_timestamp DATETIME NOT NULL,
    valid_from_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to_timestamp DATETIME NULL,
    PRIMARY KEY (original_vehicle_id, load_timestamp)
);

CREATE TABLE archive_employees (
    original_employee_id INT NOT NULL,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    role VARCHAR(100),
    hire_date DATE,
    source_last_updated_timestamp DATETIME,
    load_timestamp DATETIME NOT NULL,
    valid_from_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to_timestamp DATETIME NULL,
    PRIMARY KEY (original_employee_id, load_timestamp)
);

CREATE TABLE archive_parts_inventory (
    original_part_id INT NOT NULL,
    part_name VARCHAR(255),
    part_number VARCHAR(100),
    category VARCHAR(100),
    current_stock_quantity INT,
    unit_cost DECIMAL(10, 2),
    source_last_updated_timestamp DATETIME,
    load_timestamp DATETIME NOT NULL,
    valid_from_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to_timestamp DATETIME NULL,
    PRIMARY KEY (original_part_id, load_timestamp)
);

CREATE TABLE archive_service_appointments (
    original_appointment_id INT NOT NULL,
    vehicle_id INT,
    customer_id INT,
    appointment_date DATE,
    appointment_time TIME,
    service_type VARCHAR(255),
    status VARCHAR(50),
    technician_id INT,
    total_labor_hours DECIMAL(10, 2),
    total_parts_cost DECIMAL(10, 2),
    total_service_cost DECIMAL(10, 2),
    notes TEXT,
    source_created_at DATETIME,
    source_last_updated_timestamp DATETIME,
    load_timestamp DATETIME NOT NULL,
    valid_from_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to_timestamp DATETIME NULL,
    PRIMARY KEY (original_appointment_id, load_timestamp)
);

CREATE TABLE archive_service_details (
    original_service_detail_id INT NOT NULL,
    appointment_id INT,
    task_description VARCHAR(255),
    labor_hours DECIMAL(5, 2),
    part_id INT,
    quantity_used INT,
    unit_cost_at_time_of_service DECIMAL(10, 2),
    source_created_at DATETIME,
    source_last_updated_timestamp DATETIME,
    load_timestamp DATETIME NOT NULL,
    valid_from_timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to_timestamp DATETIME NULL,
    PRIMARY KEY (original_service_detail_id, load_timestamp)
);

CREATE TABLE dim_customer (
    customer_key INT AUTO_INCREMENT PRIMARY KEY,
    original_customer_id INT NOT NULL,
    customer_name VARCHAR(200),
    email VARCHAR(255),
    phone_number VARCHAR(50),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    registration_date DATE,
    UNIQUE INDEX idx_original_customer_id (original_customer_id)
);

CREATE TABLE dim_vehicle (
    vehicle_key INT AUTO_INCREMENT PRIMARY KEY,
    original_vehicle_id INT NOT NULL,
    VIN VARCHAR(17) NOT NULL,
    make VARCHAR(100),
    model VARCHAR(100),
    year INT,
    fuel_type VARCHAR(50),
    transmission_type VARCHAR(50),
    dim_valid_from DATETIME NULL COMMENT,
    dim_valid_to DATETIME NULL COMMENT,
    is_current BOOLEAN DEFAULT TRUE COMMENT,
    INDEX idx_dim_vehicle_orig_id_current (original_vehicle_id, is_current),
    INDEX idx_dim_vehicle_vin_current (VIN, is_current)
);

CREATE TABLE dim_employee (
    employee_key INT AUTO_INCREMENT PRIMARY KEY,
    original_employee_id INT NOT NULL,
    employee_name VARCHAR(200),
    role VARCHAR(100),
    hire_date DATE,
    dim_valid_from DATETIME NULL COMMENT,
    dim_valid_to DATETIME NULL COMMENT,
    is_current BOOLEAN DEFAULT TRUE COMMENT,
    INDEX idx_dim_employee_orig_id_current (original_employee_id, is_current)
);

CREATE TABLE dim_service_type (
    service_type_key INT AUTO_INCREMENT PRIMARY KEY,
    service_type_name VARCHAR(255) NOT NULL,
    UNIQUE INDEX idx_service_type_name (service_type_name)
);

CREATE TABLE dim_part (
    part_key INT AUTO_INCREMENT PRIMARY KEY,
    original_part_id INT NOT NULL,
    part_name VARCHAR(255) NOT NULL,
    part_number VARCHAR(100) NOT NULL,
    category VARCHAR(100),
    dim_valid_from DATETIME NULL COMMENT,
    dim_valid_to DATETIME NULL COMMENT,
    is_current BOOLEAN DEFAULT TRUE COMMENT,
    INDEX idx_dim_part_orig_id_current (original_part_id, is_current),
    INDEX idx_dim_part_part_num_current (part_number, is_current)
);

CREATE TABLE dim_date (
    date_key INT AUTO_INCREMENT PRIMARY KEY,
    full_date DATE,
    year INT,
    month INT,
    month_name VARCHAR(20),
    day INT,
    day_of_week VARCHAR(20),
    quarter INT,
    hour INT,
    UNIQUE INDEX idx_full_date_hour (full_date, hour)
);

CREATE TABLE dim_location (
    location_key INT AUTO_INCREMENT PRIMARY KEY,
    street VARCHAR(255),
    city VARCHAR(100),
    county VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    country VARCHAR(50),
    latitude DECIMAL(9, 6),
    longitude DECIMAL(9, 6),
    UNIQUE INDEX idx_lat_lng_city_state (latitude, longitude, city, state)
);

CREATE TABLE dim_weather (
    weather_key INT AUTO_INCREMENT PRIMARY KEY,
    weather_condition VARCHAR(255),
    temperature_f DECIMAL(5, 2),
    wind_chill_f DECIMAL(5, 2),
    humidity_percent DECIMAL(5, 2),
    pressure_in DECIMAL(5, 2),
    visibility_mi DECIMAL(5, 2),
    wind_direction VARCHAR(50),
    wind_speed_mph DECIMAL(5, 2),
    precipitation_in DECIMAL(5, 2),
    UNIQUE INDEX idx_weather_combo (weather_condition, temperature_f, wind_speed_mph, visibility_mi)
);

CREATE TABLE dim_road_features (
    road_features_key INT AUTO_INCREMENT PRIMARY KEY,
    has_amenity BOOLEAN,
    has_bump BOOLEAN,
    has_crossing BOOLEAN,
    has_give_way BOOLEAN,
    has_junction BOOLEAN,
    has_no_exit BOOLEAN,
    has_railway BOOLEAN,
    has_roundabout BOOLEAN,
    has_station BOOLEAN,
    has_stop BOOLEAN,
    has_traffic_calming BOOLEAN,
    has_traffic_signal BOOLEAN,
    is_turning_loop BOOLEAN,
    UNIQUE INDEX idx_road_features_combo (
        has_amenity, has_bump, has_crossing, has_give_way, has_junction,
        has_no_exit, has_railway, has_roundabout, has_station, has_stop,
        has_traffic_calming, has_traffic_signal, is_turning_loop
    )
);

CREATE TABLE dim_daylight (
    daylight_key INT AUTO_INCREMENT PRIMARY KEY,
    sunrise_sunset VARCHAR(50),
    civil_twilight VARCHAR(50),
    nautical_twilight VARCHAR(50),
    astronomical_twilight VARCHAR(50),
    UNIQUE INDEX idx_daylight_combo (sunrise_sunset, civil_twilight, nautical_twilight, astronomical_twilight)
);

CREATE TABLE fact_service_appointments (
    fact_service_appointment_key INT AUTO_INCREMENT PRIMARY KEY,
    original_appointment_id INT NOT NULL,
    date_key INT,
    customer_key INT,
    vehicle_key INT,
    employee_key INT,
    service_type_key INT,
    total_labor_hours DECIMAL(10, 2),
    total_parts_cost DECIMAL(10, 2),
    total_service_cost DECIMAL(10, 2),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (customer_key) REFERENCES dim_customer(customer_key),
    FOREIGN KEY (vehicle_key) REFERENCES dim_vehicle(vehicle_key),
    FOREIGN KEY (employee_key) REFERENCES dim_employee(employee_key),
    FOREIGN KEY (service_type_key) REFERENCES dim_service_type(service_type_key),
    UNIQUE INDEX idx_original_appointment_id (original_appointment_id)
);

CREATE TABLE fact_service_parts_usage (
    fact_service_parts_usage_key INT AUTO_INCREMENT PRIMARY KEY,
    original_service_detail_id INT NOT NULL,
    original_appointment_id INT NOT NULL,
    date_key INT,
    vehicle_key INT,
    part_key INT,
    quantity_used INT,
    unit_cost_at_time_of_service DECIMAL(10, 2),
    total_cost_for_part_line DECIMAL(10, 2),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (vehicle_key) REFERENCES dim_vehicle(vehicle_key),
    FOREIGN KEY (part_key) REFERENCES dim_part(part_key),
    UNIQUE INDEX idx_original_service_detail_id (original_service_detail_id)
);

CREATE TABLE fact_accidents (
    fact_accident_key INT AUTO_INCREMENT PRIMARY KEY,
    original_accident_id VARCHAR(255) NOT NULL,
    date_key INT,
    location_key INT,
    weather_key INT,
    road_features_key INT,
    daylight_key INT,
    severity INT,
    distance_mi DECIMAL(10, 2),
    duration_minutes DECIMAL(10, 2),
    description TEXT,
    source_system VARCHAR(50),
    FOREIGN KEY (date_key) REFERENCES dim_date(date_key),
    FOREIGN KEY (location_key) REFERENCES dim_location(location_key),
    FOREIGN KEY (weather_key) REFERENCES dim_weather(weather_key),
    FOREIGN KEY (road_features_key) REFERENCES dim_road_features(road_features_key),
    FOREIGN KEY (daylight_key) REFERENCES dim_daylight(daylight_key),
    INDEX idx_original_accident_id (original_accident_id)
);