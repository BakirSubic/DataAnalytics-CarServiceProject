CREATE DATABASE CarAnalyticsDB;

USE CarAnalyticsDB;

CREATE TABLE Customers (
    customer_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE,
    phone_number VARCHAR(50),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    registration_date DATE NOT NULL,
    last_updated_timestamp DATETIME NOT NULL
);

CREATE TABLE Employees (
    employee_id INT AUTO_INCREMENT PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    role VARCHAR(100) NOT NULL,
    hire_date DATE NOT NULL,
    last_updated_timestamp DATETIME NOT NULL
);

CREATE TABLE PartsInventory (
    part_id INT AUTO_INCREMENT PRIMARY KEY,
    part_name VARCHAR(255) NOT NULL,
    part_number VARCHAR(100) UNIQUE NOT NULL,
    category VARCHAR(100),
    current_stock_quantity INT NOT NULL DEFAULT 0,
    unit_cost DECIMAL(10, 2) NOT NULL,
    last_updated_timestamp DATETIME NOT NULL
);

CREATE TABLE Vehicles (
    vehicle_id INT AUTO_INCREMENT PRIMARY KEY,
    customer_id INT NOT NULL,
    VIN VARCHAR(17) UNIQUE NOT NULL,
    make VARCHAR(100) NOT NULL,
    model VARCHAR(100) NOT NULL,
    year INT NOT NULL,
    mileage INT NOT NULL,
    fuel_type VARCHAR(50),
    transmission_type VARCHAR(50),
    last_updated_timestamp DATETIME NOT NULL,
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id)
);

CREATE TABLE ServiceAppointments (
    appointment_id INT AUTO_INCREMENT PRIMARY KEY,
    vehicle_id INT NOT NULL,
    customer_id INT NOT NULL,
    appointment_date DATE NOT NULL,
    appointment_time TIME,
    service_type VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL,
    technician_id INT,
    total_labor_hours DECIMAL(5, 2),
    total_parts_cost DECIMAL(10, 2),
    total_service_cost DECIMAL(10, 2),
    notes TEXT,
    created_at DATETIME NOT NULL,
    last_updated_timestamp DATETIME NOT NULL,
    FOREIGN KEY (vehicle_id) REFERENCES Vehicles(vehicle_id),
    FOREIGN KEY (customer_id) REFERENCES Customers(customer_id),
    FOREIGN KEY (technician_id) REFERENCES Employees(employee_id)
);

CREATE TABLE ServiceDetails (
    service_detail_id INT AUTO_INCREMENT PRIMARY KEY,
    appointment_id INT NOT NULL,
    task_description VARCHAR(255) NOT NULL,
    labor_hours DECIMAL(5, 2) NOT NULL,
    part_id INT,
    quantity_used INT,
    unit_cost_at_time_of_service DECIMAL(10, 2),
    created_at DATETIME NOT NULL,
    last_updated_timestamp DATETIME NOT NULL,
    FOREIGN KEY (appointment_id) REFERENCES ServiceAppointments(appointment_id),
    FOREIGN KEY (part_id) REFERENCES PartsInventory(part_id)
);