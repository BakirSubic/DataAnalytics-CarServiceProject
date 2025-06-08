import mysql.connector
from faker import Faker
from datetime import datetime
import random
import sys
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    'host': os.getenv('SOURCE_DB_HOST', 'localhost'),
    'user': os.getenv('SOURCE_DB_USER'),
    'password': os.getenv('SOURCE_DB_PASSWORD'),
    'database': os.getenv('SOURCE_DB_NAME', 'CarAnalyticsDB'),
    'port': int(os.getenv('SOURCE_DB_PORT', 3306))
}

fake = Faker()

REALISTIC_PART_NAMES = [
    "Oil Filter", "Air Filter", "Cabin Air Filter", "Fuel Filter", "Spark Plug",
    "Ignition Coil", "Battery", "Alternator", "Starter Motor", "Brake Pad Set - Front",
    "Brake Pad Set - Rear", "Brake Rotor - Front", "Brake Rotor - Rear", "Brake Caliper",
    "Brake Fluid", "Engine Oil - Synthetic 5W-30", "Engine Oil - Conventional 10W-40",
    "Coolant/Antifreeze", "Windshield Wiper Blade - Driver Side", "Windshield Wiper Blade - Passenger Side",
    "Headlight Bulb - Low Beam", "Headlight Bulb - High Beam", "Taillight Bulb", "Brake Light Bulb",
    "Turn Signal Bulb", "Serpentine Belt", "Timing Belt", "Water Pump", "Thermostat",
    "Radiator", "Radiator Hose - Upper", "Radiator Hose - Lower", "Oxygen Sensor",
    "Mass Air Flow Sensor (MAF)", "Throttle Body", "Fuel Injector", "Fuel Pump",
    "Power Steering Fluid", "Transmission Fluid - Automatic", "Transmission Fluid - Manual",
    "Shock Absorber - Front", "Shock Absorber - Rear", "Strut Assembly - Front",
    "Strut Assembly - Rear", "Control Arm - Upper", "Control Arm - Lower", "Ball Joint",
    "Tie Rod End - Inner", "Tie Rod End - Outer", "Wheel Bearing", "CV Axle Assembly",
    "Exhaust Manifold Gasket", "Valve Cover Gasket", "Oil Pan Gasket", "Muffler",
    "Catalytic Converter", "Tire - P215/65R17", "Tire - P225/60R18", "Engine Mount",
    "Transmission Mount", "Fuse Assortment Kit", "Relay - Multi-Purpose", "Power Window Switch",
    "Door Handle - Exterior", "Door Handle - Interior", "Side View Mirror Assembly", "Wheel Hub Assembly"
]

def get_db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        print("Successfully connected to the database.")
        return conn
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}", file=sys.stderr)
        return None


def generate_customer_data(num_customers):
    customers = []
    for _ in range(num_customers):
        registration_date = fake.date_between(start_date='-5y', end_date='today')
        customers.append({
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'email': fake.unique.email(),
            'phone_number': fake.phone_number(),
            'address': fake.street_address(),
            'city': fake.city(),
            'state': fake.state_abbr(),
            'zip_code': fake.postcode(),
            'registration_date': registration_date,
            'last_updated_timestamp': datetime.now()
        })
    return customers


def generate_employee_data(num_employees):
    employees = []
    roles = ['Technician', 'Service Advisor', 'Manager', 'Administrator']
    for _ in range(num_employees):
        hire_date = fake.date_between(start_date='-10y', end_date='-1y')
        employees.append({
            'first_name': fake.first_name(),
            'last_name': fake.last_name(),
            'role': random.choice(roles),
            'hire_date': hire_date,
            'last_updated_timestamp': datetime.now()
        })
    return employees


def generate_parts_inventory_data(num_parts):
    parts = []
    categories = ['Brakes', 'Engine', 'Filters', 'Tires', 'Suspension', 'Electrical', 'Fluids', 'Lighting', 'Exhaust']
    for _ in range(num_parts):
        part_name = random.choice(REALISTIC_PART_NAMES)

        parts.append({
            'part_name': part_name,
            'part_number': fake.unique.bothify(text='????-######'),
            'category': random.choice(categories),
            'current_stock_quantity': random.randint(10, 500),
            'unit_cost': round(random.uniform(5.0, 500.0), 2),
            'last_updated_timestamp': datetime.now()
        })
    return parts


def generate_vehicle_data(customer_ids, num_vehicles_per_customer_avg=1.5):
    vehicles = []
    makes = ['Toyota', 'Honda', 'Ford', 'Chevrolet', 'BMW', 'Mercedes-Benz', 'Audi', 'Nissan', 'Hyundai', 'Kia']
    models = ['Camry', 'Civic', 'F-150', 'Silverado', 'X5', 'C-Class', 'A4', 'Altima', 'Elantra', 'Sportage']
    fuel_types = ['Gasoline', 'Electric', 'Hybrid', 'Diesel']
    transmission_types = ['Automatic', 'Manual']

    for customer_id in customer_ids:
        num_vehicles = round(random.gauss(num_vehicles_per_customer_avg, 0.5))
        num_vehicles = max(1, num_vehicles)
        for _ in range(num_vehicles):
            vehicles.append({
                'customer_id': customer_id,
                'VIN': fake.unique.vin(),
                'make': random.choice(makes),
                'model': random.choice(models),
                'year': random.randint(1990, 2024),
                'mileage': random.randint(5000, 200000),
                'fuel_type': random.choice(fuel_types),
                'transmission_type': random.choice(transmission_types),
                'last_updated_timestamp': datetime.now()
            })
    return vehicles


def generate_service_appointment_data(vehicle_ids, customer_ids, technician_ids, num_appointments):
    appointments = []
    service_types = ['Oil Change', 'Tire Rotation', 'Brake Repair', 'Diagnostic', 'Engine Repair',
                     'Scheduled Maintenance']
    statuses = ['Scheduled', 'In Progress', 'Completed', 'Cancelled']

    for _ in range(num_appointments):
        vehicle_id = random.choice(vehicle_ids)
        customer_id = random.choice(customer_ids)
        technician_id = random.choice(technician_ids)
        appointment_date = fake.date_between(start_date='-3y', end_date='today')
        appointment_time = fake.time_object()
        total_labor_hours = round(random.uniform(0.5, 8.0), 1)
        total_parts_cost = round(random.uniform(0, 1000.0), 2)
        total_service_cost = round(total_labor_hours * 75 + total_parts_cost, 2)
        created_at = datetime.now()

        appointments.append({
            'vehicle_id': vehicle_id,
            'customer_id': customer_id,
            'appointment_date': appointment_date,
            'appointment_time': appointment_time,
            'service_type': random.choice(service_types),
            'status': random.choice(statuses),
            'technician_id': technician_id,
            'total_labor_hours': total_labor_hours,
            'total_parts_cost': total_parts_cost,
            'total_service_cost': total_service_cost,
            'notes': fake.sentence() if random.random() > 0.7 else None,
            'created_at': created_at,
            'last_updated_timestamp': created_at
        })
    return appointments


def generate_service_detail_data(appointment_ids, part_ids, num_details_per_appointment_avg=2):
    service_details = []
    task_descriptions = [
        'Oil and filter change', 'Tire pressure check', 'Brake pads replacement',
        'Engine diagnostic', 'Spark plug replacement', 'Fluid top-up', 'Battery check'
    ]
    for appointment_id in appointment_ids:
        num_details = random.randint(1, max(1, round(num_details_per_appointment_avg * 2)))
        for _ in range(num_details):
            labor_hours = round(random.uniform(0.2, 4.0), 1)
            part_id = random.choice(part_ids) if random.random() > 0.3 else None
            quantity_used = random.randint(1, 4) if part_id else None
            unit_cost_at_time_of_service = round(random.uniform(10.0, 200.0), 2) if part_id else None
            created_at = datetime.now()

            service_details.append({
                'appointment_id': appointment_id,
                'task_description': random.choice(task_descriptions),
                'labor_hours': labor_hours,
                'part_id': part_id,
                'quantity_used': quantity_used,
                'unit_cost_at_time_of_service': unit_cost_at_time_of_service,
                'created_at': created_at,
                'last_updated_timestamp': created_at
            })
    return service_details


def insert_data_into_table(conn, table_name, data):
    if not data:
        print(f"No data to insert into {table_name}.")
        return []

    cursor = conn.cursor()
    columns = ', '.join(data[0].keys())
    placeholders = ', '.join(['%s'] * len(data[0]))
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    inserted_ids = []
    try:
        records = [tuple(d.values()) for d in data]
        cursor.executemany(insert_query, records)
        conn.commit()
        print(f"Successfully inserted {len(data)} records into {table_name}.")

        pass

    except mysql.connector.Error as err:
        print(f"Error inserting into {table_name}: {err}", file=sys.stderr)
        conn.rollback()
    finally:
        cursor.close()
    return inserted_ids


def get_all_ids(conn, table_name, id_column_name):
    cursor = conn.cursor()
    query = f"SELECT {id_column_name} FROM {table_name}"
    try:
        cursor.execute(query)
        ids = [row[0] for row in cursor.fetchall()]
        return ids
    except mysql.connector.Error as err:
        print(f"Error fetching IDs from {table_name}: {err}", file=sys.stderr)
        return []
    finally:
        cursor.close()


def perform_initial_load(conn):
    print("\n--- Performing Initial Data Load ---")

    print("Generating Customers data...")
    customers_data = generate_customer_data(500)
    insert_data_into_table(conn, 'Customers', customers_data)
    customer_ids = get_all_ids(conn, 'Customers', 'customer_id')

    print("Generating Employees data...")
    employees_data = generate_employee_data(20)
    insert_data_into_table(conn, 'Employees', employees_data)
    employee_ids = get_all_ids(conn, 'Employees', 'employee_id')

    print("Generating PartsInventory data...")
    parts_data = generate_parts_inventory_data(150)
    insert_data_into_table(conn, 'PartsInventory', parts_data)
    part_ids = get_all_ids(conn, 'PartsInventory', 'part_id')

    if customer_ids:
        print("Generating Vehicles data...")
        vehicles_data = generate_vehicle_data(customer_ids, num_vehicles_per_customer_avg=1.5)
        insert_data_into_table(conn, 'Vehicles', vehicles_data)
        vehicle_ids = get_all_ids(conn, 'Vehicles', 'vehicle_id')
    else:
        print("No customers found, skipping Vehicle generation.")
        vehicle_ids = []

    if vehicle_ids and customer_ids and employee_ids:
        print("Generating ServiceAppointments data...")
        appointments_data = generate_service_appointment_data(vehicle_ids, customer_ids, employee_ids, 2000)
        insert_data_into_table(conn, 'ServiceAppointments', appointments_data)
        appointment_ids = get_all_ids(conn, 'ServiceAppointments', 'appointment_id')
    else:
        print("Missing IDs for ServiceAppointments, skipping generation.")
        appointment_ids = []

    if appointment_ids and part_ids:
        print("Generating ServiceDetails data...")
        service_details_data = generate_service_detail_data(appointment_ids, part_ids,
                                                            num_details_per_appointment_avg=2.5)
        insert_data_into_table(conn, 'ServiceDetails', service_details_data)
    else:
        print("Missing IDs for ServiceDetails, skipping generation.")

    print("\n--- Initial Data Load Complete ---")


def simulate_new_customer(conn):
    print("\nSimulating new customer...")
    customer_data = generate_customer_data(1)
    insert_data_into_table(conn, 'Customers', customer_data)
    print("New customer added.")


def simulate_new_service_appointment(conn):
    print("\nSimulating new service appointment...")
    customer_ids = get_all_ids(conn, 'Customers', 'customer_id')
    vehicle_ids = get_all_ids(conn, 'Vehicles', 'vehicle_id')
    employee_ids = get_all_ids(conn, 'Employees', 'employee_id')
    part_ids = get_all_ids(conn, 'PartsInventory', 'part_id')

    if not (customer_ids and vehicle_ids and employee_ids and part_ids):
        print("Cannot simulate new appointment: missing base data (customers, vehicles, employees, parts).")
        return

    appointments_data = generate_service_appointment_data(vehicle_ids, customer_ids, employee_ids, 1)
    insert_data_into_table(conn, 'ServiceAppointments', appointments_data)

    cursor = conn.cursor()
    cursor.execute("SELECT MAX(appointment_id) FROM ServiceAppointments;")
    new_appointment_id = cursor.fetchone()[0]
    cursor.close()

    if new_appointment_id:
        service_details_data = generate_service_detail_data([new_appointment_id], part_ids,
                                                            num_details_per_appointment_avg=2)
        insert_data_into_table(conn, 'ServiceDetails', service_details_data)
        print(f"New service appointment (ID: {new_appointment_id}) and details added.")


def simulate_update_vehicle_mileage(conn):
    print("\nSimulating vehicle mileage update...")
    vehicle_ids = get_all_ids(conn, 'Vehicles', 'vehicle_id')
    if not vehicle_ids:
        print("No vehicles to update.")
        return

    vehicle_id_to_update = random.choice(vehicle_ids)
    new_mileage_increase = random.randint(1000, 10000)

    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT mileage FROM Vehicles WHERE vehicle_id = %s;", (vehicle_id_to_update,))
        current_mileage = cursor.fetchone()[0]
        updated_mileage = current_mileage + new_mileage_increase

        update_query = """
                       UPDATE Vehicles
                       SET mileage                = %s, \
                           last_updated_timestamp = %s
                       WHERE vehicle_id = %s; \
                       """
        cursor.execute(update_query, (updated_mileage, datetime.now(), vehicle_id_to_update))
        conn.commit()
        print(f"Updated vehicle ID {vehicle_id_to_update}: new mileage {updated_mileage}.")
    except mysql.connector.Error as err:
        print(f"Error updating vehicle mileage: {err}", file=sys.stderr)
        conn.rollback()
    finally:
        cursor.close()


def simulate_cancel_appointment(conn):
    print("\nSimulating appointment cancellation...")
    appointment_ids = get_all_ids(conn, 'ServiceAppointments', 'appointment_id')
    if not appointment_ids:
        print("No appointments to cancel.")
        return

    appointment_to_cancel = random.choice(appointment_ids)

    cursor = conn.cursor()
    try:
        update_query = """
                       UPDATE ServiceAppointments
                       SET status                 = 'Cancelled', \
                           last_updated_timestamp = %s
                       WHERE appointment_id = %s; \
                       """
        cursor.execute(update_query, (datetime.now(), appointment_to_cancel))
        conn.commit()
        print(f"Appointment ID {appointment_to_cancel} status changed to 'Cancelled'.")
    except mysql.connector.Error as err:
        print(f"Error cancelling appointment: {err}", file=sys.stderr)
        conn.rollback()
    finally:
        cursor.close()


if __name__ == "__main__":
    conn = get_db_connection()
    if conn:
        if len(sys.argv) > 1 and sys.argv[1] == '--incremental':
            print("\n--- Running in INCREMENTAL mode: Simulating a small number of daily changes ---")
            simulate_new_customer(conn)
            simulate_new_service_appointment(conn)
            simulate_update_vehicle_mileage(conn)
            simulate_cancel_appointment(conn)
        else:
            print("\n--- Running in FULL SETUP mode: Performing initial data load ---")
            perform_initial_load(conn)

        conn.close()
        print("\nDatabase operations complete. Connection closed.")
    else:
        print("Failed to establish database connection. Script aborted.", file=sys.stderr)
        sys.exit(1)
