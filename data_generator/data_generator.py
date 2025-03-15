import json
import random
import uuid
from datetime import datetime, timedelta
from faker import Faker
import fastavro
from fastavro.schema import load_schema

fake = Faker("en_US")

passenger_schema_path = "data_generator\\schema\\passenger_request_schema.avsc"
driver_schema_path = "data_generator\\schema\\driver_updates.avsc"

passenger_request_schema = load_schema(passenger_schema_path)
driver_update_schema = load_schema(driver_schema_path)

# Configurable parameters (from provided config)
NUM_PASSENGERS = 200
NUM_DRIVERS = 100
TRAFFIC_PEAK_HOURS = [(7, 9), (17, 19)]
REQUEST_PEAK_HOURS = [(8, 10), (18, 20)]
CANCELLATION_PROBABILITY = 0.001

UBER_TYPES = ["UberX", "UberBlack", "UberXL", "UberComfort", "UberGreen"]
UBER_TYPE_PROBABILITIES = [0.5, 0.15, 0.15, 0.1, 0.1]

WEATHER_REQUEST_MULTIPLIERS = {
    "clear": 1.0,
    "rain": 1.3,
    "snow": 1.5,
    "fog": 1.2,
}

# Initialize list of drivers with random initial conditions
drivers = []
for i in range(NUM_DRIVERS):
    drivers.append({
        "driver_id": str(uuid.uuid4()),  # Unique driver ID
        "current_location": "New York City, " + fake.street_address(),
        "availability_status": "available",  # Available to take rides
        "uber_type": random.choices(UBER_TYPES, weights=UBER_TYPE_PROBABILITIES)[0],
        "weather_condition": random.choice(["clear", "rain", "snow", "fog"]),
        "last_request_time": datetime.utcnow(),
    })

def get_traffic_conditions(hour):
    for start, end in TRAFFIC_PEAK_HOURS:
        if start <= hour <= end:
            return random.choice(["heavy", "moderate"])
    return "light"

def is_peak_hour(hour, peak_hours):
    return any(start <= hour <= end for start, end in peak_hours)

def estimate_trip_duration(traffic_conditions):
    base_duration = random.randint(10, 30)  # Base trip time in minutes
    if traffic_conditions == "moderate":
        return base_duration + random.randint(5, 15)
    elif traffic_conditions == "heavy":
        return base_duration + random.randint(15, 30)
    return base_duration

def assign_driver_to_request():
    available_drivers = [driver for driver in drivers if driver["availability_status"] == "available"]
    if available_drivers:
        driver = random.choice(available_drivers)
        driver["availability_status"] = "in_progress"
        return driver
    return None

def complete_trip(driver, trip_duration):
    driver["availability_status"] = "available"
    driver["current_location"] = "New York City, " + fake.street_address()
    driver["last_request_time"] = datetime.utcnow() + timedelta(minutes=trip_duration)

def generate_passenger_request():
    hour = datetime.utcnow().hour
    traffic_conditions = get_traffic_conditions(hour)
    weather_condition = random.choice(["clear", "rain", "snow", "fog"])
    trip_duration = estimate_trip_duration(traffic_conditions)
    request_multiplier = WEATHER_REQUEST_MULTIPLIERS.get(weather_condition, 1.0)
    adjusted_probability = 0.9 if is_peak_hour(hour, REQUEST_PEAK_HOURS) else 0.8
    adjusted_probability = min(adjusted_probability * request_multiplier, 1.0)
    
    passenger_request = {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "passenger_id": str(uuid.uuid4()),
        "pickup_location": "New York City, " + fake.street_address(),
        "dropoff_location": "New York City, " + fake.street_address(),
        "request_status": random.choices(["requested", "cancelled"], weights=[adjusted_probability, 1 - adjusted_probability])[0],
        "surge_multiplier": round(random.uniform(1.5, 3) if is_peak_hour(hour, REQUEST_PEAK_HOURS) else random.uniform(1, 1.5), 1),
        "traffic_conditions": traffic_conditions,
        "weather_condition": weather_condition,
        "estimated_trip_duration": trip_duration,
        "estimated_arrival_time": (datetime.utcnow() + timedelta(minutes=trip_duration)).isoformat(),
    }
    
    driver = assign_driver_to_request()
    if driver:
        print(f"Driver {driver['driver_id']} assigned to request {passenger_request['event_id']}")
        complete_trip(driver, trip_duration)
    else:
        print(f"No available drivers for request {passenger_request['event_id']}")
    
    return passenger_request

def generate_driver_update():
    hour = datetime.utcnow().hour
    availability_status = random.choices(["available", "busy"], weights=[0.6, 0.4])[0]
    driver = random.choice(drivers)
    if availability_status == "available" and random.random() > CANCELLATION_PROBABILITY:
        driver["availability_status"] = "available"
    else:
        driver["availability_status"] = "cancelled"
    
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "driver_id": driver["driver_id"],
        "current_location": driver["current_location"],
        "availability_status": driver["availability_status"],
        "uber_type": driver["uber_type"],
        "weather_condition": driver["weather_condition"]
    }

def write_json_file(data, filename):
    # Debugging: print the first few items to check the data being written
    print(f"Writing {len(data)} records to {filename}")
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)

def write_avro_file(data, filename, schema):
    # Debugging: print the number of records being written
    print(f"Writing {len(data)} records to {filename}")
    with open(filename, "wb") as out:
        fastavro.writer(out, schema, data)

def generate_data_samples(num_passengers=NUM_PASSENGERS, num_drivers=NUM_DRIVERS):
    # Debugging: Check if data is being generated
    print(f"Generating {num_passengers} passenger requests and {num_drivers} driver updates...")
    passenger_requests = [generate_passenger_request() for _ in range(num_passengers)]
    driver_updates = [generate_driver_update() for _ in range(num_drivers)]
    
    # Check if lists are populated
    print(f"Generated {len(passenger_requests)} passenger requests and {len(driver_updates)} driver updates")
    
    write_json_file(passenger_requests, "data_generator\\data\\passenger_requests.json")
    write_json_file(driver_updates, "data_generator\\data\\driver_updates.json")
    
    write_avro_file(passenger_requests, "data_generator\\data\\passenger_requests.avro", passenger_request_schema)
    write_avro_file(driver_updates, "data_generator\\data\\driver_updates.avro", driver_update_schema)
    
    print("Data generation complete: JSON & AVRO files created.")

# Run the generator
generate_data_samples(NUM_PASSENGERS, NUM_DRIVERS)