import json
import random
import time
import uuid
from datetime import datetime, timedelta
import fastavro
from fastavro.schema import load_schema
from faker import Faker
import config as cfg

fake = Faker("en_US")

# Load AVRO schema (to be defined later)
data_schema_path = "data_generator\\schema\\ride_event_schema.avsc"
ride_event_schema = load_schema(data_schema_path)

# Configurable parameters
NUM_PASSENGERS = cfg.params["NUM_PASSENGERS"]
NUM_DRIVERS = cfg.params["NUM_DRIVERS"]
TRAFFIC_PEAK_HOURS = cfg.params["TRAFFIC_PEAK_HOURS"]
REQUEST_PEAK_HOURS = cfg.params["REQUEST_PEAK_HOURS"]
CANCELLATION_PROBABILITY = cfg.params["CANCELLATION_PROBABILITY"]

# Uber types with probabilities
UBER_TYPES = list(cfg.uber_types.keys())
UBER_TYPE_PROBABILITIES = list(cfg.uber_types.values())

# Weather impact on requests
WEATHER_REQUEST_MULTIPLIERS = cfg.weather_request_multipliers

# Traffic conditions based on peak hours
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

def generate_passenger_request():
    hour = datetime.utcnow().hour
    traffic_conditions = get_traffic_conditions(hour)
    weather_condition = random.choice(["clear", "rain", "snow", "fog"])
    trip_duration = estimate_trip_duration(traffic_conditions)
    request_multiplier = WEATHER_REQUEST_MULTIPLIERS.get(weather_condition, 1.0)
    request_probability = 0.9 if is_peak_hour(hour, REQUEST_PEAK_HOURS) else 0.8
    adjusted_probability = min(request_probability * request_multiplier, 1.0)
    
    return {
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
        "estimated_arrival_time": (datetime.utcnow() + timedelta(minutes=trip_duration)).isoformat()
    }

def generate_driver_update():
    hour = datetime.utcnow().hour
    availability_status = random.choices(["available", "busy"], weights=[0.6, 0.4])[0]
    accepted_request = availability_status == "available" and random.random() > CANCELLATION_PROBABILITY
    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "driver_id": str(uuid.uuid4()),
        "current_location": "New York City, " + fake.street_address(),
        "availability_status": "available" if accepted_request else "cancelled",
        "uber_type": random.choices(UBER_TYPES, weights=UBER_TYPE_PROBABILITIES)[0],
        "weather_condition": random.choice(["clear", "rain", "snow", "fog"])
    }

def write_json_file(data, filename):
    with open(filename, "w") as f:
        json.dump(data, f, indent=4)

def write_avro_file(data, filename, schema):
    with open(filename, "wb") as out:
        fastavro.writer(out, schema, data)

def generate_data_samples(num_passengers=NUM_PASSENGERS, num_drivers=NUM_DRIVERS):
    passenger_requests = [generate_passenger_request() for _ in range(int(num_passengers * random.uniform(1, WEATHER_REQUEST_MULTIPLIERS["rain"])))]
    driver_updates = [generate_driver_update() for _ in range(num_drivers)]
    
    write_json_file(passenger_requests, "data_generator\\data\\passenger_requests.json")
    write_json_file(driver_updates, "data_generator\\data\\driver_updates.json")
    
    write_avro_file(passenger_requests, "data_generator\\data\\passenger_requests.avro", ride_event_schema)
    write_avro_file(driver_updates, "data_generator\\data\\driver_updates.avro", ride_event_schema)
    
    print("Data generation complete: JSON & AVRO files created.")

# Run the generator
generate_data_samples(NUM_PASSENGERS, NUM_DRIVERS)
