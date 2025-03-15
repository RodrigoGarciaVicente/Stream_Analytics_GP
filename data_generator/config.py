params = {
    "NUM_PASSENGERS": 1000,
    "NUM_DRIVERS": 200,
    "TRAFFIC_PEAK_HOURS": [(7, 9), (17, 19)],
    "REQUEST_PEAK_HOURS": [(8, 10), (18, 20)],
    "CANCELLATION_PROBABILITY": 0.001,
}

uber_types = {
    "UberX": 0.5,
    "UberBlack": 0.15,
    "UberXL": 0.15,
    "UberComfort": 0.1,
    "UberGreen": 0.1,
}

weather_request_multipliers = {
    "clear": 1.0,
    "rain": 1.3,
    "snow": 1.5,
    "fog": 1.2,
}

