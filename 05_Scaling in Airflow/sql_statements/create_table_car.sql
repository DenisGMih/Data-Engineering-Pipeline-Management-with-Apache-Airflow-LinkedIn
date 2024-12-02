CREATE TABLE IF NOT EXISTS {{ params.table_name }} (
    registration VARCHAR(10),
    car_make VARCHAR(50),
    car_model VARCHAR(50),
    car_model_year INT,
    color VARCHAR(50),
    mileage INT,
    price DECIMAL(10, 2),
    transmission VARCHAR(20),
    fuel_type VARCHAR(20),
    condition VARCHAR(50),
    location VARCHAR(50)
);
