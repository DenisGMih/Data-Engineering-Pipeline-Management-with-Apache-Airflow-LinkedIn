COPY {{ params.table_name }} (
	registration, car_make, car_model, 
	car_model_year, color, mileage, price, 
	transmission, fuel_type, condition, location
)
FROM {{ params.csv_path }}
WITH (FORMAT csv, HEADER true)
WHERE condition = {{ params.condition }};
