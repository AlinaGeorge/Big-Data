-- Load the dataset, assuming comma-separated values
weather_data = LOAD 'weather_data.txt' USING PigStorage(',') 
               AS (year:int, month:int, day:int, temperature:int);

-- Group data by year
grouped_data = GROUP weather_data BY year;

-- Calculate max temperature per year
max_temp_per_year = FOREACH grouped_data GENERATE
                    group AS year,
                    MAX(weather_data.temperature) AS max_temperature;

-- Store or dump the results
DUMP max_temp_per_year;
