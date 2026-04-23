-- 1) Daily flights and on-time percentage trend
SELECT
  flight_date,
  total_flights,
  on_time_percentage
FROM public_public.fct_flight_metrics
ORDER BY flight_date;

-- 2) Flight status distribution
SELECT
  flight_status,
  COUNT(*) AS flights
FROM public_public.fact_passenger_flights
GROUP BY flight_status
ORDER BY flights DESC;

-- 3) Top airports by departing passengers
SELECT
  departure_airport,
  COUNT(*) AS passenger_flights
FROM public_public.fact_passenger_flights
GROUP BY departure_airport
ORDER BY passenger_flights DESC
LIMIT 15;

-- 4) Passenger split by nationality (top 15)
SELECT
  nationality,
  COUNT(*) AS passenger_count
FROM public_public.fact_passenger_flights
GROUP BY nationality
ORDER BY passenger_count DESC
LIMIT 15;
