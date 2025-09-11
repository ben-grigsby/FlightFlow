-- SELECT * FROM avstack.bronze_info
--     WHERE dept_airport = 'Kansai International';

SELECT DISTINCT created_at FROM avstack.silver_arr_info;
SELECT DISTINCT created_at FROM avstack.silver_dept_info;
SELECT DISTINCT created_at FROM avstack.silver_flight_info;

SELECT DISTINCT created_at FROM avstack.gold_dim_timezone_info;
SELECT distinct created_at FROM avstack.gold_dim_airline_info;
SELECT distinct created_at FROM avstack.gold_dim_airport_info;
SELECT distinct created_at FROM avstack.gold_dim_flight_info;
SELECT distinct created_at FROM avstack.gold_fact_arr_dept_table;
SELECT distinct created_at FROM avstack.gold_fact_arrivals_table;
SELECT distinct created_at FROM avstack.gold_fact_departures_table;


SELECT * FROM avstack.bronze_info;


SELECT * FROM avstack.gold_dim_airport_info