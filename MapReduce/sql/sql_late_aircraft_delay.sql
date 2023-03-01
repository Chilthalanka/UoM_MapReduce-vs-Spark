set hive.cli.print.header=true;

SELECT
    `Year`,
    avg((`LateAircraftDelay` / `ArrDelay`)*100) AS `average_late_aircraft_delay`
FROM
    airline_delay
GROUP BY
    `Year`
ORDER BY
    `Year`