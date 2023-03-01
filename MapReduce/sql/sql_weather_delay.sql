set hive.cli.print.header=true;

SELECT
    `Year`,
    avg((`WeatherDelay` / `ArrDelay`)*100) AS `average_weather_delay`
FROM
    airline_delay
GROUP BY
    `Year`
ORDER BY
    `Year`