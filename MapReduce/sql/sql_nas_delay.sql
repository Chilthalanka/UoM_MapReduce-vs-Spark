set hive.cli.print.header=true;

SELECT
    `Year`,
    avg((`NASDelay` / `ArrDelay`)*100) AS `average_nas_delay`
FROM
    airline_delay
GROUP BY
    `Year`
ORDER BY
    `Year`