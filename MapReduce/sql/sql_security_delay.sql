set hive.cli.print.header=true;

SELECT
    `Year`,
    avg((`SecurityDelay` / `ArrDelay`)*100) AS `average_security_delay`
FROM
    airline_delay
GROUP BY
    `Year`
ORDER BY
    `Year`