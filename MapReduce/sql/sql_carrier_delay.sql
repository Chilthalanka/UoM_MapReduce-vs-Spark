set hive.cli.print.header=true;

SELECT
    `Year`,
    avg((`CarrierDelay` / `ArrDelay`)*100) AS `average_carrier_delay`
FROM
    airline_delay
GROUP BY
    `Year`
ORDER BY
    `Year`