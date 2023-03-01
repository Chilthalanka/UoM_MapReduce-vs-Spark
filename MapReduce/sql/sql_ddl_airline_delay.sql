CREATE EXTERNAL TABLE airline_delay (
  `index` int,
  `Year` int,
  `Month` int,
  `DayofMonth` int,
  `DayOfWeek` int,
  `DepTime` int,
  `CRSDepTime` int,
  `ArrTime` int,
  `CRSArrTime` int,
  `UniqueCarrier` string,
  `FlightNum` int,
  `TailNum` string,
  `ActualElapsedTime` int,
  `CRSElapsedTime` int,
  `AirTime` int,
  `ArrDelay` int,
  `DepDelay` int,
  `Origin` string,
  `Dest` string,
  `Distance` int,
  `TaxiIn` int,
  `TaxiOut` int,
  `Cancelled` int,
  `CancellationCode` string,
  `Diverted` int,
  `CarrierDelay` int,
  `WeatherDelay` int,
  `NASDelay` int,
  `SecurityDelay` int,
  `LateAircraftDelay` int
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://airline-delay-analysis/input/'
TBLPROPERTIES ("skip.header.line.count"="1");
