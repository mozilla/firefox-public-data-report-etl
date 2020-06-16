# Firefox Public Data Report ETL
The [Firefox Public Data](https://data.firefox.com) project is a public facing website which tracks various metrics over time and helps the general public understand what kind of data is being tracked by Mozilla and how it is used.

This repository contains the code used to pull and process the data for the _Hardware_ section of the report.

The website itself is generated by the [Ensemble](https://github.com/mozilla/ensemble) and [Ensemble Transposer](https://github.com/mozilla/ensemble-transposer) repositories.

## Data
### Hardware report
[Hardware report job](public_data_report/hardware_report) uses data from [Main pings](https://firefox-source-docs.mozilla.org/toolkit/components/telemetry/data/main-ping.html), pulled from main ping BigQuery table.

It produces weekly aggregates organized by various dimensions, which are stored in BigQuery and exported to S3 where they can be consumed by Transposer.

## Development
### Testing
To run the tests, ensure you have Docker installed. First build the container using:
```shell script
make build
```
then run the tests with:
```shell script
make test
```