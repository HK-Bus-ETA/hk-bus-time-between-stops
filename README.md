# hk-bus-time-between-stops
 
This repository hosts an **experimental attempt** to fetch and calculate the journey times between bus stops and MTR/Light Rail stations where services are available in Hong Kong.

It essentially works by looking at the distance between stops/stations and the ETA difference between them on services the stops at them consecutively. Values are averaged out over time in hopes of getting more and more accurate.

The code is being executed on an external server 24/7 and its results are pushed to the [`pages`](https://github.com/LOOHP/hk-bus-time-between-stops/tree/pages) branch at minute 0 every hour. The files are indexed with the first 2 characters (or the first character if there isn't a 2nd one) to strike a balance between not having one big data file and not having a few thousand small files.

Example request: https://timeinterval.hkbuseta.com/times/07.json

## Disclaimer
As this is an **experimental attempt**, please **expect inaccuracies**, especially to highly frequent & busy services.

## Credits
Many thanks to `HK Bus Crawling@2021, https://github.com/hkbus/hk-bus-crawling`
