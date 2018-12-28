#!/bin/sh

#curl -L https://archive.org/download/nycTaxiTripData2013/faredata2013.zip/trip_fare_1.csv.zip -o ~/data/trip_fare_1.csv.zip
#unzip ~/data/trip_data_1.csv.zip

R -e 'x <- data.table::fread("~/data/trip_fare_1.csv")' -e 'data.table::fwrite(x, "~/data/trip_fare_1.tsv", sep = "\\t")'
