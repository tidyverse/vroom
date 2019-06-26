#!/bin/bash

mkdir ~/data/
wget -O ~/data/trip_fare.7z https://archive.org/download/nycTaxiTripData2013/trip_fare.7z && \
  sudo apt install p7zip-full && \
  cd ~/data && \
  7z x trip_fare.7z &> data.out && \
  # fix trailing space in header for every file
  ls *trip_fare*.csv | xargs -P 16 sed -i '1 s/, /,/g'
