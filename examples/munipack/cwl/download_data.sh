#!/bin/sh
SCRIPT_DIRECTORY="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
DATA_DIRECTORY=${SCRIPT_DIRECTORY}/data

#Download and untar data
curl -O ftp://munipack.physics.muni.cz/pub/munipack/munipack-data-blazar.tar.gz
tar -xzf munipack-data-blazar.tar.gz

# Create directories
mkdir -p ${DATA_DIRECTORY}/dark10
mkdir -p ${DATA_DIRECTORY}/dark120
mkdir -p ${DATA_DIRECTORY}/flat
mkdir -p ${DATA_DIRECTORY}/main

# Move data
mv munipack-data-blazar/d10_*.fits ${DATA_DIRECTORY}/dark10
mv munipack-data-blazar/d120_*.fits ${DATA_DIRECTORY}/dark120
mv munipack-data-blazar/f10_*.fits ${DATA_DIRECTORY}/flat
mv munipack-data-blazar/0716_*.fits ${DATA_DIRECTORY}/main

# Remove unnecessary stuff
rm -f munipack-data-blazar.tar.gz
rm -rf munipack-data-blazar/
