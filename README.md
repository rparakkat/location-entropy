**Project to calculate location entropy using Spark for user location data.**

**Test with Real Data**

Real data used to test the code with user and location data from : https://snap.stanford.edu/data/loc-gowalla.html
_Cannot share the same via github due to size limitations (file size < 100 MB)_

**Usage**

Run:
 
    LocationEntropy "Location data file" "Schema (Header)"
  
    Location Data File should be tab separated text file with user and location column
    Header line should be tab separated list of column names with user and location column

_Example Usage with downloaded test data 
LocationEntropy "./loc-gowalla_totalCheckins.txt.gz" "user check-in-time latitude longitude location"_