# Spark-Derived MapReduce of Automobile Post-Sales Report

This program obtains autombile post-sales data and performs a simple MapReduce to obtain the count of incidents associated with a car make and year.

The raw data contains the following fields:

| Car Details   | Date Type                                        |
| ------------- | ------------------------------------------------ |
| Incident_id   | INT                                              |
| incident_type | STRING (I: initial sale, A: accident, R: repair) |
| vin_number    | STRING                                           |
| make          | STRING (brand of the car)                        |
| model         | STRING (model of the car)                        |
| year          | STRING (year of the car make/model)              |
| Incident_date | DATE (date that the incident occurred)           |

Specific to the problem statement, only incident reports of type "I" are of interest.

The **main.py** file divides the MapReduce process into multiple steps:

1. Loading the input data from **data.csv** into a Spark RDD
2. Creating a paired RDD using the vin_number as a key, and a value list containing incident_type, make, and year.
3. Loading missing make and year fields using the vin_number as the key for reference.
4. Filtering out the incident_type to only contain category "I"
5. Creating a paired RDD using a combined make-year key and a count as value.
6. Reducing by key to sum of the incident counts for each make and year.



The code can be executed on terminal by switching to the project directly and simply typing:

​					`spark-submit --py-files main.py main.py`





