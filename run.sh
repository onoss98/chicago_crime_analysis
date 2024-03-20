mvn clean package
#---task#1 scripts---
#bin/beast --conf spark.executor.memory=16g --conf spark.driver.memory=16g --class edu.ucr.cs.cs167.group24.DataPreparation target/Project24-1.0-SNAPSHOT.jar Chicago_Crimes_1k.csv.bz2
#bin/beast --conf spark.executor.memory=16g --conf spark.driver.memory=16g --class edu.ucr.cs.cs167.group24.DataPreparation target/Project24-1.0-SNAPSHOT.jar Chicago_Crimes_100k.csv.bz2
bin/beast --conf spark.executor.memory=16g --conf spark.driver.memory=16g --class edu.ucr.cs.cs167.group24.DataPreparation target/Project24-1.0-SNAPSHOT.jar Chicago_Crimes_10k.csv.bz2

#---task#2 scripts---
bin/beast --conf spark.executor.memory=16g --conf spark.driver.memory=16g --class edu.ucr.cs.cs167.group24.SpatialAnalysis target/Project24-1.0-SNAPSHOT.jar Chicago_Crimes_ZIP

#---task#3 scripts---
bin/beast --conf spark.executor.memory=16g --conf spark.driver.memory=16g --class edu.ucr.cs.cs167.group24.TemporalAnalysis target/Project24-1.0-SNAPSHOT.jar Chicago_Crimes_ZIP 01/01/2000 12/31/2023

#---task#4 scripts---
spark-submit --conf spark.executor.memory=16g --conf spark.driver.memory=16g --class edu.ucr.cs.cs167.group24.ArrestPrediction --master "local[*]" target/Project24-1.0-SNAPSHOT.jar Chicago_Crimes_ZIP


