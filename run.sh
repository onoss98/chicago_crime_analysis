mvn clean package
#---task#1 scripts---
#bin/beast --conf spark.executor.memory=16g --conf spark.driver.memory=16g --class edu.ucr.cs.cs167.group24.DataPreparation target/amoha120_lab10-1.0-SNAPSHOT.jar Chicago_Crimes_1k.csv.bz2
#bin/beast --conf spark.executor.memory=16g --conf spark.driver.memory=16g --class edu.ucr.cs.cs167.group24.DataPreparation target/amoha120_lab10-1.0-SNAPSHOT.jar Chicago_Crimes_100k.csv.bz2
bin/beast --conf spark.executor.memory=16g --conf spark.driver.memory=16g --class edu.ucr.cs.cs167.group24.DataPreparation target/amoha120_lab10-1.0-SNAPSHOT.jar Chicago_Crimes_10k.csv.bz2

#---task#2 scripts---
bin/beast --conf spark.executor.memory=16g --conf spark.driver.memory=16g --class edu.ucr.cs.cs167.group24.SpatialAnalysis target/amoha120_lab10-1.0-SNAPSHOT.jar

#---task#3 scripts---

#---task#4 scripts---
spark-submit --class edu.ucr.cs.cs167.group24.ArrestPrediction --master "local[*]" target/amoha120_lab10-1.0-SNAPSHOT.jar

