mvn clean package
#---task#1 scripts---
#bin/beast --class edu.ucr.cs.cs167.onoss001.PreprocessCSV --master "local[*]" --driver-memory 6g --executor-memory 6g target/onoss001_finalproject-1.0-SNAPSHOT.jar Chicago_Crimes_1k.csv.bz2
#bin/beast --class edu.ucr.cs.cs167.onoss001.PreprocessCSV --master "local[*]" --driver-memory 6g --executor-memory 6g target/onoss001_finalproject-1.0-SNAPSHOT.jar Chicago_Crimes_10k.csv.bz2
bin/beast --class edu.ucr.cs.cs167.onoss001.PreprocessCSV --master "local[*]" --driver-memory 6g --executor-memory 6g target/onoss001_finalproject-1.0-SNAPSHOT.jar Chicago_Crimes_100k.csv.bz2




