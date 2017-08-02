#!/bin/bash
echo "DELETING FORECAST FOLDER..."
/home/ubuntu/hadoop/bin/hadoop fs -rmr forecast
echo "CREATING FORECAST FOLDER..."
/home/ubuntu/hadoop/bin/hadoop fs -mkdir forecast/input
echo "COPYING THE INPUT FILE FROM LOCAL TO HDFS..."
/home/ubuntu/hadoop/bin/hadoop fs -put /home/ubuntu/hadoop/forecast/sampledata.csv forecast/input
echo "CREATING JAVA CLASS FILES..."
javac -classpath /home/ubuntu/hadoop/hadoop-core-1.2.1.jar -d /home/ubuntu/hadoop/forecast /home/ubuntu/hadoop/forecast/MagnitudeRange.java
echo "CREATING JAR FILE..."
jar -cvf /home/ubuntu/hadoop/forecast/MagnitudeRange.jar -C /home/ubuntu/hadoop/forecast/ .
echo "STARTING MAP-REDUCE JOB..."
start=$(date +%s.%N);
/home/ubuntu/hadoop/bin/hadoop jar /home/ubuntu/hadoop/forecast/MagnitudeRange.jar MagnitudeRange forecast/input forecast/output
dur=$(echo "$(date +%s.%N) - $start" | bc);
echo "DELETING THE OUTPUT FILE ALREADY PRESENT IN LOCAL..."
rm /home/ubuntu/user_output/uop.csv
echo "GETTING THE OUTPUT FILE FROM HDFS TO LOCAL..."
/home/ubuntu/hadoop/bin/hadoop fs -getmerge forecast/output /home/ubuntu/user_output/uop.csv
echo "COPYING THE MERGED OUTPUT FILE BACK TO HDFS... THIS IS THE MERGED O/P FILES"
/home/ubuntu/hadoop/bin/hadoop fs -put /home/ubuntu/user_output/uop.csv forecast/output
echo "DISPLAYING THE PART FILE..."
/home/ubuntu/hadoop/bin/hadoop fs -cat forecast/output/uop.csv
echo "WRITING EXECUTION TIME TO FILE..."
echo $dur > /home/ubuntu/user_output/uop.txt
echo "JOB COMPLETE!"
printf "Execution time: %.6f seconds\n" $dur