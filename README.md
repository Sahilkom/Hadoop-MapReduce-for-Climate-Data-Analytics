# This is Big Data Analytics Project prepared and submitted by Sahil (107121086) EEE
This project was completed on linux Ubuntu. So please proceed accordingly.

# Instructions to Compile the JAVA files.
- First launch eclipse in a fresh new directory.
- Then make a new java project with the same name as java file.
- Now make a new class named same as the java file.
- Inside the class.java file copy and paste the code of file provided in the assignment.
- Now import all the libraries of hadoop form /hdoop/hadoop-3.2.3/share/hadoop/common & 
    /hdoop/hadoop-3.2.3/share/hadoop/mapreduce
- Now export the file as a jar file into hadoop-3.2.3/bin directory
- Start the hadoop nodes.
- first upload all the input files into hdfs using the following command:
    hdfs dfs -put /file_path_on_pc /hinput
- now run the jar file using the following command
    hadoop jar JAR_FILE_NAME CLASS_NAME /hinput/FILE_NAME /houtput/out1
- you can find the output file in /houtput/out1 in the hdfs file system.

# Instructions to Upload Dataset(Part A) on Hadoop input Directory
- Upload all the input files into hdfs using the following command:
    hdfs dfs -put /file_path_on_pc /hinput
