#COMMANDS to run on a setup of HDFS on your machine.

> set CLASSPATH=C:\hadoop-2.8.0\share\hadoop\mapreduce\hadoop-mapreduce-client-core-2.8.0.jar;C:\hadoop-2.8.0\share\hadoop\mapreduce\hadoop-mapreduce-client-common-2.8.0.jar;C:\hadoop-2.8.0\share\hadoop\common\hadoop-common-2.8.0.jar;C:\CS236_MapReduce_Project\TotalRevenue\*;C:\hadoop-2.8.0\lib\*

> javac -d . TotalRevenueMapper.java TotalRevenueReducer.java TotalRevenueDriver.java

> jar cfm TotalRevenue.jar Manifest_final.txt TotalRevenue/*.class

> C:\hadoop-2.8.0\sbin\start-all.cmd

> C:\hadoop-2.8.0\bin\hdfs dfs -copyFromLocal 2017_18 /2017_18

> C:\hadoop-2.8.0\bin\hdfs dfs -ls /input

> C:\hadoop-2.8.0\bin\hadoop jar TotalRevenue.jar /input/hotel-booking.csv /input/customer-reservations.csv /merged_revenue_months19

> C:\hadoop-2.8.0\bin\hdfs dfs -cat /merged_revenue_months19/part-00000
> C:\hadoop-2.8.0\bin\hdfs dfs -cat /merged_revenue_months19/output.txt
