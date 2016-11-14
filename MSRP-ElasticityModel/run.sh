#--driver-class-path /Users/Subu/Desktop/Work/KaizenAnalytix/MSRPElasticityModel/lib/spark-csv_2.10-1.3.0.jar:/Users/Subu/Desktop/Work/KaizenAnalytix/MSRPElasticityModel/lib/commons-csv-1.2.jar \
#need to use the above if we don't want to use the --packages option

export CLASSPATH=
echo CLASSPATH:$CLASSPATH

spark-submit --class com.toyota.analytix.model.MSRPElasticityModelMain \
--master yarn-client --driver-memory 512m \
--conf spark.executor.memory=512m  \
--conf spark.driver.memory=512m \
--conf spark.executor.instances=3 \
--conf spark.executor.cores=1 \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--packages com.databricks:spark-csv_2.10:1.3.0 \
./target/MSRP-ElasticityModel-0.0.1-SNAPSHOT.jar 405016 MSRP_MRM irm irm_app_logic_dev \
/Users/Subu/Desktop/Work/KaizenAnalytix/MSRPElasticityModel/Query.properties


#spark-submit --class com.toyota.analytix.mrm.msrp.bayesian.BayesianModeler --master yarn-client \
#spark-submit --class com.toyota.analytix.model.MSRPElasticityModelMain --master local[4] \
#--num-executors 3 --driver-memory 512m \
#-executor-memory 512m --executor-cores 1 \
#--packages com.databricks:spark-csv_2.10:1.3.0 \
#./target/MSRP-ElasticityModel-0.0.1-SNAPSHOT.jar run.properties >1

#spark-submit --class com.toyota.analytix.model.MSRPElasticityModelMain \
#--master yarn-client --driver-memory=8g \
#--conf spark.executor.memory=48g  \
#--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
#--conf spark.driver.memory=8g \
#--conf spark.executor.instances=8 \
#--conf spark.executor.cores=8 \
#--conf spark.storage.memoryFraction=0.7  \
#--conf spark.sql.shuffle.partitions=700  \
#--driver-java-options "-Dlog4j.configuration=file:///irm/ihub/irmihubtxfer/inbound/misc/jars/application/mrm/log4j.properties"  \
#MSRP-ElasticityModel-0.0.1-SNAPSHOT.jar run.properties 
