from pyspark.sql import SparkSession
spark = SparkSession \
  .builder \
  .appName("SPARK LLAP YARN Cluster Mode") \
  .enableHiveSupport() \
  .config("spark.sql.hive.llap", "true") \
  .config("spark.sql.hive.hiveserver2.url", "jdbc:hive2://ctr-e129-1487033772569-58245-01-000002.hwx.site:10500/;auth=delegationToken") \
  .config("spark.hadoop.hive.llap.daemon.service.hosts", "@llap0") \
  .config("spark.hadoop.hive.zookeeper.quorum", "ctr-e129-1487033772569-58245-01-000007.hwx.site:2181,ctr-e129-1487033772569-58245-01-000003.hwx.site:2181,ctr-e129-1487033772569-58245-01-000006.hwx.site:2181") \
  .getOrCreate()
spark.sql("show databases").show()
spark.sql("select * from spark_ranger_test.t_mask_and_filter").show()
spark.stop()
