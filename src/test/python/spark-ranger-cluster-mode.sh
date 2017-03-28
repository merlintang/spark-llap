kdestroy; kinit -kt /etc/security/keytabs/hive.service.keytab hive/`hostname`@EXAMPLE.COM
~/2.6-maint/bin/spark-submit --packages com.hortonworks.spark:spark-llap-assembly_2.11:1.0.7-2.1.0.2.6.0.2-54 --repositories http://nexus-private.hortonworks.com/nexus/content/groups/public --conf spark.yarn.security.credentials.hiveserver2.enabled=true --master yarn --deploy-mode cluster cluster_mode.py

kdestroy; kinit -kt /etc/security/keytabs/spark.headless.keytab spark@EXAMPLE.COM
~/2.6-maint/bin/spark-submit --packages com.hortonworks.spark:spark-llap-assembly_2.11:1.0.7-2.1.0.2.6.0.2-54 --repositories http://nexus-private.hortonworks.com/nexus/content/groups/public --conf spark.yarn.security.credentials.hiveserver2.enabled=true --master yarn --deploy-mode cluster cluster_mode.py

# The following should raise exception.
# org.apache.hadoop.hive.ql.security.authorization.plugin.HiveAccessControlException: Permission denied: user [ambari-qa] does not have [USE] privilege on [null]
kdestroy; kinit -kt /etc/security/keytabs/smokeuser.headless.keytab ambari-qa-cl1@EXAMPLE.COM
~/2.6-maint/bin/spark-submit --packages com.hortonworks.spark:spark-llap-assembly_2.11:1.0.7-2.1.0.2.6.0.2-54 --repositories http://nexus-private.hortonworks.com/nexus/content/groups/public --conf spark.yarn.security.credentials.hiveserver2.enabled=true --master yarn --deploy-mode cluster cluster_mode.py
