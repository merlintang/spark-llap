/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.llap

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.hive.{HiveMetastoreCatalog, HiveSessionStateBuilder}
import org.apache.spark.sql.internal._


class LlapSessionStateBuilder(sparkSession: SparkSession, parentState: Option[SessionState] = None)
  extends HiveSessionStateBuilder(sparkSession, parentState) with Logging {

  self =>
  /**
   * Create a [[LlapSessionCatalog]] for Llap related data processing
   */
  override protected lazy val catalog: LlapSessionCatalog = {
    val catalog = new LlapSessionCatalog(
      sparkSession.sharedState.externalCatalog.asInstanceOf[LlapExternalCatalog],
      session.sharedState.globalTempViewManager,
      new HiveMetastoreCatalog(session),
      sparkSession,
      resourceLoader,
      functionRegistry,
      conf,
      sqlParser,
      SessionState.newHadoopConf(session.sparkContext.hadoopConfiguration, conf)
    )
    parentState.foreach(_.catalog.copyStateTo(catalog))
    catalog
  }

  /**
   * SQL-specific key-value configurations.
   *
   * These either get cloned from a pre-existing instance or newly created. The conf is always
   * merged with its [[org.apache.spark.SparkConf]].
   */
  override  protected lazy val conf: SQLConf = {
    val conf = parentState.map(_.conf.clone()).getOrElse(new SQLConf)
    mergeSparkConf(conf, session.sparkContext.conf)
    conf.setConfString("spark.sql.hive.llap.user", this.getUserString())
    conf.setConfString("spark.sql.hive.llap.url", this.getConnectionUrl(sparkSession))
    conf
  }

  // ------------------------------------------------------
  //  Configuration for HDP Ranger with LLAP
  // ------------------------------------------------------
  val HIVESERVER2_JDBC_URL =
  SQLConf.buildConf("spark.sql.hive.hiveserver2.jdbc.url")
    .doc("HiveServer2 JDBC URL.")
    .stringConf
    .createWithDefault("")

  val HIVESERVER2_JDBC_URL_PRINCIPAL =
    SQLConf.buildConf("spark.sql.hive.hiveserver2.jdbc.url.principal")
      .doc("HiveServer2 JDBC Principal.")
      .stringConf
      .createWithDefault("")

  val HIVESERVER2_CREDENTIAL_ENABLED =
    SQLConf.buildConf("spark.yarn.security.credentials.hiveserver2.enabled")
      .doc("When true, HiveServer2 credential provider is enabled.")
      .booleanConf
      .createWithDefault(false)

  // ------------------------------------------------------
  //  Helper methods for HDP Ranger with LLAP
  // ------------------------------------------------------
  /**
   * @return User name for the STS connection
   */
  def getUserString(): String = {
    System.getProperty("user")
  }

  /**
   * Return connection URL (with replaced proxy user name if exists).
   */
  def getConnectionUrl(sparkSession: SparkSession): String = {
    var userString = getUserString()
    if (userString == null) {
      userString = ""
    }
    val urlString = getConnectionUrlFromConf(sparkSession)
    urlString.replace("${user}", userString)
  }

  /**
   * For the given HiveServer2 JDBC URLs, attach the postfix strings if needed.
   *
   * For kerberized clusters,
   *
   * 1. YARN cluster mode: ";auth=delegationToken"
   * 2. YARN client mode: ";principal=hive/_HOST@EXAMPLE.COM"
   *
   * Non-kerberied clusters,
   * 3. Use the given URLs.
   */
  private def getConnectionUrlFromConf(sparkSession: SparkSession): String = {
    if (!sparkSession.conf.contains(HIVESERVER2_JDBC_URL.key)) {
      throw new Exception("Spark conf does not contain config " + HIVESERVER2_JDBC_URL.key)
    }

    if (sparkSession.conf.get(HIVESERVER2_CREDENTIAL_ENABLED, false)) {
      // 1. YARN Cluster mode for kerberized clusters
      s"${sparkSession.conf.get(HIVESERVER2_JDBC_URL.key)};auth=delegationToken"
    } else if (sparkSession.sparkContext.conf.contains(HIVESERVER2_JDBC_URL_PRINCIPAL.key)) {
      // 2. YARN Client mode for kerberized clusters
      s"${sparkSession.conf.get(HIVESERVER2_JDBC_URL.key)};" +
        s"principal=${sparkSession.conf.get(HIVESERVER2_JDBC_URL_PRINCIPAL.key)}"
    } else {
      // 3. For non-kerberized cluster
      sparkSession.conf.get(HIVESERVER2_JDBC_URL.key)
    }
  }

}

