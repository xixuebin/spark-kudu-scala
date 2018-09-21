package com.kevin.scala


import java.net.InetAddress

import com.kevin.scala.ImportExportKudu.ArgsCls
import org.apache.kudu.client.KuduClient
import org.apache.spark.sql.{Column, SparkSession, functions}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.kudu.spark.kudu._


object ImportExportKudu {
  val LOG: Logger = LoggerFactory.getLogger(ImportExportKudu.getClass)

  def fail(msg: String): Nothing = {
    System.err.println(msg)
    sys.exit(1)
  }

  def defaultMasterAddrs: String = InetAddress.getLocalHost.getCanonicalHostName

  def usage: String =
    s"""
       | Usage: --operation=import/export --format=<data-format(csv,parquet,avro)> --master-addrs=<master-addrs> --path=<path> --table-name=<table-name>
       |    where
       |      operation: import or export data from or to Kudu tables, default: import
       |      format: specify the format of data want to import/export, the following formats are supported csv,parquet,avro default:csv
       |      masterAddrs: comma separated addresses of Kudu master nodes, default: $defaultMasterAddrs
       |      path: path to input or output for import/export operation, default: file://
       |      tableName: table name to import/export, default: ""
       |      columns: columns name for select statement on export from kudu table, default: *
       |      delimiter: delimiter for csv import/export, default: ,
       |      header: header for csv import/export, default:false
     """.stripMargin

  case class ArgsCls(
                      operation: String = "import",
                      format: String = "csv",
                      masterAddrs: String = defaultMasterAddrs,
                      path: String = "file://",
                      tableName: String = "",
                      columns: String = "*",
                      delimiter: String = ",",
                      header: String = "false",
                      inferschema: String = "false")

  object ArgsCls {
    private def parseInner(options: ArgsCls, args: List[String]): ArgsCls = {
      LOG.info(args.mkString(","))
      args match {
        case Nil => options
        case "--help" :: _ =>
          System.err.println(usage)
          sys.exit(0)
        case flag :: Nil => fail(s"flag $flag has no value\n$usage")
        case flag :: value :: tail =>
          val newOptions: ArgsCls = flag match {
            case "--operation" => options.copy(operation = value)
            case "--format" => options.copy(format = value)
            case "--master-addrs" => options.copy(masterAddrs = value)
            case "--path" => options.copy(path = value)
            case "--table-name" => options.copy(tableName = value)
            case "--columns" => options.copy(columns = value)
            case "--delimiter" => options.copy(delimiter = value)
            case "--header" => options.copy(header = value)
            case "--inferschema" => options.copy(inferschema = value)
            case _ => fail(s"unknown argument given $flag")
          }
          parseInner(newOptions, tail)
      }
    }

    def parse(args: Array[String]): ArgsCls = {
      parseInner(ArgsCls(), args.flatMap(_.split('=')).toList)
    }
  }
}

object ImportExportFiles {

  import ImportExportKudu.fail

  def  run(args: ArgsCls, ss: SparkSession): Unit = {
    val kc = new KuduContext(args.masterAddrs, ss.sparkContext)
    val sqlContext = ss.sqlContext

    val client: KuduClient = kc.syncClient
    if (!client.tableExists(args.tableName)) {
      fail(args.tableName + s" table doesn't exist")
    }

    val kuduOptions =
      Map("kudu.table" -> args.tableName, "kudu.master" -> args.masterAddrs)

    args.operation match {
      case "import" =>
        args.format match {
          case "csv" =>
            val df = sqlContext.read
              .option("header", args.header)
              .option("delimiter", args.delimiter)
              .csv(args.path)
            kc.upsertRows(df, args.tableName)
          case "parquet" =>
            var df = sqlContext.read.parquet(args.path.split(","):_*)
            System.out.println(df.count())
            System.out.println(df.schema.simpleString)
            if(df.schema.fieldNames contains "time_stamp"){
              df = df.withColumn("part", functions.date_format(
                functions.col("time_stamp"), "yyyy-MM-dd"))
            }
            df = df.repartition(new Column("state"))
            kc.upsertRows(df, args.tableName)
          case "avro" =>
            val df = sqlContext.read
              .format("com.databricks.spark.avro")
              .load(args.path)
            kc.upsertRows(df, args.tableName)
          case _ => fail(args.format + s"unknown argument given ")
        }
      case "export" =>
        val df = sqlContext.read.options(kuduOptions).kudu.select(args.columns)
        args.format match {
          case "csv" =>
            df.write
              .format("com.databricks.spark.csv")
              .option("header", args.header)
              .option("delimiter", args.delimiter)
              .save(args.path)
          case "parquet" =>
            df.write.parquet(args.path)
          case "avro" =>
            df.write.format("com.databricks.spark.avro").save(args.path)
          case _ => fail(args.format + s"unknown argument given  ")
        }
      case _ => fail(args.operation + s"unknown argument given ")
    }
  }


  def testMain(args: Array[String], ss: SparkSession): Unit = {
    run(ArgsCls.parse(args), ss)
  }

  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME", "hdfs")

    val sparkSession = SparkSession
      .builder.appName("Import or Export CSV files from/to Kudu")
      .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
      .config("spark.eventLog.enabled", true)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.parquet.binaryAsString", true)
      .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse")
      .config("spark.driver.memory", "1g")
      .config("spark.executor.memory", "2g")
      .config("HADOOP_USER_NAME", "hdfs")
      .config("spark.sql.shuffle.partitions", "30")
      .config("spark.default.parallelism", "20")
      .master("local[*]")
      .getOrCreate()
    testMain(args, sparkSession)
  }

}
