一个单独的assembly包,用于使用本地模式的spark导入数据到kudu中.

打包方法:
sbt assembly


运行方法:

```sh
java -jar spark-kudu-scala-assembly-0.1.jar --operation=import --format=parquet  --master-addrs=master-hostname:7051 --path hdfs://hdfs-hostname:8022/user/hdfs/2018-09-15/fd4e2177a2bbc7ea-31fc928edb2b6cad_804313574_data.5.parq --table-name=impala::impala_kudu.test
```

参考:
- https://stackoverflow.com/questions/13116075/unresolved-dependency-org-scala-tools-sbinarysbinary-2-9-00-4-0-not-found
- https://blog.csdn.net/silentwolfyh/article/details/80820511
- http://mvnrepository.com/artifact/org.apache.spark/spark-core_2.11/2.1.1
- https://qiita.com/AKB428/items/fb019a2061c664d0e660
- http://queirozf.com/entries/creating-scala-fat-jars-for-spark-on-sbt-with-sbt-assembly-plugin

分区
https://stackoverflow.com/questions/30995699/how-to-define-partitioning-of-dataframe