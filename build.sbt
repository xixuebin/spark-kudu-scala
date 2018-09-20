name := "spark-kudu-scala"

version := "0.1"

scalaVersion := "2.11.8"

resolvers += "mvnrepository" at "http://mvnrepository.com/"

scalacOptions += "-target:jvm-1.7"
javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

//libraryDependencies ++= Seq(
//  ("org.apache.spark" %% "spark-sql" % "2.1.1")
//    .exclude("commons-beanutils", "commons-beanutils")
//    .exclude("commons-beanutils", "commons-beanutils-core")
//    .exclude("commons-collections", "commons-collections")
//)

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-sql" % "2.1.1")
    .exclude("commons-beanutils", "commons-beanutils")
    .exclude("commons-beanutils", "commons-beanutils-core")
    .exclude("commons-collections", "commons-collections")
)

libraryDependencies += "org.apache.kudu" % "kudu-client" % "1.6.0"
libraryDependencies += "org.apache.kudu" %% "kudu-spark2" % "1.6.0"

assemblyMergeStrategy in assembly := {
  case PathList("org","aopalliance", xs @ _*) => MergeStrategy.last
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.last
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}