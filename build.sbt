name := "DataPreparation_Spark"

version := "0.1"

scalaVersion := "2.11.0"

// https://mvnrepository.com/artifact/org.apache.bahir/spark-streaming-akka
libraryDependencies +="org.apache.spark" % "spark-sql_2.11" % "2.0.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.0.0"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.5.9"
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.0.0"
libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.0.1"

libraryDependencies +="org.iq80.leveldb" % "leveldb" % "0.7"
libraryDependencies +="org.fusesource.leveldbjni" % "leveldbjni-all" % "1.8"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
