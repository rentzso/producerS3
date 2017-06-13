name := "producerS3"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.9.6"
libraryDependencies += "org.apache.kafka" % "kafka_2.11" % "0.10.2.1"
libraryDependencies += "joda-time" % "joda-time" % "2.8.1"
libraryDependencies += "org.apache.avro" % "avro" % "1.8.2"
libraryDependencies += "com.twitter" % "bijection-avro_2.11" % "0.9.5"

mainClass in assembly := Some("insightproject.s3.producer.Producer")
