package insightproject.s3.producer

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider

import scala.collection.JavaConversions._
import com.amazonaws.services.s3.model.{GetObjectRequest, ListObjectsRequest, S3ObjectSummary}
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.Properties

import org.apache.kafka.clients.producer._


/**
  * Created by rfrigato on 6/11/17.
  */
object Producer {

  def taskFromS3(summaries: java.util.List[S3ObjectSummary],
                 kafkaProps: Properties,
                 bucket: String, taskIndex: Int, numTasks: Int): Unit = {
    for (i <- Range(start = taskIndex, end = summaries.length, step = numTasks)) {
      val key = summaries(i).getKey()
      val producer = new KafkaProducer[String, Array[Byte]](kafkaProps)
      val s3Client = new AmazonS3Client(new EnvironmentVariableCredentialsProvider())
      val TOPIC = "fromS3"
      if (key.endsWith("gkg.csv")) {
        val content = retry(3) {
          val s3object = s3Client.getObject(new GetObjectRequest(bucket, key))
          s3object.getObjectContent()
        }
        val reader = new BufferedReader(new InputStreamReader(content))
        var line = reader.readLine()
        while (line != null) {
          try {
            GdeltCsv2Avro.parse(line) match {
              case Some(avroRecord) => {
                producer.send(new ProducerRecord(TOPIC, key, avroRecord))
              }
              case None =>
            }
          } catch {
            case e: Exception => {
              print(key + " ")
              println(e)
            }
          }
          line = reader.readLine()
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val s3Client = new AmazonS3Client(new EnvironmentVariableCredentialsProvider())
    val bucket = "gdelt-open-data"
    val folderName = "v2/gkg"
    val listObjectsRequest = new ListObjectsRequest()
      .withBucketName(bucket)
      .withPrefix(folderName)
      .withMarker(folderName)
    var listing = s3Client.listObjects(listObjectsRequest)
    val summaries = listing.getObjectSummaries
    while (listing.isTruncated()) {
      listing = s3Client.listNextBatchOfObjects (listing)
      summaries.addAll (listing.getObjectSummaries())
    }
    val  props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    val numTasks = 5
    for (i <- 0 until numTasks) {
      task({
        taskFromS3(summaries, props, bucket, i, numTasks)
      })

    }
  }
}
