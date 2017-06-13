package insightproject.s3.producer

import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider
import scala.collection.JavaConversions._
import com.amazonaws.services.s3.model.ListObjectsRequest
import com.amazonaws.services.s3.model.GetObjectRequest
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.Properties
import org.apache.kafka.clients.producer._

/**
  * Created by rfrigato on 6/11/17.
  */
object Producer {
  def main(args: Array[String]): Unit = {
    val s3Client = new AmazonS3Client(new EnvironmentVariableCredentialsProvider())


    val bucket = "gdelt-open-data"
    val folderName = "v2/gkg"

    val listObjectsRequest = new ListObjectsRequest()
      .withBucketName(bucket)
      .withPrefix(folderName)
      .withMarker(folderName)

    val listings = s3Client.listObjects(listObjectsRequest).getObjectSummaries


    val  props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")

    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val obj = listings.tail.head

    val key = obj.getKey()
    val TOPIC = "fromS3"


    val s3object = s3Client.getObject(new GetObjectRequest(bucket, key));
    val content = s3object.getObjectContent()
    val reader = new BufferedReader(new InputStreamReader(content))
    var line = reader.readLine()
    while (line != null) {
      val record = new ProducerRecord(TOPIC, key, line)
      producer.send(record)
      line = reader.readLine()
    }
  }
}
