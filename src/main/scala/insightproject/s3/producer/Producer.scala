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
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")

    val producer = new KafkaProducer[String, Array[Byte]](props)

    for (obj <- listings){


      val key = obj.getKey()
      val TOPIC = "fromS3"
      if (key.endsWith("gkg.csv")){
        val s3object = s3Client.getObject(new GetObjectRequest(bucket, key))
        val content = s3object.getObjectContent()
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
}
