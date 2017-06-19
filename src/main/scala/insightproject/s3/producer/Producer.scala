package insightproject.s3.producer

import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider

import scala.collection.JavaConversions._
import com.amazonaws.services.s3.model.{GetObjectRequest, ListObjectsRequest, S3ObjectSummary}
import java.io.BufferedReader
import java.io.InputStreamReader
import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.log4j.{Logger, PropertyConfigurator}


/**
  * Created by rfrigato on 6/11/17.
  */
object Producer {
  val logger = Logger.getLogger("ProducerGDELT")
  PropertyConfigurator.configure("log4j.properties")

  def taskFromS3(summaries: java.util.List[S3ObjectSummary],
                 kafkaProps: Properties, topic: String,
                 bucket: String, taskIndex: Int, numTasks: Int): Unit = {
    logger.info(s"Starting task $taskIndex/$numTasks on topic $topic")
    val producer = new KafkaProducer[String, Array[Byte]](kafkaProps)
    val s3Client = new AmazonS3Client(new EnvironmentVariableCredentialsProvider())
    val rnd = scala.util.Random
    for (i <- Range(start = taskIndex, end = summaries.length, step = numTasks)) {
      val key = summaries(i).getKey()
      retry(3) {
        if (key.endsWith("gkg.csv")) {
          val s3object = s3Client.getObject(new GetObjectRequest(bucket, key))
          val content = s3object.getObjectContent()
          val reader = new BufferedReader(new InputStreamReader(content))
          var line = reader.readLine()
          var lineCounter = 0
          // scalastyle:off
          while (line != null) {
            //scalastyle:on
            try {
              if (line.length > 0) {
                GdeltCsv2Avro.parse(line) match {
                  case Some(avroRecord) => {
                    producer.send(new ProducerRecord(topic, rnd.nextInt.toString, avroRecord))
                  }
                  case None => // we skip records without a URL
                }
              }
            } catch {
              case e: Exception => {
                logger.error(s"error on line $lineCounter at $key", e)
              }
            }
            lineCounter += 1
            line = reader.readLine()
          }
          logger.info(s"$key completed")
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val s3Client = new AmazonS3Client(new EnvironmentVariableCredentialsProvider())
    val BUCKET = "gdelt-open-data"
    val S3FOLDER = "v2/gkg"
    val listObjectsRequest = new ListObjectsRequest()
      .withBucketName(BUCKET)
      .withPrefix(S3FOLDER)
      .withMarker(S3FOLDER)
    var listing = s3Client.listObjects(listObjectsRequest)
    val summaries = listing.getObjectSummaries
    while (listing.isTruncated()) {
      listing = s3Client.listNextBatchOfObjects (listing)
      summaries.addAll (listing.getObjectSummaries())
    }
    val props = new Properties()
    props.put("bootstrap.servers", sys.env.getOrElse("BOOTSTRAP_SERVERS", "localhost:9092"))
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer")
    if (args.size == 0) {
      println(
        """Usage:
          |java -cp producerS3-assembly-1.0.jar insightproject.s3.producer.Producer \
          |<topic> <numTasks> <taskIndex(optional)>
          |
          |--------------------------------------------------------------------------
          |
          |args:
          |  topic: the Kafka topic used to publish messages
          |  numTasks: the number of parallel tasks
          |  taskIndex: if used, the program will be single threaded and will run this task number
        """.replace("\r", "").stripMargin)
    } else if (args.size == 2) {
      val topic = args(0)
      val numTasks = args(1).toInt
      val tasks = 0 until numTasks map(
        i => task({taskFromS3(summaries, props, topic, BUCKET, i, numTasks)})
        )
      tasks.foreach(_.join())
    } else {
      val topic = args(0)
      val numTasks = args(1).toInt
      val taskIndex = args(2).toInt
      assert(numTasks > taskIndex, "numTask is less than or equal than the index task")
      taskFromS3(summaries, props, topic, BUCKET, taskIndex, numTasks)
    }

  }
}
