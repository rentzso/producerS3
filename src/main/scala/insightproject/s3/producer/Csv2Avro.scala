package insightproject.s3.producer
/**
  * Created by rfrigato on 6/13/17.
  */
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import java.io.File
import collection.JavaConverters._

/**
  * Defines the methods used to parse the files of the GDELT dataset
  */
object GdeltCsv2Avro {

  /**
    * Map that returns the positions in the csv of the various fields
    */
  val indexOf =  Array(
    "GKGRECORDID",
    "V2.1DATE",
    "V2SOURCECOLLECTIONIDENTIFIER",
    "V2SOURCECOMMONNAME",
    "V2DOCUMENTIDENTIFIER",
    "V1COUNTS",
    "V2.1COUNTS",
    "V1THEMES",
    "V2ENHANCEDTHEMES",
    "V1LOCATIONS",
    "V2ENHANCEDLOCATIONS",
    "V1PERSONS",
    "V2ENHANCEDPERSONS",
    "V1ORGANIZATIONS",
    "V2ENHANCEDORGANIZATIONS",
    "V1.5TONE",
    "V2.1ENHANCEDDATES",
    "V2GCAM",
    "V2.1SHARINGIMAGE",
    "V2.1RELATEDIMAGES",
    "V2.1SOCIALIMAGEEMBEDS",
    "V2.1SOCIALVIDEOEMBEDS",
    "V2.1QUOTATIONS",
    "V2.1ALLNAMES",
    "V2.1AMOUNTS",
    "V2.1TRANSLATIONINFO",
    "V2EXTRASXML"
  ).zipWithIndex.toMap

  /**
    * Returns a map of methods to extract strings from the fields used for topics
    */
  val separators = {
    def convertLocations = (cell: String) => if (cell != "") {
      cell.split(";").map(_.split("#")(1))
    } else {
      Array[String]()
    }
    def convertCell = (cell:String) => cell.split(";").map(_.split(",")(0))
    Map(
      "V1THEMES" -> convertCell,
      "V2ENHANCEDTHEMES" -> convertCell,
      "V1LOCATIONS" -> convertLocations,
      "V2.1ALLNAMES" -> convertCell
    )
  }
  /**
    * Creates the Avro schema used to create the Avro record
    */
  val gdeltAvroSchema = {
    val parser = new Schema.Parser
    val schemaFile = getClass().getResourceAsStream("/avroSchemas/parsed-gdelt-avro-schema.json")
    parser.parse(schemaFile)
  }
  /**
    * Converts an Avro record into Bytes
    */
  val recordInjection : Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary[GenericRecord](gdeltAvroSchema)

  /**
    *
    * @param a the input array
    * @param index an index
    * @return an Option which is None only if the index is out of bound
    */
  def getArrayValue(a: Array[String], index: Int): Option[String] = {
    if (a.length > index) {
      Option(a(index))
    } else {
      None
    }
  }

  /**
    * Parse a line of a GDELT file into an Avro encoded message
    *
    * @param line
    * @return an Option wrapping a byte array if parsing is successful
    */
  def parse(line: String): Option[Array[Byte]] = {
    val rawValues = line.split("\t")
    if (rawValues(indexOf("V2SOURCECOLLECTIONIDENTIFIER")) == "1") {
      val topics = separators.flatMap(fieldConverter => {
        val field = fieldConverter._1
        val convertMethod = fieldConverter._2
          getArrayValue(rawValues, indexOf(field)) match {
            case Some(s) => convertMethod(s)
            case None => Array.empty[String]
          }
      }).filter(
        (s) => s.size != 0 //remove empty strings
      ).map(
        _.replace(", ", ",").replace(" ", "_") //remove spaces from topics
      ).toSet.asJava
      // creates the Avro record
      val avroRecord = new GenericData.Record(gdeltAvroSchema)
      avroRecord.put("id", rawValues(indexOf("GKGRECORDID")))
      avroRecord.put("date", rawValues(indexOf("V2.1DATE")))
      avroRecord.put("url", rawValues(indexOf("V2DOCUMENTIDENTIFIER")))
      avroRecord.put("topics", topics)
      avroRecord.put("num_topics", topics.size())
      // encodes the record in a byte array
      Option(recordInjection(avroRecord))
    } else {
      None
    }
  }
}
