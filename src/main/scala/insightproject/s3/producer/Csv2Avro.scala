package insightproject.s3.producer

import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import java.io.File
import collection.JavaConverters._
/**
  * Created by rfrigato on 6/13/17.
  */
object GdeltCsv2Avro {
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

  val gdeltAvroSchema = {
    val parser = new Schema.Parser
    val schemaFile = getClass().getResourceAsStream("/avroSchemas/parsed-gdelt-avro-schema.json")
    parser.parse(schemaFile)
  }
  val recordInjection : Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary[GenericRecord](gdeltAvroSchema)
  def getArrayValue[String](a: Array[String], index: Int):Option[String] = {
    if (a.length > index) {
      Option(a(index))
    } else {
      None
    }
  }
  def parse(line: String):Option[Array[Byte]] = {
    val rawValues = line.split("\t")
    if (rawValues(indexOf("V2SOURCECOLLECTIONIDENTIFIER")) == "1") {
      val topics = separators.flatMap(fieldConverter => {
        val field = fieldConverter._1
        val convertMethod = fieldConverter._2
          getArrayValue(rawValues, indexOf(field)) match {
            case Some(s) => convertMethod(s)
            case None => Array.empty[String]
          }
      }).toSet.asJava
      val avroRecord = new GenericData.Record(gdeltAvroSchema)
      avroRecord.put("id", rawValues(indexOf("GKGRECORDID")))
      avroRecord.put("date", rawValues(indexOf("V2.1DATE")))
      avroRecord.put("url", rawValues(indexOf("V2DOCUMENTIDENTIFIER")))
      avroRecord.put("topics", topics)
      Option(recordInjection(avroRecord))
    } else {
      None
    }
  }
}
