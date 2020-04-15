package Transformation

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType, _}

import scala.io.Source



object Test_onefc {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Onefctest").master("local").getOrCreate()

   /* val ipschema = StructType(Array(StructField("person_id",StringType,false),
      StructField("datetime",StringType,false),
      StructField("floor_level",LongType,false),
      StructField("building",StringType,false)))*/

    val url = ClassLoader.getSystemResource("/home/cloudera/onefc/schema.json")
    val schemaSource = Source.fromFile(url.getFile).getLines.mkString
    val ipschema = DataType.fromJson(schemaSource).asInstanceOf[StructType]
    
      // Each event should comply with the schema located in **schema.json**.
    val sampledf=spark.read.option("header","true")
      .option("inferschema","false").option("mode","PERMISSIVE")
      .schema(ipschema)
      .csv("/home/cloudera/onefc/data.csv")

    sampledf.show()
    sampledf.printSchema()

    //Your task it to output each floor access event in json format
    sampledf.write
      .partitionBy("floor_level")
      .json("/home/cloudera/onefc/dataoutput")

    //Read same file which is written in above location
    val df=spark.read.option("header","true").json("/home/cloudera/onefc/dataoutput")
    df.show()


  }

}
