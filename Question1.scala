package Transformation

import org.apache.spark.sql.SparkSession

object Test_onefcq {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Event").master("local").getOrCreate()
    val Eventdf=spark.read.option("header","true").csv("/home/cloudera/onefc/event.csv")
    Eventdf.show()

 

    Eventdf.createOrReplaceTempView("Eventx")
    val Eventopdf=spark.sql("""select ev1.* from Eventx ev1 CROSS JOIN Eventx ev2 CROSS JOIN Eventx ev3
                         where ev1.people >= 100 and ev2.people >= 100 and ev3.people >= 100
                         and(
                               (ev1.id - ev2.id = 1 and ev1.id - ev3.id = 2 and ev2.id - ev3.id =1)
                             or
                             (ev2.id - ev1.id = 1 and ev2.id - ev3.id = 2 and ev1.id - ev3.id =1)
                             or
                             (ev3.id - ev2.id = 1 and ev2.id - ev1.id =1 and ev3.id - ev1.id = 2)
                         ) """)
        .dropDuplicates("id")
    Eventopdf.show()
  }

}
