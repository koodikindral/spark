import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object HW5 {

  def main(args: Array[String]): Unit = {
    val log = LogManager.getRootLogger
    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf
      .setMaster("local")
      .setAppName("HW1 assignment")

    val sc = new SparkContext(conf)

    val sqlContext = SparkSession
      .builder()
      .appName("Spark In Action")
      .master("local")
      .getOrCreate()

    val DFTitanic = sqlContext.read
      .format("csv")
      .option("inferSchema", "true")
      .option("header", "true")
      .load("/home/gert/Documents/Kool/Spark/titanic.csv")

    DFTitanic.show()
    DFTitanic.printSchema()
    DFTitanic.createOrReplaceTempView("Titanic")
    sqlContext.sql("select * from Titanic order by age desc limit 10").show()
    DFTitanic.select("name").show(5)
  }
}
