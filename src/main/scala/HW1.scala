import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object HW1 {

  def main(args: Array[String]): Unit = {
    val log = LogManager.getRootLogger
    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf
      .setMaster("local")
      .setAppName("HW1 assignment")

    val sc = new SparkContext(conf)

    val baseRdd = sc.textFile("src/main/resources/shakespeare.txt")

    val splitRdd = baseRdd.flatMap(l => l.split("\\W+"))
    val filterRdd = splitRdd.filter(f => f.length > 10)
    val distinctRdd = splitRdd.distinct

    log.info("splitRdd: " + splitRdd.count)
    log.info("filterRdd: " + filterRdd.count)
    log.info("distinctRdd: " + distinctRdd.count)

    distinctRdd.coalesce(1).saveAsTextFile("src/main/resources/Rdistinct")
  }
}
