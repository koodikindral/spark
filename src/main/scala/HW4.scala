import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object HW4 {

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
    log.info(splitRdd.take(5))

    val mappedRdd = splitRdd.map(f => (f, 1))
    log.info(mappedRdd.collect)

    val reducedByKeyRdd = mappedRdd.reduceByKey(_ + _)
    log.info(mappedRdd.collect)
  }
}
