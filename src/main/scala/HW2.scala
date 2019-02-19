import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object HW2 {

  def main(args: Array[String]): Unit = {
    val log = LogManager.getRootLogger
    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf
      .setMaster("local")
      .setAppName("HW2 assignment")

    val sc = new SparkContext(conf)

    val numbersRdd = sc.parallelize(Array(15, 20, 95, 80))
    val stats = numbersRdd.stats
    log.info(stats)

    val maryFile = "Mary had a little lamb"
    val maryRdd = sc.parallelize(maryFile)
    log.info(maryRdd.collect)

    val numbersRddString = numbersRdd.flatMap(_.toString)
    val comboRdd = maryRdd.union(numbersRddString)
    log.info(comboRdd.collect)
  }
}
