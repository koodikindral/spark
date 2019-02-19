import org.apache.log4j.LogManager
import org.apache.spark.{SparkConf, SparkContext}

object HW3 {

  def main(args: Array[String]): Unit = {
    val log = LogManager.getRootLogger
    //Create a SparkContext to initialize Spark
    val conf = new SparkConf()
    conf
      .setMaster("local")
      .setAppName("HW2 assignment")

    val sc = new SparkContext(conf)

    val months = Array("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul")
    val monthsRdd = sc.parallelize(months)
    log.info(monthsRdd.collect)

    val monthsIndexed0Rdd = sc.parallelize(months.zipWithIndex)
    log.info(monthsIndexed0Rdd.collect)

    val monthsIndexed1Rdd = monthsIndexed0Rdd.map{case (k, v) ⇒ k → (v + 1)}
    log.info(monthsIndexed1Rdd.collect)

    val monthsIndexed2Rdd = monthsIndexed0Rdd.mapValues(_ + 1)
    log.info(monthsIndexed2Rdd.collect)

    val quarters = Array(1, 1, 1, 2, 2, 2, 3)
    val quartersRdd = sc.parallelize(quarters).zip(monthsRdd)
    log.info(quartersRdd.collect)

    log.info(quartersRdd.keys.collect)
    log.info(quartersRdd.values.collect)
    log.info(quartersRdd.sortByKey(false).collect)
  }
}
