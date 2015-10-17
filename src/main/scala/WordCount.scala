import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by SaiKrishnaKishore on 10/13/2015.
 */
object WordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Word Count Application").setMaster("local")
    val sc = new SparkContext(conf)

    val logData = sc.textFile(getClass.getResource("logging.log").getPath, 2)
    val wordCount = logData.flatMap(s => s.split("\\s+"))
                            .map(s => (s, 1))
                            .reduceByKey((x, y) => x + y)
                            //.saveAsTextFile(getClass.getResource("logging-output" + System.currentTimeMillis() + ".txt").getPath);

    wordCount.foreach(s => println(s))
  }
}
