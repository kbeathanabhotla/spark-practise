import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * Created by SaiKrishnaKishore on 10/17/2015.
 */
class WordCountTest extends FunSuite with BeforeAndAfter {
  var sc: SparkContext = _

  before {
    val conf = new SparkConf().setMaster("local").setAppName("Word Count Sample")
    sc = new SparkContext(conf)
  }

  test("Word Count on gutenberg file") {
    // @@ SETUP
    val wordCounter = new WordCount

    // @@ EXERCISE
    val wc = wordCounter.countWords(sc.textFile(getClass.getClassLoader.getResource("word_count_input.txt").getPath))

    // @@ VERIFY
    assert(wc.count() == 50106)
  }
}
