import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * Created by SaiKrishnaKishore on 10/24/2015.
 */
class AnagramWordsTest extends FunSuite with BeforeAndAfter {

  var sc: SparkContext = _

  before {
    val conf = new SparkConf().setMaster("local").setAppName("Anagrm Words Test")
    sc = new SparkContext(conf)
  }

  test("Test for Anagram words on gutenberg file") {
    // @@ SETUP
    val aw = new AnagramWords

    // @@ EXERCISE
    val anagrams: RDD[(String, String)] = aw.getAnagrams(sc.textFile(getClass.getClassLoader.getResource("word_count_input.txt").getPath))

    // @@ VERIFY
    anagrams.take(10).foreach(println)
  }
}
