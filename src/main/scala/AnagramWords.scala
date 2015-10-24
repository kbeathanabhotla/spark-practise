import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD

/**
 * Created by SaiKrishnaKishore on 10/24/2015.
 */
class AnagramWords {

  def getAnagrams(rdd: RDD[String]): RDD[(String, String)] = {
    rdd.flatMap(line => line.split("\\W+")).filter(word => !StringUtils.isEmpty(word)).map(word => {
      (word.toLowerCase.sortWith(_ < _).toString, word.toLowerCase)
    }).reduceByKey((x, y) => x.concat(" , ").concat(y)).mapValues(value => value.split(" , ").toList.distinct.mkString(" ,"))
  }
}
