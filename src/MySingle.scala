import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * Created by chicm on 2015/7/11.
 */
object MySingle {
  def main(args: Array[String]): Unit = {
    if (args == null || args.length < 4) {
      println("usage: MySingle hotelFile id minCount out")
      return
    }
    val conf = new SparkConf().setAppName("GroupFind")
    val sc = new SparkContext(conf)

    val regRDD = sc.textFile(args(0)).map(_.split(",")).filter((arr)=>arr(2).equalsIgnoreCase(args(1)))

    regRDD.map((arr) =>(arr(6)+arr(4).substring(0,10), arr(2) )).groupByKey().values
          .map(_.mkString("", ",", "")).saveAsTextFile(args(3)+"/basket")

    val trans = sc.textFile(args(3)+"/basket").map(_.split(","))
    val model = new FPGrowth()
      .setMinSupport(args(2).toDouble/trans.count())
      .setNumPartitions(8)
      .run(trans)

    //println(s"Number of frequent itemsets: ${model.freqItemsets.count()}")


      model.freqItemsets.filter(_.items.length>1).map(itemset =>
      itemset.items.mkString("[", ",", "]") + ", " + itemset.freq).saveAsTextFile(args(3)+"/result")

    /*model.freqItemsets.filter(_.items.length>1).collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }*/
  }
}
