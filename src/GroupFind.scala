import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable

/**
 * Created by chicm on 2015/7/11.
 */
object GroupFind {
  def main(args: Array[String]): Unit = {
    if (args == null || args.length < 2) {
      println("usage: GroupFind candidateFile hotelFile")
      return
    }
    val conf = new SparkConf().setAppName("GroupFind")
    val sc = new SparkContext(conf)
    val cand = new mutable.HashSet[String]()
    sc.textFile(args(0)).map(_.split(",")(0)).collect().foreach(cand.add(_))
    val bcCand = sc.broadcast(cand);

    val regRDD = sc.textFile(args(1)).map(_.split(",")).filter((arr)=>bcCand.value.contains(arr(2))).sortBy(_(6))

    regRDD.map((arr) =>(arr(6), arr(2) )).groupByKey().values
          .map(_.mkString("", ",", "")).saveAsTextFile("/opt/basket")

    val trans = sc.textFile("/opt/basket").map(_.split(","))
    val model = new FPGrowth()
      .setMinSupport(2.0/trans.count())
      .setNumPartitions(2)
      .run(trans)

    println(s"Number of frequent itemsets: ${model.freqItemsets.count()}")

    model.freqItemsets.filter(_.items.length>1).map(itemset =>
      itemset.items.mkString("[", ",", "]") + ", " + itemset.freq).saveAsTextFile("/opt/result")

    model.freqItemsets.filter(_.items.length>1).collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }
  }
}
