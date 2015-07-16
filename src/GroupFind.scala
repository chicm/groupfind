import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, ArrayBuffer}

/**
 * Created by chicm on 2015/7/11.
 */
object GroupFind {
  def main(args: Array[String]): Unit = {
    if (args == null || args.length < 4) {
      println("usage: GroupFind candidateFile hotelFile minCount out")
      return
    }
    val conf = new SparkConf().setAppName("GroupFind")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //conf.set("spark.kryo.registrationRequired", "true")
    conf.set("spark.kryoserializer.buffer.max", "2047m")
    conf.registerKryoClasses(Array(classOf[org.apache.spark.rdd.RDD[scala.Predef.String]], classOf[mutable.HashSet[String]],
      classOf[ArrayBuffer[String]], classOf[ListBuffer[String]])) // try to workaround SPARK-7483 https://issues.apache.org/jira/browse/SPARK-7483
    val sc = new SparkContext(conf)
    val cand = new mutable.HashSet[String]()
    sc.textFile(args(0)).map(_.split(",")(0)).collect().foreach(cand.add(_))
    val bcCand = sc.broadcast(cand);

    val regRDD = sc.textFile(args(1)).map(_.split(",")).filter((arr)=>bcCand.value.contains(arr(2)))

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
