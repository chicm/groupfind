import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
 * Created by chicm on 2015/7/11.
 */
object GroupFind2 {
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
      classOf[ArrayBuffer[String]], classOf[ListBuffer[String]], classOf[Array[String]],
      classOf[Array[scala.Tuple2[String,String]]])) // try to workaround SPARK-7483 https://issues.apache.org/jira/browse/SPARK-7483

    val sc = new SparkContext(conf)

    val candarray = sc.textFile(args(0)).map(_.split(",")(0)).collect()
    val candMap = new mutable.HashMap[String, Int]
    for(i <- 0 until candarray.length) { candMap.put(candarray(i), i)}

    val bcCandMap = sc.broadcast(candMap)
    val regRDD = sc.textFile(args(1)).map(_.split(","))//.filter((arr)=>bcCandMap.value.contains(arr(2)))

    val trans = regRDD.map((arr) =>(arr(6)+arr(4).substring(0,10), bcCandMap.value.getOrElse(arr(2), -1) )).groupByKey().values
                .map(_.toArray).cache()
          //.map(_.mkString("", ",", "")).saveAsTextFile(args(3)+"/basket")

    //val trans = regRDD.map((arr) =>(arr(6)+arr(4).substring(0,10), bcCandMap.value.getOrElse(arr(2), -1) )).groupByKey().values
    //trans.collect.map(_.mkString("", ",", "")).saveAsTextFile(args(3)+"/basket")
   // val trans = sc.textFile(args(3)+"/basket").map(_.split(",")).map(_.map(_.toInt))

    val model = new FPGrowth()
      .setMinSupport(args(2).toDouble/trans.count())
      .setNumPartitions(48)
      .run(trans)

    //println(s"Number of frequent itemsets: ${model.freqItemsets.count()}")
    val bcCandArray = sc.broadcast(candarray)
    model.freqItemsets.filter(_.items.length>1).map(itemset =>(itemset.items.map(bcCandArray.value(_)),itemset.freq))
      .filter(itemset=>itemset._1.filter(item=>bcCandMap.value.contains(item)).size>0)
      .map(itemset =>
      itemset._1.mkString("[", ",", "]") + "," + itemset._2).saveAsTextFile(args(3)+"/result")

    /*model.freqItemsets.filter(_.items.length>1).collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }*/
  }
}
