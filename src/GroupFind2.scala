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
      classOf[Array[scala.Tuple2[String,String]]] /*, classOf[org.apache.spark.mllib.fpm.FPTree]*/)) // try to workaround SPARK-7483 https://issues.apache.org/jira/browse/SPARK-7483

    val sc = new SparkContext(conf)
    //val candSet = new mutable.HashSet[String]()
    val candMap = new mutable.HashMap[String, Int]
    val candarray = sc.textFile(args(0)).map(_.split(",")(0)).collect()
    //candarray.foreach((ele) => { var i = 0; candMap.put(ele, i) ; i+=1; })
    for(i <- 0 until candarray.length) { candMap.put(candarray(i), i)}

    val bcCandMap = sc.broadcast(candMap)
    val regRDD = sc.textFile(args(1)).map(_.split(",")).filter((arr)=>bcCandMap.value.contains(arr(2)))
    //bcCand.destroy()

    for(i <- 1 to 1000) {
      println("*******"+candarray(i)+ ":" + bcCandMap.value.getOrElse(candarray(i), -1))
    }
    regRDD.map((arr) =>(arr(6)+arr(4).substring(0,10), bcCandMap.value.getOrElse(arr(2), -1) )).groupByKey().values
          .map(_.mkString("", ",", "")).saveAsTextFile(args(3)+"/basket")

    //val trans = regRDD.map((arr) =>(arr(6)+arr(4).substring(0,10), bcCandMap.value.getOrElse(arr(2), -1) )).groupByKey().values
    //trans.collect.map(_.mkString("", ",", "")).saveAsTextFile(args(3)+"/basket")
    val trans = sc.textFile(args(3)+"/basket").map(_.split(",")).map(_.map(_.toInt))

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
