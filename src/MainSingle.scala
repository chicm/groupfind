package com.skynet.scalapro.main

import org.apache.spark._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{ StructType, StructField, StringType }
import org.apache.spark.sql.SQLContext
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.rdd.RDD
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.Cell
import org.apache.spark.rdd.PairRDDFunctions

object MainSingle {
  def main(args: Array[String]) {

    if (args != null && args.length >= 3) {
      val sparkConf = new SparkConf().setAppName("First-Scala") //.setMaster("local[2]")

      val sparkContext = new SparkContext(sparkConf)
      val sqlContext = new SQLContext(sparkContext)

      val nativeDoc = sparkContext.textFile(args(0))

      val nativeFiedls = Array("xm", "xb", "sfzh", "csrq", "rzsj", "rzfh", "lgdm", "lgmc", "lgxz", "xzqh", "xzqhmc", "gajgdm", "gajgmc")
      val nativeSchema = StructType(nativeFiedls.map(fieldName => StructField(fieldName, StringType, true)))
      val nativeRDD = nativeDoc.map(_.split(",")).filter(arrs => arrs.length > 12).map(arrs => Row(arrs(0), arrs(1), arrs(2), arrs(3), arrs(4), arrs(5), arrs(6), arrs(7), arrs(8), arrs(9), arrs(10), arrs(11), arrs(12)))
      val nativeDataFrame = sqlContext.createDataFrame(nativeRDD, nativeSchema).cache()

      nativeDataFrame.registerTempTable("native")

      val rsRDD = sqlContext.sql("select sfzh,lgdm,rzsj from native where sfzh='" + args(1) + "'")

      val columns = rsRDD.collect()

      if (columns != null && columns.length > 0) {
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm")

        val travels = columns.map(rs => {
          val sfzh = rs.getString(0)
          val lgdm = rs.getString(1)
          val rzsj = format.parse(rs.getString(2)).getTime

          val st = format.format(new Date(rzsj - 300000))
          val et = format.format(new Date(rzsj + 300000))

          val travelRDD = sqlContext.sql("select * from native where lgdm='" + lgdm + "' and rzsj>='" + st + "' and rzsj<='" + et + "' and sfzh<>'" + sfzh + "'")

          travelRDD.map { row => (row.get(2), row) }
        }).reduce((r1, r2) => r1.union(r2)).cache().groupBy(rs => rs._1).cache().filter(trs => trs._2.size >= Integer.valueOf(args(2))).cache() //.saveAsTextFile(args(1))

        val travelsRDD = sparkContext.parallelize(travels.map(travel => {
          travel._2.map(row => {
            row._2
          }).toList
        }).reduce((r1, r2) => {
          r1.++(r2)
        })).saveAsTextFile(args(3))
      }
          /*.map(row => {
          val put = new Put(Bytes.toBytes(row.getString(2) + "|" + row.getString(6) + "|" + row.getString(4)))
          put.add(Bytes.toBytes("r"), Bytes.toBytes("xb"), Bytes.toBytes(row.getString(0)))
          put.add(Bytes.toBytes("r"), Bytes.toBytes("xb"), Bytes.toBytes(row.getString(1)))
          put.add(Bytes.toBytes("r"), Bytes.toBytes("sfzh"), Bytes.toBytes(row.getString(2)))
          put.add(Bytes.toBytes("r"), Bytes.toBytes("csrq"), Bytes.toBytes(row.getString(3)))
          put.add(Bytes.toBytes("r"), Bytes.toBytes("rzsj"), Bytes.toBytes(row.getString(4)))
          put.add(Bytes.toBytes("r"), Bytes.toBytes("rzfh"), Bytes.toBytes(row.getString(5)))
          put.add(Bytes.toBytes("r"), Bytes.toBytes("lgdm"), Bytes.toBytes(row.getString(6)))
          put.add(Bytes.toBytes("r"), Bytes.toBytes("lgmc"), Bytes.toBytes(row.getString(7)))
          put.add(Bytes.toBytes("r"), Bytes.toBytes("lgxz"), Bytes.toBytes(row.getString(8)))
          put.add(Bytes.toBytes("r"), Bytes.toBytes("xzqh"), Bytes.toBytes(row.getString(9)))
          put.add(Bytes.toBytes("r"), Bytes.toBytes("xzqhmc"), Bytes.toBytes(row.getString(10)))
          put.add(Bytes.toBytes("r"), Bytes.toBytes("gajgdm"), Bytes.toBytes(row.getString(11)))
          put.add(Bytes.toBytes("r"), Bytes.toBytes("gajgmc"), Bytes.toBytes(row.getString(12)))

          (new org.apache.hadoop.hbase.io.ImmutableBytesWritable, put)
        })

        val hbaseConf = HBaseConfiguration.create()
        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
        hbaseConf.set("hbase.zookeeper.quorum", "jd-master,jd-slave1,jd-slave2")

        val jobConf = new JobConf(hbaseConf, this.getClass)
        jobConf.setOutputFormat(classOf[TableOutputFormat])
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, "travels")

        new PairRDDFunctions(travelsRDD).saveAsHadoopDataset(jobConf)
      }*/
      sparkContext.stop();
    }
  }
}