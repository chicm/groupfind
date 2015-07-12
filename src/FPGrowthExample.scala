/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples.mllib


import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Example for mining frequent itemsets using FP-growth.
 * Example usage: ./bin/run-example mllib.FPGrowthExample \
 *   --minSupport 0.8 --numPartition 2 ./data/mllib/sample_fpgrowth.txt
 */
object FPGrowthExample {



  def run(params: Array[String]) {
    val conf = new SparkConf().setAppName(s"FPGrowthExample with $params")
    val sc = new SparkContext(conf)
    val transactions = sc.textFile(params(2)).map(_.split(" ")).cache()

    println(s"Number of transactions: ${transactions.count()}")

    val model = new FPGrowth()
      .setMinSupport(2)
      .setNumPartitions(2)
      .run(transactions)

    println(s"Number of frequent itemsets: ${model.freqItemsets.count()}")

    model.freqItemsets.collect().foreach { itemset =>
      println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq)
    }

    sc.stop()
  }
}
