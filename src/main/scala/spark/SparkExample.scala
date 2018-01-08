package spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object SparkExample {
  def main(args: Array[String]): Unit = {

    val csvFiles = "~/Temp/spark/*.csv"

    val spark = SparkSession.builder.appName("Spark example").master("local[8]").getOrCreate

    val data = spark.read.option("header", "true").option("delimiter", "\t").csv(csvFiles)

    data.persist()

    println("(sum)?")
    data.groupBy("NIS Favorecido", "Nome Favorecido").agg(sum(data.col("Valor Parcela")), avg(data.col("Valor Parcela")), count(data.col("NIS Favorecido")))
      .orderBy(desc("sum(Valor Parcela)"))
      .show

    println("(Avg)?")
    data.groupBy("NIS Favorecido", "Nome Favorecido").agg(sum(data.col("Valor Parcela")), avg(data.col("Valor Parcela")), count(data.col("NIS Favorecido")))
      .orderBy(desc("avg(Valor Parcela)"))
      .show

    println("Total?")
    data.agg(sum(data.col("Valor Parcela"))).orderBy(desc("sum(Valor Parcela)")).persist().show
    spark.stop
  }
}
