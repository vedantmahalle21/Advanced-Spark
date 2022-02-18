package com.linkage.analysis

import org.apache.spark.sql.SparkSession

object Linkage {
  def main(args: Array[String]){
      val spark = SparkSession
      .builder
      .appName("Linkage")
      .getOrCreate()

    val parsed = spark.read.option("header", "true").option("nullValue", "?").option("inferSchema", "true").csv("datasets/donations")

    parsed.show()

    parsed.printSchema

    parsed.rdd.map(_.getAs[Boolean]("is_match")).countByValue()

    parsed.groupBy("is_match").count().orderBy($"count".desc).show()

    parsed.agg(avg($"cmp_sex"), stddev($"cmp_sex")).show()

    parsed.createOrReplaceTempView("linkage")

    spark.sql("""
            SELECT is_match, COUNT(*) cnt
            FROM linkage
            GROUP BY is_match
            ORDER BY cnt DESC
      """).show()

    val summary = parsed.describe()

    summary.show()

    summary.select("summary", "cmp_fname_c1", "cmp_fname_c2").show()

    val matches = parsed.where("is_match = true") 
    val matchSummary = matches.describe()
    val misses = parsed.filter($"is_match" === false) 
    val missSummary = misses.describe()

    val schema = summary.schema

    val longForm = summary.flatMap(row => {
      val metric = row.getString(0)
      (1 until row.size).map(i => {
              (metric, schema(i).name, row.getString(i).toDouble)
      })
      })

      val longDF = longForm.toDF("metric", "field", "value") 
      longDF.show()

      val wideDF = longDF.
        groupBy("field").
        pivot("metric", Seq("count", "mean", "stddev", "min", "max")). 
        agg(first("value"))

      wideDF.select("field", "count", "mean").show()

      case class Score(value: Double) { 
        def +(oi: Option[Int]) = {
          Score(value + oi.getOrElse(0)) }
      }

      val matchSummaryT = pivotSummary(matchSummary) 
      val missSummaryT = pivotSummary(missSummary)

      matchSummaryT.createOrReplaceTempView("match_desc")
      missSummaryT.createOrReplaceTempView("miss_desc")

      spark.sql("""
            SELECT a.field, a.count + b.count total, a.mean - b.mean delta
            FROM match_desc a INNER JOIN miss_desc b ON a.field = b.field
            WHERE a.field NOT IN ("id_1", "id_2")
            ORDER BY delta DESC, total DESC
          """).show()

      case class MatchData(
      id_1: Int,
      id_2: Int,
      cmp_fname_c1: Option[Double], 
      cmp_fname_c2: Option[Double], 
      cmp_lname_c1: Option[Double], 
      cmp_lname_c2: Option[Double], 
      cmp_sex: Option[Int], 
      cmp_bd: Option[Int],
      cmp_bm: Option[Int], 
      cmp_by: Option[Int], 
      cmp_plz: Option[Int], 
      is_match: Boolean
      )

      val matchData = parsed.as[MatchData] 
      matchData.show()

      case class Score(value: Double) { def +(oi: Option[Int]) = {
        Score(value + oi.getOrElse(0)) }
        }


      def scoreMatchData(md: MatchData): Double = { 
        (Score(md.cmp_lname_c1.getOrElse(0.0)) + md.cmp_plz +
          md.cmp_by + md.cmp_bd + md.cmp_bm).value
      }

    

      val scored = matchData.map { md => 
      (scoreMatchData(md), md.is_match) 
      }.toDF("score", "is_match")

      def crossTabs(scored: DataFrame, t: Double): DataFrame = { scored.
        selectExpr(s"score >= $t as above", "is_match"). groupBy("above").
        pivot("is_match", Seq("true", "false")). count()
        }

      crossTabs(scored, 4.0).show()


  }
      
}



