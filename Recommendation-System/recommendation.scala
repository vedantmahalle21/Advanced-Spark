package com.recommendation.scrobbler
import org.apache.spark.ml.recommendation._
import scala.util.Random
import org.apache.spark.sql.SparkSession


object Recommendation {
  def main(args: Array[String]){
      val spark = SparkSession
      .builder
      .appName("Recommendation")
      .getOrCreate()

    val rawUserArtistData = spark.read.textFile("datasets/Audio-Scrobbler-Datasets/user_artist_data.txt").repartition(8) 

    rawUserArtistData.take(5).foreach(println)

    val userArtistDF = rawUserArtistData.map { 
        line => val Array(user, artist, _*) = line.split(' ') 
        (user.toInt, artist.toInt)
    }.toDF("user", "artist")

    userArtistDF.agg(
      min("user"), max("user"), min("artist"), max("artist")).show()

    val rawArtistData = spark.read.textFile("datasets/Audio-Scrobbler-Datasets/artist_data.txt")


      val artistByID = rawArtistData.flatMap { 
          line => val (id, name) = line.span(_ != '\t')
            if (name.isEmpty) {
                None
            }
            else{ 
                try {
                    Some((id.toInt, name.trim)) 
                } 
                catch {
                    case _: NumberFormatException => None 
                }
            }
        }.toDF("id", "name")

        val rawArtistAlias = spark.read.textFile("datasets/Audio-Scrobbler-Datasets/artist_alias.txt") 
        val artistAlias = rawArtistAlias.flatMap { line =>
        val Array(artist, alias) = line.split('\t') 
        if (artist.isEmpty) {
        None
        }else{
        Some((artist.toInt, alias.toInt))
            }
            }.collect().toMap
        artistAlias.head

        val bArtistAlias = spark.sparkContext.broadcast(artistAlias)

        val trainData = buildCounts(rawUserArtistData, bArtistAlias)
        trainData.cache()


    
        val model = new ALS()
        .setSeed(Random.nextLong())
        .setImplicitPrefs(true)
        .setRank(10)
        .setRegParam(0.01)
        .setAlpha(1.0)
        .setMaxIter(5)
        .setUserCol("userId")
        .setItemCol("artistId")
        .setRatingCol("count")
        .setPredictionCol("prediction")
        .fit(trainData)


  }
}
