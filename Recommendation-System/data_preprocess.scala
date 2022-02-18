import org.apache.spark.sql._
import org.apache.spark.broadcast._

def buildCounts(
    rawUserArtistData: Dataset[String],
    bArtistAlias: Broadcast[Map[Int, Int]]): DataFrame = {
        rawUserArtistData.map { line =>
            val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
            val finalArtistID = bArtistAlias.value.getOrElse(artistID, artistID)
            (userID, finalArtistID, count)
        }.toDF("userId", "artistId", "count")
    }
    