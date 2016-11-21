/**
  * Created by wuyiran on 11/20/16.
  */

//val datadir = "/Users/wuyiran/Documents/workspace/data/profiledata_06-May-2005"
val datadir = "/testdata/"
val artist_alias = datadir + "artist_alias.txt"
val artist_data = datadir + "artist_data.txt"
val user_artist_data = datadir + "user_artist_data.txt"

val rawUserArtistData = sc.textFile(user_artist_data)

val rawArtistData = sc.textFile(artist_data)
val artistByID = rawArtistData.map {
  line =>
    val (id, name) = line.span( _ != '\t')
    if (name.isEmpty) {
      None
    }else {
      try {
        Some((id.toInt, name.trim))
      }catch {
        case e: NumberFormatException => None
      }
    }
}

val rawArtistAlias = sc.textFile(artist_alias)
val artistAlias = rawArtistAlias.flatMap { line =>
  val tokens = line.split('\t')
  if (tokens(0).isEmpty) {
    None
  }else {
    Some((tokens(0).toInt, tokens(1).toInt))
  }
}.collectAsMap()

import org.apache.spark.mllib.recommendation._
val bArtistAlias = sc.broadcast (artistAlias)

val trainData = rawUserArtistData.map {
  line =>
    val Array(userID, artistID, count) = line.split(' ').map(_.toInt)
    val finalArtistID =
      bArtistAlias.value.getOrElse (artistID, artistID)
    Rating(userID, finalArtistID, count)
}.cache()

val model = ALS.trainImplicit(trainData, 10, 5, 0.01, 1.0)

model.userFeatures.mapValues(_.mkString(",")).first()


val rawArtistsForUser = rawUserArtistData.map(_.split(' ')).filter {
  case Array (user,_,_) => user.toInt == 2094760
}

val existingProducts = rawArtistsForUser.map {
  case Array(_,artist,_) => artist.toInt
}.collect.toSet

artistByID.filter { case (id , name) =>
  existingProducts.contains(id)
}.values.collect().foreach(println)
