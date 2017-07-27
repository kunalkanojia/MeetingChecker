package io.kkanojia.service

import java.nio.file.Paths

import io.kkanojia.models.UserLocationInfo
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object CheckerService {

  val mainTableName = "reducedData"
  val partyATableName = "partyAData"
  val partyBTableName = "partyBData"


  /**
    * Given all information checks if meeting happened between the two parties.
    * Definition of meeting :
    * It is assumend that meeting happened if -
    * 1. They were on the same floor within 5 units distance
    * 2. In time frame of approximately 6 mins.
    */
  def checkMeeting(filePath: String, partyA: String, partyB: String)(implicit spark: SparkSession) = {



    val ds = read(filePath)
    ds.createOrReplaceTempView(mainTableName)

    val dfA = getSimpleAverageDataForParty(partyA)
    dfA.createOrReplaceTempView(partyATableName)

    val dfB = getSimpleAverageDataForParty(partyB)
    dfB.createOrReplaceTempView(partyBTableName)

    val userData = getMeetingFloorAndTime()

    userData.show(50)
  }

  /**
    * Checks if the two parties where within distance of 5 units at the same time.
    * It returns the floor and time where they met.
    * @return
    */
  def getMeetingFloorAndTime()(implicit spark: SparkSession): DataFrame = {
    spark.sql(
      s"""
        |select a.floor , to_timestamp(a.avgTime, 'yyMMddHHm') Approximate_Meeting_Time
        |from $partyATableName a
        |join $partyBTableName b on (a.floor = b.floor and a.avgTime = b.avgTime)
        |where sqrt(pow(a.x - b.x, 2) + pow(a.y - b.y, 2)) < 5
      """.stripMargin)
  }


  /**
    * Returns floor and mean x and y locations for 6 mins interval.
    * The function just truncates the second digit of the minute as a hack to get 6 mins interval.
    *
    * @param uid The UID of party
    * @return Dataframe Returns dataframe with data of individual party.
    */
  def getSimpleAverageDataForParty(uid: String)(implicit spark: SparkSession): DataFrame = {
    spark.sql(s"select floor, " +
      s"AVG(X) X, " +
      s"AVG(Y) Y, " +
      s"substring(date_format(timestamp, 'yyMMddHHmm'), 0,9)  avgTime " +
      s"FROM $mainTableName " +
      s"WHERE UID = '$uid' " +
      s"group by floor, avgTime")
  }

  /**
    * Reads the file and converts it into a Dataset of UserLocationInfo
    * @return
    */
  def read(filePath: String)(implicit spark: SparkSession): Dataset[UserLocationInfo] = {
    import spark.implicits._

    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      //.option("dateFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      .csv(filePath).as[UserLocationInfo]
  }

}
