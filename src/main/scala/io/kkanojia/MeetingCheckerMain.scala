package io.kkanojia

import io.kkanojia.service.CheckerService

object MeetingCheckerMain {

  def main(args: Array[String]): Unit = {

    import org.apache.spark.sql.SparkSession

    implicit val spark = SparkSession
      .builder()
      .appName("Meeting Checker")
      .config("spark.master", "local[*]")
      .getOrCreate()


    val partyA = args(0)
    val partyB = args(1)
    val filePath = args(2)

    CheckerService.checkMeeting(filePath, partyA, partyB)

  }
}
