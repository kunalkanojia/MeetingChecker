package io.kkanojia

import java.text.SimpleDateFormat

import io.kkanojia.service.CheckerService
import io.kkanojia.service.CheckerService.mainTableName
import org.apache.spark.sql.SparkSession
import org.scalatest._

class MeetingCheckerMainSpec extends FlatSpec with MustMatchers {

  implicit val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Meeting Checker Test")
      .config("spark.master", "local[*]")
      .getOrCreate()

  "Meeting checker" must "be able to read files" in {
    val ds = CheckerService.read("/Users/kunalkanojia/workspace/meetingcheck/src/test/resources/test.csv")
    ds.count() mustEqual 5103

    val firstLocation = ds.take(1).toList.head
    firstLocation.uid mustEqual "600dfbe2"
    firstLocation.x mustEqual 103.79211
    firstLocation.y mustEqual 71.50419417988532
    firstLocation.floor mustEqual 1
    // Take care of tiemzone and enable below
    // df.format(firstLocation.timestamp) mustEqual "2014-07-19T16:00:06.071Z"
  }

  it must "be able to get average for particular uid" in {
    val ds = CheckerService.read("/Users/kunalkanojia/workspace/meetingcheck/src/test/resources/test.csv")
    ds.createOrReplaceTempView(CheckerService.mainTableName)

    val party = "600dfbe2"
    val partyData = CheckerService.getSimpleAverageDataForParty(party)

    partyData.filter(s"where uid != $party").count() mustEqual 0

  }

}
