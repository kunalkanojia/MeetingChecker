package io.kkanojia.models


case class UserLocationInfo(
  uid: String,
  floor: Int,
  x: Double,
  y: Double,
  timestamp: java.sql.Timestamp
)