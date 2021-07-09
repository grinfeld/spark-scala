package com.mikerusoft.spark.scala.apps.`export`.model

case class IncomingEventExport(
             userId:  Option[Long],
             eventType:  Option[String],
             reqTimestamp:  Option[Long],
             procTimestamp:  Option[Long],
             customerId:  Option[Int],
             sessionId:  Option[Int],
             eventId:  Option[Int],
             eventName:  Option[String],
             eventValue:  Option[Long],
             productIds:  List[String],
             url:  Option[String],
             contextType:  Option[String],
             contextData:  List[String],
             experimentId:  Option[Int],
             experienceId:  Option[Int],
             experienceName:  Option[String],
             versionId:  Option[Int],
             variationNames:  List[String],
             variationIds:  List[Int],
             engagementType:  Option[String],
             campaignId:  Option[Int],
             campaignName:  Option[String],
             audiences:  List[Int],
             audiencesNames:  List[String],
             userType:  Option[String],
             trafficSource:  Option[String],
             userAgent: Option[String]
           ) {
  def this() = this(None,None,None,None,None,None,None,None,None,List(),None,None,List(),None,None,None,None,List(),List(),None,None,None, List(), List(), None, None, None)
}

object IncomingEventExport {

  def apply(): IncomingEventExport = new IncomingEventExport()

  // basic
  def apply(customerId: Option[Int], variationPairs: List[String], experimentId: Option[Int], experienceId: Option[Int], experienceName: Option[String], expVerId: Option[Int], campaignId: Option[Int], campaignName: Option[String]): IncomingEventExport =
    new IncomingEventExport().copy(customerId = customerId, variationNames = variationPairs, experimentId = experimentId, experienceId = experienceId, experienceName = experienceName, versionId = expVerId, campaignId = campaignId, campaignName = campaignName)
}