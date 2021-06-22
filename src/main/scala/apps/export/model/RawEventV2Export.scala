package com.dy.spark.scala
package apps.`export`.model

case class RawEventV2Export(
    dyid:  Option[Long], 
    eventType:  Option[String], 
    timestamp:  Option[Long], 
    reqTimestamp:  Option[Long], 
    procTimestamp:  Option[Long], 
    sectionId:  Option[Int], 
    sessionId:  Option[Int], 
    eventId:  Option[Int], 
    eventName:  Option[String], 
    eventProperties:  Option[String], 
    eventValue:  Option[Long], 
    uniqueTransactionId:  Option[String], 
    productIds:  List[String], 
    isGoalHit:  Option[Boolean], 
    url:  Option[String], 
    urlClean:  Option[String], 
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
    device:  Option[String], 
    deviceBrand:  Option[String], 
    userType:  Option[String], 
    trafficSource:  Option[String], 
    referringDomain:  Option[String], 
    landingPage:  Option[String], 
    landingContext:  Option[String], 
    browser:  Option[String], 
    operatingSystem:  Option[String], 
    screenResolution:  Option[String], 
    userAgent: Option[String]
)
