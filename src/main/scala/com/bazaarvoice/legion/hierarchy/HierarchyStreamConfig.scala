package com.bazaarvoice.legion.hierarchy

case class HierarchyStreamConfig (sourceTopic: String, parentTransitionTopic: String, childTransitionTopic: String, parentChildrenTopic: String, destTopic: String)
