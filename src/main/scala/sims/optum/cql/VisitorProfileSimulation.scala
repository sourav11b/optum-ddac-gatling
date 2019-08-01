package sims.optum.cql

import com.datastax.gatling.plugin.DsePredef._
import com.datastax.gatling.stress.core.BaseSimulation
import com.datastax.gatling.stress.libs.SimConfig
import io.gatling.core.Predef._
import actions.optum.cql.VisitorProfileActions
import feeds.optum.cql.VisitorProfileFeed

class DataWriteSimulation extends BaseSimulation {
  val simName = "optum"
  val scenarioName = "writeVisitorProfile"
  val simConf = new SimConfig(conf, simName, scenarioName)
  val actions = new VisitorProfileActions(cass, simConf)
  val writeFeed = new VisitorProfileFeed(simConf.conf)
  

  val drugsCsvFeeder = csv("./data/dc_drugs.csv").random
  val plansCsvFeeder = csv("./data/dc_plans.csv").random
  val providersCsvFeeder = csv("./data/dc_enrolments.csv").random
  val enrollmentsCsvFeeder = csv("./data/dc_providers.csv").random

  val writeScenario = scenario("Write")
      .feed(writeFeed.getVisitorProfile)
      .feed(drugsCsvFeeder)
      .feed(plansCsvFeeder)
      .feed(providersCsvFeeder)
      .feed(enrollmentsCsvFeeder)
      .exec(actions.writeDataLoad)

  setUp(loadGenerator.rampUpToConstant(writeScenario, simConf)).protocols(cqlProtocol)
}
