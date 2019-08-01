package sims.optum.cql

import com.datastax.gatling.plugin.DsePredef._
import com.datastax.gatling.stress.core.BaseSimulation
import com.datastax.gatling.stress.libs.SimConfig
import io.gatling.core.Predef._
import actions.optum.cql.VisitorProfileActions
import feeds.optum.cql.VisitorProfileFeed
import actions.optum.cql.DCUsersActions

class DCUsersWriteSimulation extends BaseSimulation {
  val simName = "optum"
  val scenarioName = "writeDCUsers"
  val simConf = new SimConfig(conf, simName, scenarioName)
  val actions = new DCUsersActions(cass, simConf)
  val writeFeed = new VisitorProfileFeed(simConf.conf)
  


  val writeScenario = scenario("Write")
      .feed(writeFeed.getVisitorProfile)
      .exec(actions.writeDataLoad)

  setUp(loadGenerator.rampUpToConstant(writeScenario, simConf)).protocols(cqlProtocol)
}
