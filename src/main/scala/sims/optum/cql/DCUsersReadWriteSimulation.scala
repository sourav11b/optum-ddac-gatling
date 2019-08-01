package sims.optum.cql

import com.datastax.gatling.plugin.DsePredef._
import com.datastax.gatling.stress.core.BaseSimulation
import com.datastax.gatling.stress.libs.{FetchBaseData, SimConfig}
import io.gatling.core.Predef._
import actions.optum.cql.VisitorProfileActions
import feeds.optum.cql.VisitorProfileFeed
import actions.optum.cql.DCUsersActions

class DCUsersReadWriteSimulation extends BaseSimulation {
  val simName = "optum"
  
  val writeScenarioName = "writeDCUsers"
  val readScenarioName = "readDCUsers"
  
  val writeSimConf = new SimConfig(conf, simName, writeScenarioName)
  val readSimConf = new SimConfig(conf, simName, readScenarioName)
  
  
  val actions = new DCUsersActions(cass, writeSimConf)
  val writeFeed = new VisitorProfileFeed(writeSimConf.conf)
  


//  val writeScenario = scenario("Write")
//      .feed(writeFeed.getVisitorProfile)
//      .exec(actions.writeDataLoad)
   
  if(readSimConf.conf.getBoolean("simulations.optum.readDCUsers.createFreshCSV")){    
  new FetchBaseData(readSimConf, cass).createBaseDataCsv() 
  }
  
  val feederFile = getDataPath(readSimConf)
  val csvFeeder = csv(feederFile).random
      
//  val readScenario = scenario("Read")
//      .feed(csvFeeder)
//      .exec(actions.readData)   
      
  val readScenario = scenario("ReadData").randomSwitch((readSimConf.conf.getInt("simulations.optum.ratio.profile.read"),
    exec(feed(csvFeeder).exec(actions.readData))))
      
  val writeScenario = scenario("WriteData").randomSwitch((readSimConf.conf.getInt("simulations.optum.ratio.profile.write"),
    exec(feed(writeFeed.getVisitorProfile).exec(actions.writeDataLoad))))
  

  setUp(
      
      loadGenerator.rampUpToConstant(writeScenario, writeSimConf),
      loadGenerator.rampUpToConstant(readScenario, readSimConf)
      
  ).protocols(cqlProtocol)
}
