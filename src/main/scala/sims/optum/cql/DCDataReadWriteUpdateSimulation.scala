package sims.optum.cql

import com.datastax.gatling.plugin.DsePredef._
import com.datastax.gatling.stress.core.BaseSimulation
import com.datastax.gatling.stress.libs.{FetchBaseData, SimConfig}
import io.gatling.core.Predef._
import actions.optum.cql.VisitorProfileActions
import feeds.optum.cql.VisitorProfileFeed
import actions.optum.cql.DCUsersActions
import actions.optum.cql.DCDataActions
import feeds.optum.cql.UpdateDataFeed

class DCDataReadWriteUpdateSimulation extends BaseSimulation {
  val simName = "optum"
  
  val writeScenarioName = "writeDCData"
  val readScenarioName = "readDCData"
  
  val writeSimConf = new SimConfig(conf, simName, writeScenarioName)
  val readSimConf = new SimConfig(conf, simName, readScenarioName)
  
  
  
  val actions = new DCDataActions(cass, writeSimConf,readSimConf.conf.getString("simulations.optum.readDCData.dataPoint"))
  val writeFeed = new VisitorProfileFeed(writeSimConf.conf)
  
  val updateFeed = new UpdateDataFeed(writeSimConf.conf)

//  val writeScenario = scenario("Write")
//      .feed(writeFeed.getVisitorProfile)
//      .exec(actions.writeDataLoad)
  
   
  if(readSimConf.conf.getBoolean("simulations.optum.readDCData.createFreshCSV")){    
  new FetchBaseData(readSimConf, cass).createBaseDataCsv() 
  }
  
  val feederFile = getDataPath(readSimConf)
  val csvFeeder = csv(feederFile).random
  
  val dataCsvFeeder = csv("./data/dc_data.csv").random

      
//  val readScenario = scenario("Read")
//      .feed(csvFeeder)
//      .exec(actions.readData)   
      
  val readScenario = scenario("ReadData").randomSwitch((readSimConf.conf.getInt("simulations.optum.ratio.data.read"),
    exec(feed(csvFeeder).exec(actions.readData))))
      
  val writeScenario = scenario("WriteData").randomSwitch((readSimConf.conf.getInt("simulations.optum.ratio.data.write"),
    exec(feed(writeFeed.getVisitorProfile).feed(dataCsvFeeder).exec(actions.writeDataLoad))))
  
  val updateScenario = scenario("UpdateData").randomSwitch((readSimConf.conf.getInt("simulations.optum.ratio.data.update"),
    exec(feed(csvFeeder).feed(updateFeed.getData).feed(dataCsvFeeder).exec(actions.updateDataLoad))))

  setUp(
      
      loadGenerator.rampUpToConstant(writeScenario, writeSimConf),
      loadGenerator.rampUpToConstant(readScenario, readSimConf),
      loadGenerator.rampUpToConstant(updateScenario, readSimConf)
      
  ).protocols(cqlProtocol)
}
