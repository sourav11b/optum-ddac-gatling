package feeds.optum.cql

import java.nio.ByteBuffer
import java.sql.Timestamp

import scala.util.Random
import com.datastax.gatling.stress.core.BaseFeed
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import com.datastax.gatling.stress.helpers.SolrQueryBuilder
import java.text.SimpleDateFormat

import com.github.javafaker.Faker
import com.typesafe.config.Config

import scala.collection.mutable

case class UpdateDataFeed(conf:Config) extends BaseFeed with LazyLogging {


    def getData = {
      
    def rowData = this.getDataRow

    Iterator.continually(rowData)
  }
    


  private def getDataRow = {
    Map(
      "last_modified_by" -> getUuid.toString(),
      "last_modified_date" -> getRandomEpoch

    )
  }

 
  
}
