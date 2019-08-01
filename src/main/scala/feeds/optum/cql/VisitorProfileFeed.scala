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

case class VisitorProfileFeed(conf:Config) extends BaseFeed with LazyLogging {


    def getVisitorProfile = {
      
    def rowData = this.getVisitorProfileRow

    Iterator.continually(rowData)
  }
    


  private def getVisitorProfileRow = {
    Map(
      "user_id" -> getUuid.toString(),
      "optum_id" ->  getUuid.toString(),
      "email" -> faker.internet.emailAddress(),
      "full_name" -> faker.name().fullName(),
      "first_name" -> faker.name().firstName(),
      "last_name" -> faker.name().lastName(),
      "nick_name" -> faker.name.firstName(),
      "site_id" -> getUuid.toString(),
      "created_by" -> getUuid.toString(),
      "creation_date" -> getRandomEpoch,
      "last_modified_by" -> getUuid.toString(),
      "last_modified_date" -> getRandomEpoch

    )
  }

 
  
}
