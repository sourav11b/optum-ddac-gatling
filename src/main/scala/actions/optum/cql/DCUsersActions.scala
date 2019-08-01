package actions.optum.cql

import java.io.InputStream

import com.datastax.driver.core.{ConsistencyLevel, DataType}
import com.datastax.driver.core.querybuilder.QueryBuilder
import com.datastax.driver.core.querybuilder.QueryBuilder._
import com.datastax.driver.core.schemabuilder.SchemaBuilder
import com.datastax.gatling.plugin.CqlPredef.{cql, rowCount}
import com.datastax.gatling.plugin.DsePredef._
import com.datastax.gatling.stress.core.BaseAction
import com.datastax.gatling.stress.libs.{Cassandra, SimConfig}
import com.mashape.unirest.http.Unirest
import io.gatling.core.Predef._
import com.datastax.driver.core.schemabuilder.SchemaBuilder._

class DCUsersActions(cassandra: Cassandra, simConf: SimConfig) extends BaseAction(cassandra, simConf) {
  // create table(s) if they do not exist
  createKeyspace
  createTables()
  

  // A regular string query can be used as well as the QueryBuilder
  private val writeDataQuery = QueryBuilder.insertInto(keyspace, table)
      .value("user_id", raw("?"))
      .value("optum_id", raw("?"))
      .value("full_name", raw("?"))
      .value("first_name", raw("?"))
      .value("last_name", raw("?"))
      .value("nick_name", raw("?"))
      .value("site_id", raw("?"))
      .value("created_by", raw("?"))
      .value("creation_date", raw("?"))
      .value("last_modified_by", raw("?"))
      .value("last_modified_date", raw("?"))


  def writeDataLoad = {
    val preparedStatement = session.prepare(writeDataQuery)
    group(Groups.INSERT) {
      exec(cql("Write DCUsers")
        .executePrepared(preparedStatement)
        .withParams(
      "${user_id}",
      "${optum_id}",
      "${full_name}",
      "${first_name}",
      "${last_name}",
      "${nick_name}",
      "${site_id}",
      "${created_by}",
      "${creation_date}",
      "${last_modified_by}",
      "${last_modified_date}"
          )
        .withConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM) 	// ConsistencyLevel can be set per query
      )
    }
  }
      

    
   val readDataQuery = QueryBuilder.select().from(keyspace, table).where(QueryBuilder.eq("user_id",raw("?")))

  def readData = {
    val preparedStatement = session.prepare(readDataQuery)
    group(Groups.SELECT) {
      exec(cql("Read DCUsers")
          .executePrepared(preparedStatement)
          .withParams(
            "${user_id}"          )
          .check(rowCount greaterThan 0)
      )
    }
  }



  def createTables(): Unit = {
    session.execute("""
     CREATE TABLE IF NOT EXISTS digital_checkout.dc_users (
                                                                user_id text,
                                                                optum_id text,
                                                                email text,
                                                                full_name text,
                                                                first_name text,
                                                                last_name text,
                                                                nick_name text,
                                                                site_id text,
                                                                created_by text,
                                                                creation_date timestamp,
                                                                last_modified_by text,
                                                                last_modified_date timestamp,
                                                                PRIMARY KEY (user_id));
""")
  }
  
  
}


