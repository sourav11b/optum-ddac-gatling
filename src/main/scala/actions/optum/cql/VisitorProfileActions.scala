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

class VisitorProfileActions(cassandra: Cassandra, simConf: SimConfig) extends BaseAction(cassandra, simConf) {
  // create table(s) if they do not exist
  createKeyspace
  createTables()
  

  // A regular string query can be used as well as the QueryBuilder
  private val writeDataQuery = QueryBuilder.insertInto(keyspace, table)
      .value("user_id", raw("?"))
      .value("optum_id", raw("?"))
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
      .value("plans_data", raw("?"))
      .value("providers_data", raw("?"))
      .value("drugs_data", raw("?"))
      .value("enrolments_data", raw("?"))

  def writeDataLoad = {
    val preparedStatement = session.prepare(writeDataQuery)
    group(Groups.INSERT) {
      exec(cql("Write Data")
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
      "${last_modified_date}",
      "${plans_data}",
      "${providers_data}",
      "${drugs_data}",
      "${enrolments_data}"
          )
        .withConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM) 	// ConsistencyLevel can be set per query
      )
    }
  }
      
  private val updateDataQuery = QueryBuilder.update(keyspace, table)
    .`with`(QueryBuilder.set("plans_data", raw("?")))
    .and(QueryBuilder.set("providers_data", raw("?")))
    .and(QueryBuilder.set("drugs_data", raw("?")))
    .and(QueryBuilder.set("last_modified_by", raw("?")))
    .and(QueryBuilder.set("last_modified_date", raw("?")))
    .where(QueryBuilder.eq("user_id", raw("?")))
    
  def updateDataLoad = {
    val preparedStatement = session.prepare(updateDataQuery)

    group(Groups.UPDATE) {
      exec(cql("Update Data")
        .executePrepared(preparedStatement)
        .withParams(
      "${plans_data}",
      "${providers_data}",
      "${drugs_data}",
      "${last_modified_by}",
      "${last_modified_date}",
      "${user_id}"
        )
        .withConsistencyLevel(ConsistencyLevel.LOCAL_QUORUM)
      )
    }
  }

  val readDataQuery = QueryBuilder.select().from(keyspace, table).where(QueryBuilder.eq("resourcetype",raw("?"))).and(QueryBuilder.eq("user_id",raw("?")))

  def readData = {
    val preparedStatement = session.prepare(readDataQuery)
    group(Groups.SELECT) {
      exec(cql("Read Data")
          .executePrepared(preparedStatement)
          .withParams(
            "${user_id}"          )
          .check(rowCount greaterThan 0)
      )
    }
  }



  def createTables(): Unit = {
    session.execute("""
     CREATE TABLE IF NOT EXISTS digital_checkout.visitor_profile (
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
                                                                plans_data text,
                                                                providers_data text,
                                                                drugs_data text,
                                                                enrolments_data text,
                                                                PRIMARY KEY (user_id))
    WITH bloom_filter_fp_chance = 0.1
        AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
        AND comment = 'uuid: Primary key to identify a user, plan_data: User saved plan. providers_data: User saved providers data. drugs_data: User saved providers data'
        AND compaction = {'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy', 'enabled': 'true', 'tombstone_compaction_interval': '43200', 'tombstone_threshold': '0.02', 'unchecked_tombstone_compaction': 'false'}
        AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
        AND crc_check_chance = 1.0
        AND default_time_to_live = 0
        AND gc_grace_seconds = 864000
        AND max_index_interval = 2048
        AND memtable_flush_period_in_ms = 0
        AND min_index_interval = 128
        AND read_repair_chance = 0.0
        AND speculative_retry = '99PERCENTILE'
""")
  }
  
  
}


