package mongodb

import org.scalatest.{BeforeAndAfter, FunSuite}
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.query.Implicits._
import org.joda.time.DateTime
import com.mongodb.casbah.commons.conversions.scala.RegisterJodaTimeConversionHelpers
import com.mongodb.casbah

class MongodbTest extends FunSuite with BeforeAndAfter {
  val mongoConn = MongoConnection()
  val databaseName: String = "testdb"
  val collectionName: String = "cities"

  before {
    RegisterJodaTimeConversionHelpers()
    mongoConn(databaseName).dropDatabase()
    insertCity("New York", 22200000, new DateTime("2009-07-31"), List("statue of liberty", "food"), Map("name" -> "Michael Bloomberg", "party" -> "I"))
    insertCity("Punxsutawney", 6200, new DateTime("2008-01-31"), List("phil the groundhog"), Map("name" -> "Jim Wehrle") )
    insertCity("Portland", 582000, new DateTime("2007-09-20"), List("beer", "food"), Map("name" -> "Sam Adams", "party" -> "D") )
  }

  test("querying from a Mongo collection") {
    assert(mongoConn(databaseName)(collectionName).find().size === 3)
  }

  test("querying using a regular expressions") {
      val query = MongoDBObject("name" -> "(?i)new.*".r)
      val result = mongoConn(databaseName)(collectionName).findOne(query).head
      assert(result("name") === "New York")
      println(result)
   }

   def insertCity( name: String, population: Int, last_census: DateTime, famous_for: List[String], mayor_info: DBObject ) {
    mongoConn(databaseName)(collectionName) += MongoDBObject(
      "name" -> name,
      "population" -> population,
      "last_census" ->  last_census,
      "famous_for" -> famous_for,
      "mayor" -> mayor_info
    )
  }

}
