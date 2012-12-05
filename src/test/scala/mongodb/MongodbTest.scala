package mongodb

import org.scalatest.{BeforeAndAfter, FunSuite}
import com.mongodb.casbah.Imports._
import org.joda.time.DateTime
import com.mongodb.casbah.commons.conversions.scala.RegisterJodaTimeConversionHelpers

class MongodbTest extends FunSuite with BeforeAndAfter {
  val mongoConn = MongoConnection()
  val databaseName: String = "testdb"
  val collectionName: String = "cities"

  before {
    RegisterJodaTimeConversionHelpers()
    mongoConn(databaseName).dropDatabase()
    insertCity("Punxsutawney", 6200, new DateTime("2008-01-31"), List("phil the groundhog"), Map("name" -> "Jim Wehrle") )
    insertCity("Portland", 582000, new DateTime("2007-09-20"), List("beer", "food"), Map("name" -> "Sam Adams", "party" -> "D") )
  }

  test("querying from a Mongo collection") {
    assert(mongoConn(databaseName)(collectionName).find().size === 2)
  }

  def insertCity( name: String, population: Int, last_census: DateTime, famous_for: List[String], mayor_info: Map[String,String] ) {
    mongoConn(databaseName)(collectionName) += MongoDBObject(
      "name" -> name,
      "population" -> population,
      "last_census" ->  last_census,
      "famous_for" -> famous_for,
      "mayor" -> mayor_info
    )
  }

}
