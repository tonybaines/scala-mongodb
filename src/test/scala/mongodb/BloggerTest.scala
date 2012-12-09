package mongodb

import org.scalatest.{BeforeAndAfter, FunSpec}
import com.mongodb.casbah.commons.{MongoDBObject => MO, MongoDBList}
import com.mongodb.casbah.query.Implicits._
import org.joda.time.DateTime
import com.mongodb.casbah.commons.conversions.scala.RegisterJodaTimeConversionHelpers
import com.mongodb.casbah.MongoConnection
import com.mongodb.{BasicDBObject, BasicDBList, DBObject}

class BloggerTest extends FunSpec with BeforeAndAfter {
  val mongoConn = MongoConnection()
  val databaseName: String = "blogger"
  val collectionName: String = "articles"

  before {
    RegisterJodaTimeConversionHelpers()
    mongoConn(databaseName).dropDatabase()
    insertBlogPost("Tony", "mail.bounces@gmail.com", new DateTime("2012-12-07"), "Loreum Ipsum")
  }

  describe("A collection of blog posts") {
    it("can be updated") {
      val comment = MO("comment" -> MO("author" -> "Dave T Troll", "email" -> "bill.gates@microsoft.com"))
      addComment("Tony", "mail.bounces@gmail.com", new DateTime("2012-12-07"), comment)

      val updatedPost = findPost("Tony", "mail.bounces@gmail.com", new DateTime("2012-12-07"))
      val comments = updatedPost.getAs[BasicDBList]("comments")

      val firstComment: BasicDBObject = comments.get(0).asInstanceOf[BasicDBObject]
      assert(firstComment.getAs[BasicDBObject]("comment").get("author") === "Dave T Troll")

      println(updatedPost)
    }
  }

  def theCollection = mongoConn(databaseName)(collectionName)

  def addComment(authorName: String, authorEmail: String, creationDate: DateTime, comment: DBObject) = {
    val post = findPost(authorName, authorEmail, creationDate)
    val operation : MO = $push( ("comments" -> comment))
    theCollection.update(post, operation)
  }

  def findPost(authorName: String, authorEmail: String, creationDate: DateTime): DBObject = {
    val query = MO("author_name" -> authorName) ++
      MO("author_email" -> authorEmail) ++
      MO("creation_date" -> creationDate)
    return theCollection.findOne(query).head
  }

  def insertBlogPost(authorName: String, authorEmail: String, creationDate: DateTime, text: String) = {
    theCollection += MO(
      "author_name" -> authorName,
      "author_email" -> authorEmail,
      "creation_date" -> creationDate,
      "text" -> text
    )
  }

}
