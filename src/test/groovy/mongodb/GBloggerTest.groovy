package mongodb

import com.gmongo.GMongo
import org.joda.time.DateTime

class GBloggerTest extends GroovyTestCase {
    def mongo = new GMongo()
    def db = mongo.getDB("blogger")
    def theCollection = db.articles


    @Override
    public void setUp() {
        db.dropDatabase()
        insertBlogPost("Tony", "mail.bounces@gmail.com", new DateTime("2012-12-07"), "Loreum Ipsum")
    }

    void testUpdatingBlogPosts() {
        def comment = ["comment" : ["author": "Dave T Troll", "email": "bill.gates@microsoft.com"]]
        addComment("Tony", "mail.bounces@gmail.com", new DateTime("2012-12-07"), comment)

        def updatedPost = findPost("Tony", "mail.bounces@gmail.com", new DateTime("2012-12-07"))

        def firstComment = updatedPost.comments[0]
        assert(firstComment.comment.author == "Dave T Troll")

        println(updatedPost)
    }

    def addComment(String authorName, String authorEmail,DateTime creationDate, comment) {
        def post = findPost(authorName, authorEmail, creationDate)
        theCollection.update(post, [$push: ["comments" : comment]] )
    }

    def findPost(String authorName, String authorEmail,DateTime creationDate) {
        return theCollection.findOne("author_name" : authorName,
                "author_email" : authorEmail,
                "creation_date" : creationDate.toDate())
    }

    def insertBlogPost(String authorName, String authorEmail, DateTime creationDate, String text) {
        theCollection.insert([
                "author_name" : authorName,
                "author_email" : authorEmail,
                "creation_date" : creationDate.toDate(),
                "text" : text
        ])
    }
}