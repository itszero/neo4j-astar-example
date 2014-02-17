package zalgo

import javax.ws.rs.{Produces, GET, Path}
import net.liftweb.json.DefaultFormats
import org.neo4j.graphdb.{Direction, DynamicRelationshipType, Node, GraphDatabaseService}
import javax.ws.rs.core.Context
import org.neo4j.tooling.GlobalGraphOperations
import org.neo4j.kernel.{Uniqueness, Traversal}
import org.neo4j.graphdb.traversal.Evaluators
import scala.collection.JavaConversions._

@Path("/cc")
class CCHandler {
  implicit val formats = DefaultFormats

  @GET
  @Path("/do")
  @Produces(Array("application/json"))
  def doConnectedComponents(@Context db:GraphDatabaseService) = {
    val nodes = GlobalGraphOperations.at(db).getAllNodes().iterator()
    var tx = db.beginTx()
    var i = 0
    var ccid = 0

    nodes foreach { node: Node =>
      if (!node.hasProperty("cc_id")) {
        i = i + 1
        if (i % 1000 == 0) {
          tx.success()
          tx.finish()
          tx = db.beginTx()
          println(s"Progress: $i")
        }

        println(s"ccid = $ccid")
        node.setProperty("cc_id", ccid)
        ccid = ccid + 1

        val traverser = Traversal.description()
          .breadthFirst()
          .relationships(DynamicRelationshipType.withName("next"), Direction.BOTH)
          .evaluator(Evaluators.excludeStartPosition())
          .uniqueness(Uniqueness.NODE_GLOBAL)
          .traverse(node)

        traverser foreach { p: org.neo4j.graphdb.Path =>
          p.endNode().setProperty("cc_id", ccid)

          i = i + 1
          if (i % 1000 == 0) {
            tx.success()
            tx.finish()
            tx = db.beginTx()
            println(s"Progress: $i")
          }
        }
      }
    }
  }
}