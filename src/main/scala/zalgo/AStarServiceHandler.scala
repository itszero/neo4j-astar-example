package zalgo

import net.liftweb.json._

import javax.ws.rs._
import javax.ws.rs.core.{Response, Context}

import org.neo4j.graphdb.{Direction, RelationshipType, Node, GraphDatabaseService}
import org.neo4j.graphalgo._
import org.neo4j.kernel.Traversal

import scala.collection.JavaConversions._
import org.neo4j.cypher.javacompat.ExecutionEngine
import com.infomatiq.jsi.rtree.RTree
import com.infomatiq.jsi.{Point, Rectangle}
import gnu.trove.TIntProcedure
import org.neo4j.tooling.GlobalGraphOperations

@Path("/astar")
class AStarServiceHandler {
  implicit val formats = DefaultFormats

  @GET
  @Path("/ping")
  def ping : String = { "pong" }

  @GET
  @Path("/a/{from}/{to}")
  @Produces(Array("application/json"))
  def astar(@PathParam("from") fromID:Long, @PathParam("to") toID:Long, @Context db:GraphDatabaseService) : Response = {
    val startNode = findNode(fromID, db)
    val endNode = findNode(toID, db)
    val path = doAstar(startNode, endNode, "next")

    response(path, expandPath = false)
  }

  @GET
  @Path("/o/{from}/{to}")
  @Produces(Array("application/json"))
  def astarOverlay(@PathParam("from") fromID:Long, @PathParam("to") toID:Long, @Context db:GraphDatabaseService) : Response = {
    val startNode = findNode(fromID, db)
    val endNode = findNode(toID, db)

    val path = time("astar") { doAstar(startNode, endNode, "next_overlay") }

    time("response") { response(path) }
  }

  @GET
  @Path("/ball")
  @Produces(Array("application/json"))
  def buildAllOverlay(@Context db:GraphDatabaseService) : Response = {
    println("Clear old overlay...")
    val engine = new ExecutionEngine(db)
    engine.execute("start n=node(*) match (n)-[r:next_overlay]-(c) delete r")

    println("Querying intersections...")
    val result = engine.execute("start n = node(*) match (n)-[:ways]-(c) where n.type = 'node' with n, count(*) as degree where degree > 1 return id(n) as id")
    val intersects = result.columnAs[Long]("id").toSeq

    println(s"intersections = ${intersects.length}")

    var count = 0
    intersects foreach { n =>
      buildOverlayOneNode(n, intersects, db)
      count += 1
      if (count % 500 == 0) {
        println(s"Progress: $count / ${intersects.length}")
      }
    }

    Response.ok().entity("{\"status\": \"ok\"}").build()
  }

  @GET
  @Path("/n/{lat}/{lon}/{type}")
  @Produces(Array("application/json"))
  def nearestQuery(@PathParam("lat") lat:Double, @PathParam("lon") lon:Double, @PathParam("type") nodeType:String, @Context db:GraphDatabaseService) : Response = {
    val rtree = if (nodeType.equals("intersect")) AStarServiceHandler.rtreeIntersect else AStarServiceHandler.rtree
    val p = new Point(lon.toFloat, lat.toFloat)
    var nodeID : Long = -1
    rtree.nearest(p, new TIntProcedure {
      def execute(value: Int) : Boolean = { nodeID = value; true }
    }, Float.MaxValue)

    import net.liftweb.json.JsonDSL._
    if (nodeID > -1) {
      val json = ("nodeID" -> db.getNodeById(nodeID).getProperty("id").asInstanceOf[Long])
      Response.ok().entity(compact(render(json))).build()
    } else {
      val json = ("nodeID" -> -1)
      Response.ok().entity(compact(render(json))).build()
    }
  }

  @GET
  @Path("/bindex")
  @Produces(Array("application/json"))
  def buildIndex(@Context db:GraphDatabaseService) : Response = {
    val engine = new ExecutionEngine(db)

    println("Building the index for all nodes...")
    val nodes : Seq[Node] = engine.execute("start n = node(*) where n.type = 'node' and n.cc_id = 2 return id(n) as id").columnAs[Long]("id").toSeq.map { db.getNodeById(_) }
    println(s"size: ${nodes.size()}")

    AStarServiceHandler.rtree.init(null)
    buildIndexForNodes(nodes, AStarServiceHandler.rtree)

    println("Building the index for intersect nodes...")
    val result = engine.execute("start n = node(*) match (n)-[:ways]-(c) where n.type = 'node' and n.cc_id=2 with n, count(*) as degree where degree > 1 return id(n) as id")
    val intersectNodes : Seq[Node] = result.columnAs[Long]("id").toSeq.map { db.getNodeById(_) }
    println(s"size: ${intersectNodes.size()}")

    AStarServiceHandler.rtreeIntersect.init(null)
    buildIndexForNodes(intersectNodes, AStarServiceHandler.rtreeIntersect)

    Response.ok().entity("ok").build()
  }

  def buildIndexForNodes(nodes: Seq[Node], rtree: RTree) = {
    nodes.zipWithIndex foreach { case (node, i) =>
      if (i % 1000 == 0)
        println(s"Progress: $i / ${nodes.size()}")

      val x = node.getProperty("lon").asInstanceOf[Double]
      val y = node.getProperty("lat").asInstanceOf[Double]
      val point = new Point(x.toFloat, y.toFloat)
      val rect = new Rectangle()
      rect.add(point)

      rtree.add(rect, node.getId().toInt)
    }
  }

  @GET
  @Path("/u/{x1}/{y1}/{x2}/{y2}/{val}")
  @Produces(Array("application/json"))
  def updateWeight(@PathParam("x1") x1:Double, @PathParam("y1") y1:Double, @PathParam("x2") x2:Double, @PathParam("y2") y2:Double, @PathParam("val") w:Double, @Context db:GraphDatabaseService) = {
    val rect = new Rectangle(x1.toFloat, y1.toFloat, x2.toFloat, y2.toFloat)
    AStarServiceHandler.rtreeRelationships.contains(rect, new TIntProcedure {
      def execute(rid: Int): Boolean = {
        val rel = db.getRelationshipById(rid.toLong)
        if (!rel.hasProperty("weight_base"))
          rel.setProperty("weight_base", rel.getProperty("weight"))

        rel.setProperty("weight", rel.getProperty("weight_base").asInstanceOf[Double] + w)
        true
      }
    })

    Response.ok().entity("ok").build()
  }

  @POST
  @Path("/b/{from}")
  @Produces(Array("application/json"))
  def buildOverlay(@PathParam("from") fromID:Long, @FormParam("intersects") intersectsStr:String, @Context db:GraphDatabaseService) : Response = {
    val intersects = intersectsStr.split(",").map(_.toLong)
    val respMap = buildOverlayOneNode(fromID, intersects, db)

    import net.liftweb.json.JsonDSL._
    Response.ok().entity(compact(render(respMap))).build()
  }

  @GET
  @Path("/br")
  @Produces(Array("application/json"))
  def buildRelationshipsIndex(@Context db: GraphDatabaseService) = {
    AStarServiceHandler.rtreeRelationships.init(null)

    val rels = GlobalGraphOperations.at(db).getAllRelationships().filter( rel =>
      rel.getType().name().equalsIgnoreCase("next") ||
      rel.getType().name().equalsIgnoreCase("next_overlay")
    ).toSeq
    println(s"size: ${rels.size()}")

    rels.zipWithIndex foreach { case (rel, i) =>
      if (i % 1000 == 0)
        println(s"Progress: $i / ${rels.size()}")

      val id = rel.getId().asInstanceOf[Int]
      val p1 = new Point(rel.getStartNode().getProperty("lon").asInstanceOf[Double].toFloat, rel.getStartNode().getProperty("lat").asInstanceOf[Double].toFloat)
      val p2 = new Point(rel.getEndNode().getProperty("lon").asInstanceOf[Double].toFloat, rel.getEndNode().getProperty("lat").asInstanceOf[Double].toFloat)
      val r = new Rectangle(p1.x, p1.y, p2.x, p2.y)

      AStarServiceHandler.rtreeRelationships.add(r, id)
    }
  }

  def buildOverlayOneNode(fromID:Long, intersects:Seq[Long], db: GraphDatabaseService) = {
    val startNode = db.getNodeById(fromID)
    val nextRelationship = new RelationshipType { def name = { "next" } }
    val nextOverlayRelationship = new RelationshipType { def name = { "next_overlay" } }

    def traverse(node: Node, visited: Seq[Long]) : Seq[Node] = {
      if (visited.contains(node.getId())) return Seq[Node]()

      node.getRelationships(nextRelationship, Direction.OUTGOING).iterator().foldLeft(Seq[Node]())((n, r) => {
        val otherNode = r.getOtherNode(node)
        if (intersects.contains(otherNode.getId()) && startNode.getId() != otherNode.getId()) {
          n :+ otherNode
        } else {
          n ++ traverse(otherNode, visited :+ node.getId())
        }
      })
    }

    val nodes = traverse(startNode, Seq[Long]()).distinct
    val tx = db.beginTx()
    try {
      nodes foreach { otherNode =>
        val rel = startNode.createRelationshipTo(otherNode, nextOverlayRelationship)
        val path = doAstar(startNode, otherNode, "next")
        rel.setProperty("weight", path.weight())
      }
      tx.success()
    } finally {
      tx.finish()
    }

    "nodes" -> nodes.map(_.getId)
  }

  def doAstar(from:Node, to:Node, rel:String) : WeightedPath = {
    var ncount = 0

    val estimateEvaluator = new EstimateEvaluator[java.lang.Double] {
      def getCost(node: Node, goal: Node): java.lang.Double = {
        val dx = node.getProperty("lat").asInstanceOf[Double] - goal.getProperty("lat").asInstanceOf[Double]
        val dy = node.getProperty("lon").asInstanceOf[Double] - goal.getProperty("lon").asInstanceOf[Double]
        ncount = ncount + 1

        dx + dy
      }
    }

    val relationship = new RelationshipType { def name = { rel } }
    val pathExpander = Traversal.pathExpanderForTypes(relationship)
    val astar = GraphAlgoFactory.aStar(
      pathExpander,
      CommonEvaluators.doubleCostEvaluator("weight"),
      estimateEvaluator
    )

    val p = astar.findSinglePath(from, to)
    p
  }

  def response(path:WeightedPath, expandPath:Boolean = true) : Response = path match {
    case null => responseEmpty()
    case _ => responsePath(path, expandPath)
  }

  def responsePath(path:WeightedPath, expandPath:Boolean) : Response = {
    import net.liftweb.json.JsonDSL._
    val pathArray = time("expand") { expandPath match {
      case true => expandFullPath(path.nodes().iterator().toSeq)
      case false => path.nodes().iterator().toSeq
    } }
    val respMap = ("path" -> pathArray.map(_.getId)) ~ ("coordinates" -> pathArray.map(toLatLon)) ~ ("weight" -> path.weight())
    Response.ok().entity(time("json"){ compact(render(respMap)) }).build()
  }

  def toLatLon(node: Node) : Seq[Double] = Seq(node.getProperty("lat").asInstanceOf[Double], node.getProperty("lon").asInstanceOf[Double])

  def expandFullPath(path:Seq[Node]) : Seq[Node] = {
    val pairs = path.zip(path.drop(1))
    pairs.map(_ match { case (id1, id2) =>
      val p = doAstar(id1, id2, "next")
      // println(s"$id1 -> $id2 expand: ${p.nodes().iterator().toSeq.map(_.getId).mkString(" -> ")}")
      p.nodes().iterator().toSeq
    }).foldLeft(Seq[Node]())((path: Seq[Node], subpath: Seq[Node]) => path.dropRight(1) ++ subpath)
  }

  def responseEmpty() : Response = {
    import net.liftweb.json.JsonDSL._
    val respMap = ("path" -> Seq[Long]()) ~ ("weight" -> 0)
    Response.ok().entity(compact(render(respMap))).build()
  }

  def findNode(id: Long, db:GraphDatabaseService) : Node = {
    val node = try {
      db.getNodeById(id)
    } catch {
      case _:Exception => db.index().forNodes("node").get("id", id).getSingle()
    }

    node match {
      case _: Node => node
      case null => throw new IllegalArgumentException(s"No node can be found by ID $id")
    }
  }

  def time[R](title: String="Block")(block: => R): R = {
    val t0 = System.nanoTime()
    val result = block    // call-by-name
    val t1 = System.nanoTime()
    println(s"$title: ${(t1 - t0) / 1e6}ms")
    result
  }
}

object AStarServiceHandler {
  private val rtree = new RTree()
  private val rtreeIntersect = new RTree()
  private val rtreeRelationships = new RTree()
}