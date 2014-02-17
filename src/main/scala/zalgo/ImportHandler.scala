package zalgo

import net.liftweb.json._
import net.liftweb.json.Extraction.decompose

import javax.ws.rs._
import javax.ws.rs.core.{Response, Context}

import org.neo4j.graphdb.{Transaction, RelationshipType, GraphDatabaseService}
import scala.xml.pull._
import scala.io.Source
import org.neo4j.cypher.ExecutionEngine
import org.apache.cxf.jaxrs.ext.multipart.{MultipartBody, Multipart}

@Path("/import")
class ImportHandler {
  implicit val formats = DefaultFormats

  case class ImportInfo(node: Int, way: Int)
  case class XMLProcessInfo(importInfo: ImportInfo, nodes: Seq[XMLEvent], db: GraphDatabaseService, tx: Option[Transaction], nds: Seq[XMLEvent]) {
    def update(node: Int = importInfo.node, way: Int = importInfo.way, nodes: Seq[XMLEvent] = nodes, tx: Option[Transaction] = tx, nds: Seq[XMLEvent] = nds) : XMLProcessInfo = {
      XMLProcessInfo(ImportInfo(node, way), nodes, db, tx, nds)
    }
  }

  @POST
  @Path("/nodes")
  def importNode(@FormParam("path") path:String, @Context db:GraphDatabaseService) : Response = {
    val f = Source.fromFile(path)
    val xml = f.getLines().mkString("\n")
    f.close()

    println(s"Importing XML... (${xml.length()} bytes)")
    val init = XMLProcessInfo(ImportInfo(0, 0), Seq[XMLEvent](), db, Some(db.beginTx()), Seq[XMLEvent]())
    val processed = new XMLEventReader(Source.fromString(xml)).foldLeft(init)(eventHandler)
    processed.tx foreach { tx =>
      tx.success()
      tx.finish()
    }

    println("Cleanup duplicate relationships...")
    val engine = new ExecutionEngine(db)
    val tx = db.beginTx()
    try {
      engine.execute("start r=relationship(*) match s-[r]->e with s,e,type(r) as typ, tail(collect(r)) as coll foreach(x in coll:delete x)")
      tx.success()
    } finally {
      tx.finish()
    }

    println(s"Done: ${processed.importInfo}")
    Response.ok().entity(renderJson(processed.importInfo)).build()
  }

  def renderJson = decompose _ andThen render andThen compact

  def handleXMLNode(proc: XMLProcessInfo) : XMLProcessInfo = {
    proc.nodes.last match {
      case EvElemStart(_, "node", _, _) => {
        handleNode(proc.nodes.last, proc.db)
        proc.update(node = proc.importInfo.node + 1)
      }
      case EvElemStart(_, "way", _, _)  => {
        handleWay(proc.nodes, proc.nds, proc.db)
        proc.update(way = proc.importInfo.way + 1, nds = Seq[XMLEvent]())
      }
      case _ => proc
    }
  }

  def handleNode(ev: XMLEvent, db: GraphDatabaseService) = {
    assert(ev.isInstanceOf[EvElemStart])
    val el = ev.asInstanceOf[EvElemStart]

    val node = db.createNode()
    el.attrs.get("id") foreach { id =>
      node.setProperty("id", id.toString.toLong)
    }
    Seq("lat", "lon") foreach { key =>
      el.attrs.get(key) foreach { id =>
        node.setProperty(key, id.toString.toDouble)
      }
    }
    node.setProperty("type", "node")
    db.index().forNodes("node").add(node, "id", node.getProperty("id"))
  }

  def handleWay(nodes: Seq[XMLEvent], nds: Seq[XMLEvent], db: GraphDatabaseService) : Unit = {
    val nodesRelationship = new RelationshipType { def name = { "nodes" }}
    val waysRelationship  = new RelationshipType { def name = { "ways" }}
    val nextRelationship = new RelationshipType { def name = { "next" }}

    val wayNodeIDs = nds.filter(_ match {
      case EvElemStart(_, "nd", _, _) => true
      case _ => false
    }).map(_.asInstanceOf[EvElemStart].attrs.get("ref").get.toString.toLong)

    // :( Ugly code
    if (!wayNodeIDs.forall( nid => db.index().forNodes("node").get("id", nid).getSingle() != null ))
      return

    assert(nodes.last.isInstanceOf[EvElemStart])
    val el = nodes.last.asInstanceOf[EvElemStart]
    val wayNode = db.createNode()
    val way_id = el.attrs.get("id").get.toString.toLong

    // create way node
    wayNode.setProperty("id", way_id)
    wayNode.setProperty("type", "way")
    db.index().forNodes("way").add(wayNode, "id", wayNode.getProperty("id"))

    // create relationships between way and nodes
    wayNodeIDs.zipWithIndex foreach { case (nid, i) =>
      val node = db.index().forNodes("node").get("id", nid).getSingle()
      node.setProperty(s"way_${way_id}_sort", i)
      wayNode.createRelationshipTo(node, nodesRelationship)
      node.createRelationshipTo(wayNode, waysRelationship)
    }

    val cons = wayNodeIDs.zip(wayNodeIDs.drop(1))
    cons foreach { case(id1, id2) =>
      val n1 = db.index().forNodes("node").get("id", id1).getSingle()
      val n2 = db.index().forNodes("node").get("id", id2).getSingle()

      val c1 = DistanceUtil.Coord(n1.getProperty("lat").asInstanceOf[Double], n1.getProperty("lon").asInstanceOf[Double])
      val c2 = DistanceUtil.Coord(n2.getProperty("lat").asInstanceOf[Double], n2.getProperty("lon").asInstanceOf[Double])
      val weight = DistanceUtil.spheredist(c1, c2)

      val rout = n1.createRelationshipTo(n2, nextRelationship)
      val rin = n2.createRelationshipTo(n1, nextRelationship)
      val relationships = Seq(rin, rout)
      relationships foreach { rel =>
        rel.setProperty("weight", weight)
      }
    }
  }

  def eventHandler(proc: XMLProcessInfo, ev: XMLEvent) : XMLProcessInfo = {
    ev match {
      case EvElemStart(_, "nd", _, _) => proc.update(nds = proc.nds :+ ev)
      case EvElemStart(_, _, _, _) => proc.update(nodes = proc.nodes :+ ev)
      case EvElemEnd(_, "nd") => proc
      case EvElemEnd(_, label) =>
        val tx : Option[Transaction] = if ((proc.importInfo.node + proc.importInfo.way) % 1000 == 0) {
          println(s"Progress Report: ${proc.importInfo}")
          proc.tx foreach { tx =>
            tx.success()
            tx.finish()
          }

          Some(proc.db.beginTx())
        } else {
          proc.tx
        }
        val txProc = proc.update(tx = tx)
        val newProc = handleXMLNode(txProc)

        newProc.update(nodes = proc.nodes.reverse.dropWhile( _ match {
          case e: EvElemStart => e.label != label
          case _ => true
        } ).tail.reverse)
      case _ => proc
    }
  }
}