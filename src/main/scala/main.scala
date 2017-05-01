import org.anormcypher.{Cypher, Neo4jConnection, Neo4jREST}
import play.api.libs.ws.ning

import scala.xml.XML

case class xml(fileName: String, ids: Seq[String], equipments: Seq[Equipment],
               instruments: Seq[Instrument],
               pipingNetworkSegment: Seq[PipingNetworkSegment],
               connections: Seq[Connection],
               connectorSymbols: Seq[ConnectorSymbol])

object main extends App {
  val wsclient = ning.NingWSClient()
  implicit val connection: Neo4jConnection = Neo4jREST()(wsclient)

  val filenames = args
 // val filenames = Seq("""/Users/mary/Downloads/worleyparsons_20121209/11-004-Ingr.xml""")
  val xmlFiles = filenames.map (f => (f, XML.load(f)))
  val parser = new Parser()

  val xmls =  xmlFiles.map{f =>
    val x = f._2
    val ids = parser.ids(x)
    val equipments: Seq[Equipment] = parser.equipments(x)
    val instruments: Seq[Instrument] =  parser.instruments(x)
    val pipingNetworkSegment: Seq[PipingNetworkSegment] = parser.pipingNetworkSegment(x)
    val connections = parser.connections(x)
    val connectorSymbols =  parser.pipeConnectorSymbol(x)
    xml(f._1, ids, equipments,  instruments, pipingNetworkSegment, connections, connectorSymbols)
  }

  // insert nodes
  xmls.foreach{x =>
    x.ids.map(db.insertId)
    x.equipments.map(db.insertEquipment)
    x.instruments.map(db.insertInstrument)
    x.pipingNetworkSegment.map(db.insertPipingNetwotkSegment)
  }

  // insert connections after nodes
  xmls.foreach { x =>
    x.connectorSymbols.map(db.insertConnectorSybmbol)
    x.connections.map(c => db.insertConnection("Connection", c.fromId, c.toId))
  }


  // query
  val sensors = xmls.map{x =>
    x.instruments.filter(_.componentName.startsWith("DCS - Main Panel")).map { s =>
      (s.tagName, db.equipmentForInstrument(s.id), x.fileName)
    }
  }.flatten
  csv.writeSensor(sensors, "./sensors.csv")

  val equipments = xmls.map(x =>
    x.equipments.map(e => (e, x.fileName))
  ).flatten

  csv.writeEquipment(equipments, "./equipments.csv")

  db.clean

  wsclient.close()
}