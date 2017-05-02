/**
  * Created by mary on 4/26/17.
  */

import org.anormcypher._

trait db {
  implicit val ec = scala.concurrent.ExecutionContext.global

  def insertId(id: String)(implicit connection: Neo4jConnection): Boolean = {
    val insert: String = s"""create (n:Normal {id:"$id"})"""
    val r = Cypher(insert).execute()
    r
  }

  def insertEquipment(equipment: Equipment)(implicit connection: Neo4jConnection): Seq[Boolean] = {
    val name = equipment.tagName
    val id = equipment.id
    val update: String = s"""match (n {id: "$id"}) with n remove n:Normal set n:Equipment,  n.name = "$name" """
    Cypher(update).execute()

    equipment.childs.map(c => {
      insertSimpleEquipment(c)
      insertConnection("CHILD_OF", c.id, id)
    })
  }

  def insertInstrument(instru: Instrument)(implicit connection: Neo4jConnection): Option[Boolean]

  def insertInstrumentWithLabel(instru: Instrument, label: String)(implicit connection: Neo4jConnection): Option[Boolean] = {
    val name = instru.tagName
    val id = instru.id
    val compClass = instru.componentClass
    val compName = instru.componentName
    val insert: String =
      s"""
         match (n {id: "$id"}) with n remove n:Normal set n:$label,
         n.name = "$name", n.componentClass = "$compClass", n.componentName = "$compName"
       """.stripMargin
    Cypher(insert).execute()

    instru.processInstruFunc.map{func => {
      insetProcessInstruFunc(func)
      insertConnection("IS_A_COLLECTION_INCLUDING", id, func.id)
    }}
  }

  def insertPipingNetwotkSegment(segment: PipingNetworkSegment)(implicit connection: Neo4jConnection) = {
    val name = segment.tagName
    val id = segment.id
    val processInstruments = segment.processInstruments

    val insert: String =s""" match (n {id: "$id"}) with n remove n:Normal set n:PipingNetworkSegment, n.name = "$name""""
    Cypher(insert).execute()

    processInstruments.map(p => {
      val insertProcess: String =
        s""" match (n {id: "${p.id}"}) with n
           |remove n:Normal set n:ProcessInstrument,
           |n.name = "${p.tagName}"""".stripMargin
      Cypher(insertProcess).execute()

      insertConnection("CHILD_OF", p.id, id)
    })

  }

  def insetProcessInstruFunc(proc: ProcessInstruFunc)(implicit connection: Neo4jConnection): Boolean = {
    val name = proc.tagName
    val id = proc.id
    val insert: String =
      s"""match (n {id: "${id}"}) with n remove n:Norma set n:ProcessInstruFunc, n.name = "$name"
       """.stripMargin
    Cypher(insert).execute()
  }

  private def insertSimpleEquipment(comp: Component)(implicit connection: Neo4jConnection) = {
    val name = comp.tagName
    val id = comp.id
    val compType = comp match {
      case SingleEquipment(_, _, _) => "ChildEquipment"
      case Nozzle(_,_,_) => "Nozzle"
    }
    val insert: String =s"""match (n {id: "${id}"}) with n remove n:Normal set n:$compType, n.name = "$name""""
    Cypher(insert).execute()
  }

  def insertConnectorSybmbol(connectorSymbol: ConnectorSymbol)(implicit connection: Neo4jConnection) = {
    val id = connectorSymbol.id
    val name = connectorSymbol.tagName
    val insert: String =s"""match (n {id: "${id}"}) with n remove n:Normal set n:ConnectorSymbol, n.name = "$name""""
    Cypher(insert).execute()

    val a = insertConnection("LINK_TO_PAGE", id, connectorSymbol.linkedTo)
    connectorSymbol.ToFromText.map{ t =>
      val textID = getIdByName(t)
      val m = insertConnection("FROM_TO_TEXT", id, textID)
    }
  }

  def insertConnection(connType: String, fromId: String, toId: String)
                                   (implicit connection: Neo4jConnection): Boolean = {
    val insertConn =
      s"""MATCH (from {id:'${fromId}'}), (to {id:'${toId}'})
         |CREATE (from)-[:$connType]->(to)""".stripMargin
    Cypher(insertConn).execute()
  }

  private def getIdByName(name: String)(implicit connection: Neo4jConnection): String = {
    val req = Cypher(s"""match (n {name:"$name"}) return n.id as id""")
    val stream = req()
    stream.map(row => {row[String]("id")}).toList.headOption.getOrElse("")
  }

  def getEquipments(implicit connection: Neo4jConnection): List[String] = {
    val req = Cypher(s"""match (n:Equipment) return n.name""")
    val stream = req()
    stream.map(row => {row[String]("n.name")}).toList
  }

  def equipmentForInstrument(instruId: String)(implicit connection: Neo4jConnection): Option[String] = {
    val equipment = Cypher( s"""MATCH p=shortestPath((instrument:Sensor {id:"$instruId"})-[*]-(eq:Equipment))
                            RETURN eq.name as name
                            order by length(p)
                            limit 1""")
    equipment.apply().map(_[String]("name")).toList.headOption
  }
  def clean(implicit connection: Neo4jConnection) = {
    Cypher("""MATCH (n) DETACH DELETE n""").execute()
  }
}

class DexpiDb extends db {

  def insertInstrument(instru: Instrument)(implicit connection: Neo4jConnection): Option[Boolean] = {
    val label = "Sensor"
    insertInstrumentWithLabel(instru, "Sensor")
  }
}

class MimosaDb extends db {
  def insertInstrument(instru: Instrument)(implicit connection: Neo4jConnection): Option[Boolean] = {
    val label = instru.componentName match {
      case name if name.startsWith("DCS - Main Panel") => "Sensor"
      case _ => "Instrument"
    }
    insertInstrumentWithLabel(instru, label)
  }
}