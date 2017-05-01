import org.anormcypher.Neo4jConnection

import scala.xml.{Node, NodeSeq}
import scala.collection.mutable.HashMap

trait Component {
  val id: String
  val tagName: String
}

/*
the relation includes:
connection
equipment -> nozzle
PipingNetworkSegment -> ProcessInstrument
ConnectorSymbol(Including PipeConnectorSymbol & SignalConnectorSymbol) -> ToFromText , LinkedPersistentID
 */

case class Nozzle(id: String, tagName: String, ComponentClass: String) extends Component

case class SingleEquipment(id: String, tagName: String, ComponentClass: String) extends Component

case class Equipment(id: String, tagName: String, componentClass: String,
                     componentName: String, childs: Seq[Component]) extends Component

case class Instrument(id: String, tagName: String, processInstruFunc: Option[ProcessInstruFunc],
                      componentClass: String, componentName: String) extends Component

case class ProcessInstruFunc(id: String, tagName: String) extends Component

// only in mimosa?
case class PipingNetworkSegment(id: String, tagName: String, processInstruments: Seq[ProcessInstruFunc])

//include PipeConnectorSymbol and signalConnectorSymbol
case class ConnectorSymbol(id: String, tagName: String, linkedTo: String, ToFromText: Seq[String])

case class Connection(fromId: String, toId: String)

class Parser(implicit val connection: Neo4jConnection) {
  implicit def nodeToString(node: NodeSeq): String = node.text

  def equipments(node: Node): Seq[Equipment] = {
    val equipements = node \ "Equipment"
    equipements.map ( e => {
      val id = e \ "@ID"
      val tagName = e \ "@TagName"
      val componentClass = e \ "@ComponentClass"
      val componentName = e \ "@ComponentName"

      val childNodes = e \ "Equipment" ++ e \ "Nozzle"
      val childs = childNodes.map(parseChild)
      Equipment(id, tagName, componentClass, componentName, childs)
    })
  }

  // parse child equipment or nozzle under equipment
  private def parseChild(node: Node): Component = {
    node.label match {
      case "Equipment" => SingleEquipment(node \ "@ID", node \ "@TagName", node \ "@ComponentClass")
      case "Nozzle" => Nozzle(node \ "@ID", node \ "@TagName", node \ "@ComponentClass")
    }
  }

  def instruments(node: Node): Seq[Instrument] = {
    // for DEXPI

    val dexpoInstrumentsNode = node \ "InstrumentationLoopFunction"

    val dexpoInstruments =  dexpoInstrumentsNode.map {ins =>
      val associateId = ins \ "Association" \ "@ItemID"
      val associatedNode = node \\ "ProcessInstrumentationFunction" filter ( func => (func \ "@ID") == associateId)
      val proceInstruFunc = ProcessInstruFunc(associateId, associatedNode \ "@TagName")

      Instrument(ins \ "@ID", ins \ "@TagName",  Some(proceInstruFunc), ins \ "@ComponentClass", ins \ "@ComponentName" )
    }

    // for MIMOSA
    val mimosaSensors = (node \ "InstrumentComponent")
      //.filter(n => n.attributes.exists(_.value.text.startsWith("DCS - Main Panel")))
    val mimosaInstruments = mimosaSensors.map(ins => Instrument(ins \ "@ID", ins \ "@TagName", None,
      ins \ "@ComponentClass", ins \ "@ComponentName" ))
    dexpoInstruments ++ mimosaInstruments
  }

  def pipingNetworkSegment(node: Node): Seq[PipingNetworkSegment] = {
    val segmentNode = node \\ "PipingNetworkSegment"
    segmentNode.map {n =>
      val id = n \ "@ID"
      val name = n \ "@TagName"

      val processInstrumentNodes = n \ "ProcessInstrument"
      val processInstruments = processInstrumentNodes.map {nInstrument =>
        ProcessInstruFunc(nInstrument \ "@ID", nInstrument \ "@TagName")}

      PipingNetworkSegment(id, name , processInstruments)
    }
  }

  def connections(node: Node): Seq[Connection] = {
    val connections = node \\ "Connection"
    connections.map( conn => Connection(conn \ "@FromID", conn \ "@ToID"))
  }

  def pipeConnectorSymbol(node: Node): Seq[ConnectorSymbol] = {
    val pipeConnectorSybmbol = node \\ "PipeConnectorSymbol" ++ node \\ "SignalConnectorSymbol"

    pipeConnectorSybmbol.map {s =>
      val id =  s \ "@ID"
      val name = s \ "TagName"
      val toFromText = (s \\  "GenericAttribute").
        filter(a => a.attributes.exists(_.value.text == "ToFromText")).
        map(_\ "@Value").map(_.text)

      val linkedTo = s \ "CrossPageConnection" \ "LinkedPersistentID" \ "@Identifier"
      ConnectorSymbol(id, name, linkedTo, toFromText)
    }
  }

  def ids(node: Node): Seq[String] = {
    (node \\ "@ID").map(_.text)
  }

}
