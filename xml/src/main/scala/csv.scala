import java.io.{BufferedWriter, FileWriter}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Random
import au.com.bytecode.opencsv.CSVWriter

object csv{

  def writeSensor(sensors: Seq[(String, Option[String], String)], path: String) = {
    val csvWriter = newCsvWrite(path)

    val csvFields = Array("Sensor", "Equipment", "p&id")

    var listOfRecords = new ListBuffer[Array[String]]()
    listOfRecords += csvFields

    sensors.map{ s =>
      listOfRecords += Array(s._1, s._2.getOrElse(""), pidName(s._3))
    }

    csvWriter.writeAll(listOfRecords.toList)

    csvWriter.close()
  }

  def writeEquipment(equipments: Seq[(Equipment, String)], path: String) = {
    val csvWriter = newCsvWrite(path)
    val csvFields = Array("TagName", "ComponentClass", "ComponentName", "p&id")

    var listOfRecords = new ListBuffer[Array[String]]()
    listOfRecords += csvFields

    equipments.map{ equipment =>
      val e = equipment._1
      listOfRecords += Array(e.tagName, e.componentClass, e.componentName, pidName(equipment._2))
    }

    csvWriter.writeAll(listOfRecords.toList)
    csvWriter.close()
  }

  private def newCsvWrite(path: String) = {
    val file = new BufferedWriter(new FileWriter(path))
    new CSVWriter(file)
  }
  private def pidName(filePath: String): String = {
    filePath.split("/").last
  }

}