package dealLog.util

import net.sf.json.xml.XMLSerializer

object XMLLogParse {
  def xmlParse(data : String) : String = {
    val xmlSerializer = new XMLSerializer
    xmlSerializer.setTypeHintsCompatibility(false)
    xmlSerializer.setTypeHintsEnabled(false)
    println(data)
    xmlSerializer.read(data).toString()
  }
}
