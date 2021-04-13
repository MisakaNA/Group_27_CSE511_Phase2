package cse512

object HotzoneUtils {
  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    var valueArray = pointString.split(',')
    val pointX = valueArray(0).toDouble
    val pointY = valueArray(1).toDouble

    valueArray = queryRectangle.split(',')
    val rectX1 = math.min(valueArray(0).toDouble, valueArray(2).toDouble)
    val rectY1 = math.min(valueArray(1).toDouble, valueArray(3).toDouble)
    val rectX2 = math.max(valueArray(0).toDouble, valueArray(2).toDouble)
    val rectY2 = math.max(valueArray(1).toDouble, valueArray(3).toDouble)

    pointX >= rectX1 && pointX <= rectX2 && pointY >= rectY1 && pointY <= rectY2
  }
}
