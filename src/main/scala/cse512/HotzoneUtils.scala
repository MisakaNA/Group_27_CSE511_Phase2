package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    var valueArray = pointString.split(',')
    val pointX = valueArray(0).asInstanceOf[Double]
    val pointY = valueArray(1).asInstanceOf[Double]

    valueArray = queryRectangle.split(',')
    val rectX1 = valueArray(0).asInstanceOf[Double]
    val rectY1 = valueArray(1).asInstanceOf[Double]
    val rectX2 = valueArray(2).asInstanceOf[Double]
    val rectY2 = valueArray(3).asInstanceOf[Double]

    pointX >= rectX1 && pointX <= rectX2 && pointY >= rectY1 && pointY <= rectY2
  }
}
