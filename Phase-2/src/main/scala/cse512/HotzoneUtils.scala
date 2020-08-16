package cse512

object HotzoneUtils {

  case class Point(x: Double, y: Double)

  /**
   * Given a rectangle and a point, determine whether the point is in the rect.
   *
   * Note: On-boundary points are considered true. The passed rect follows the format where
   * the second point is on the bottom left of the first point.
   *
   * @param queryRectangle a rect, e.g., "-155.940114,19.081331,-155.618917,19.5307"
   * @param pointString    a point, e.g., "-88.331492,32.324142"
   * @return whether the point is in the rect
   */
  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    val rect = parseCoordinateString(queryRectangle)
    val rectPtA = Point(rect(0), rect(1))
    val rectPtC = Point(rect(2), rect(3))

    val _point = parseCoordinateString(pointString)
    val point = Point(_point(0), _point(1))

    ST_Contains(point, rectPtA, rectPtC)
  }

  def ST_Contains(point: Point, rectPtA: Point, rectPtC: Point): Boolean = {
    var result: Boolean = false

    if ((rectPtA.x <= rectPtC.x) && (rectPtA.y <= rectPtC.y)) {
      result =
        (rectPtA.x <= point.x) && (point.x <= rectPtC.x) &&
          (rectPtA.y <= point.y) && (point.y <= rectPtC.y)
    } else if ((rectPtA.x >= rectPtC.x) && (rectPtA.y >= rectPtC.y)) {
      result =
        (rectPtC.x <= point.x) && (point.x <= rectPtA.x) &&
          (rectPtC.y <= point.y) && (point.y <= rectPtA.y)
    } else if ((rectPtA.x <= rectPtC.x) && (rectPtA.y >= rectPtC.y)) {
      result =
        (rectPtA.x <= point.x) && (point.x <= rectPtC.x) &&
          (rectPtC.y <= point.y) && (point.y <= rectPtA.y)
    } else if ((rectPtA.x >= rectPtC.x) && (rectPtA.y <= rectPtC.y)) {
      result =
        (rectPtC.x <= point.x) && (point.x <= rectPtA.x) &&
          (rectPtA.y <= point.y) && (point.y <= rectPtC.y)
    }
    result
  }


  /**
   * Parse the given string and return an array of double numbers.
   *
   * @param aString a formatted string containing double numbers
   * @return an array of extracted numbers
   */
  def parseCoordinateString(aString: String): Array[Double] = {
    val sepRegex = ","
    for (p <- aString.split(sepRegex)) yield p.stripMargin.toDouble
  }
}
