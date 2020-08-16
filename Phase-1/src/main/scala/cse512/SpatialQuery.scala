package cse512

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * A class object where we define our own User Defined Functions.
 *
 * Reference: https://github.com/jiayuasu/CSE512-Project-Phase2-Template
 */
object SpatialQuery extends App {

  // ==================== Project Phase 1 UDF Below ====================

  def euclidean2D(pt1: Array[Double], pt2: Array[Double]): Double = {
    val pointPairs = pt1 zip pt2
    val squaredDiff = pointPairs map { case (x, y) => {
      scala.math.pow(x - y, 2)
    }
    }
    scala.math.sqrt(squaredDiff.sum)
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
    case class Point(x: Double, y: Double)

    val rect = parseCoordinateString(queryRectangle)
    val rectPtA = Point(rect(0), rect(1))
    val rectPtC = Point(rect(2), rect(3))

    val _point = parseCoordinateString(pointString)
    val point = Point(_point(0), _point(1))

    val checkX = (rectPtA.x <= point.x) && (point.x <= rectPtC.x)
    val checkY = (rectPtA.y <= point.y) && (point.y <= rectPtC.y)

    checkX && checkY
  }

  /**
   * Given two points and a distance, determine whether the two points are within the given distance by assuming
   * "all coordinates are on a planar space and calculate their Euclidean distance".
   *
   * Note: On-boundary points are considered true.
   *
   * @param pointString1 a point, e.g., "-88.331492,32.324142"
   * @param pointString2 another point, e.g., "-88.331492,32.324142"
   * @param distance     a distance
   * @return whether the two given points are within the given distance
   */
  def ST_Within(pointString1: String, pointString2: String, distance: Double): Boolean = {
    val point1 = parseCoordinateString(pointString1)
    val point2 = parseCoordinateString(pointString2)
    val distPoints = euclidean2D(point1, point2)
    distPoints <= distance
  }

  // ==================== Project Phase 1 UDF Above ====================


  /**
   * Range query: Use ST_Contains.
   *
   * Given a query rectangle R and a set of points P, find all the points within R.
   *
   * @param spark    A Spark session
   * @param dfPoints the path to input data
   * @param rect
   * @return
   */
  def runRangeQuery(spark: SparkSession, dfPoints: String, rect: String): Long = {
    // Read some data into a DataFrame
    val pointDf = readDfFromArg(dfPoints, spark)
    pointDf.createOrReplaceTempView("point")

    // Register the UDF ST_Contains
    spark.udf.register("ST_Contains", (queryRectangle: String, pointString: String) => {
      ST_Contains(queryRectangle, pointString)
    })

    // Execute an SQL
    val sqlToExec = s"SELECT * FROM point WHERE ST_Contains('$rect', point._c0)"
    val resultDf = spark.sql(sqlToExec)
    resultDf.show()

    resultDf.count()
  }

  /**
   * Range join query: Use ST_Contains.
   *
   * Given a set of Rectangles R, and a set of Points S, find all (Point, Rectangle) pairs such that the point is within the rectangle.
   *
   * @param spark        A Spark session
   * @param dfPoints     the path to input data of points
   * @param dfRectangles the path to input data of rectangles
   * @return
   */
  def runRangeJoinQuery(spark: SparkSession, dfPoints: String, dfRectangles: String): Long = {
    // Read some data into DataFrames
    val pointDf = readDfFromArg(dfPoints, spark)
    pointDf.createOrReplaceTempView("point")
    val rectangleDf = readDfFromArg(dfRectangles, spark)
    rectangleDf.createOrReplaceTempView("rectangle")

    // Register the UDF ST_Contains
    spark.udf.register("ST_Contains", (queryRectangle: String, pointString: String) => {
      ST_Contains(queryRectangle, pointString)
    })

    // Execute an SQL
    val sqlToExec = "SELECT * FROM rectangle, point WHERE ST_Contains(rectangle._c0, point._c0)"
    val resultDf = spark.sql(sqlToExec)
    resultDf.show()

    resultDf.count()
  }

  /**
   * Distance query: Use ST_Within.
   *
   * Given a point location P, and distance D in km, find all points that lie within a distance D from P.
   *
   * @param spark    a Spark session
   * @param dfPoints the path to input data of points
   * @param location an input location P
   * @param distance an input distance D
   * @return
   */
  def runDistanceQuery(spark: SparkSession, dfPoints: String, location: String, distance: String): Long = {
    // Read some data into a DataFrame
    val pointDf = readDfFromArg(dfPoints, spark)
    pointDf.createOrReplaceTempView("point")

    // Register the UDF ST_Within
    spark.udf.register("ST_Within", (pointString1: String, pointString2: String, distance: Double) => {
      ST_Within(pointString1, pointString2, distance)
    })

    // Execute an SQL
    val sqlToExec = s"SELECT * FROM point WHERE ST_Within(point._c0, '$location', $distance)"
    val resultDf = spark.sql(sqlToExec)
    resultDf.show()

    resultDf.count()
  }

  /**
   * Distance join query: Use ST_Within.
   *
   * Given a set of Points S1 and a set of Points S2, and a distance D in km,
   * find all (s1, s2) pairs such that s1 is within a distance D from s2
   * (i.e., s1 belongs to S1 and s2 belongs to S2).
   *
   * @param spark     a Spark session
   * @param dfPoints1 the path to input data of the first set of points (S1)
   * @param dfPoints2 the path to input data of the second set of points (S2)
   * @param distance  an input distance D
   * @return
   */
  def runDistanceJoinQuery(spark: SparkSession, dfPoints1: String, dfPoints2: String, distance: String): Long = {
    // Read some data into DataFrames
    val pointDf = readDfFromArg(dfPoints1, spark)
    pointDf.createOrReplaceTempView("point1")
    val pointDf2 = readDfFromArg(dfPoints2, spark)
    pointDf2.createOrReplaceTempView("point2")

    // Register the UDF ST_Within
    spark.udf.register("ST_Within", (pointString1: String, pointString2: String, distance: Double) => {
      ST_Within(pointString1, pointString2, distance)
    })

    // Execute an SQL
    val sqlToExec = s"SELECT * FROM point1 p1, point2 p2 WHERE ST_Within(p1._c0, p2._c0, $distance)"
    val resultDf = spark.sql(sqlToExec)
    resultDf.show()

    resultDf.count()
  }

  /**
   * Read the data at dataPath into DataFrame. This method is for replacing the original code to improve readability.
   *
   * @param dataPath Data to import
   * @param spark    A Spark session
   * @return A DataFrame
   */
  private def readDfFromArg(dataPath: String, spark: SparkSession): DataFrame = {
    val formatSource = "com.databricks.spark.csv"
    val delimiter = "\t"
    val haveHeader = "false"

    val pointDf = spark.read.format(formatSource)
      .option("delimiter", delimiter)
      .option("header", haveHeader)
      .load(dataPath)

    pointDf
  }
}
