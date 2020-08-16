package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession, functions}

object HotzoneAnalysis {

  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  /**
   * Perform a range join operation on the given rectangles and points and find out the number of points within
   * each rectangle. The "hotness" of a rectangle is defines as the number of points it includes.
   *
   * Note: This is almost the same as the project phase 1 where we define "ST_Contains".
   *
   * @param spark         a spark session
   * @param pointPath     the path to a data set of points
   * @param rectanglePath the path to a data set of rectangles
   * @return the result in a data frame
   */
  def runHotZoneAnalysis(spark: SparkSession, pointPath: String, rectanglePath: String): DataFrame = {

    // Load the input data of points into a temp view
    var pointDf = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", ";")
      .option("header", "false")
      .load(pointPath);
    pointDf.createOrReplaceTempView("point")

    // Define a UDF "trim" for parsing data of points
    spark.udf.register(
      "trim",
      (string: String) => (
        string.replace("(", "").replace(")", "")))
    // Use spark.sql to parse points (which is on the 6th column)
    pointDf = spark.sql("select trim(_c5) as _c5 from point")

    // Replace the temp view we just created
    pointDf.createOrReplaceTempView("point")

    // Load rectangle data
    val rectangleDf = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", "\t")
      .option("header", "false")
      .load(rectanglePath);
    rectangleDf.createOrReplaceTempView("rectangle")

    // Define another UDF ST_Contains for the join operation
    spark.udf.register(
      "ST_Contains",
      (queryRectangle: String, pointString: String) => (
        HotzoneUtils.ST_Contains(queryRectangle, pointString)))
    // Join rectangles and points by ST_Contains
    val joinDf = spark.sql(
      "select rectangle._c0 as rectangle, point._c5 as point " +
        "from rectangle, point " +
        "where ST_Contains(rectangle._c0,point._c5)")
    joinDf.createOrReplaceTempView("joinResult")

    // Group by rectangle and count
    joinDf.groupBy("rectangle").count().orderBy(functions.asc("rectangle"))
  }

}
