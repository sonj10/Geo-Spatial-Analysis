package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ", (pickupTime: String) => ((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("SELECT CalculateX(nyctaxitrips._c5), CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) FROM nyctaxitrips")
    pickupInfo = pickupInfo.toDF(Seq("x", "y", "z"): _*)

    // Define the min and max of x, y, z
    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

    // Filter input data to reduce noises
    pickupInfo
      .filter(s"($minX <= x) AND (x <= $maxX) AND ($minY <= y) AND (y <= $maxY) AND ($minZ <= z) AND (z <= $maxZ)")
      .createOrReplaceTempView("tmp_result")

    // Count duplicates for each cell (this will be the term "x" in the equation of the G score)
    val dfAttr = spark.sql("SELECT x, y, z, COUNT(*) AS attr FROM tmp_result GROUP BY x, y, z")
    dfAttr.createOrReplaceTempView("tmp_result")

    // Compute the X bar
    val xBar = dfAttr.agg(sum("attr")).first().getLong(0).toDouble / (numCells.toDouble)
    // Compute the SD
    val dfSquared = spark.sql("SELECT power(attr, 2) AS squared FROM tmp_result")
    val sd = math.sqrt(dfSquared.agg(sum("squared")).first().getDouble(0) / numCells.toDouble - math.pow(xBar, 2))

    // Find neighbor cells and compute the total "hotness"
    spark.udf.register(
      "ST_Within", (x1: Double, y1: Double, z1: Double, x2: Double, y2: Double, z2: Double) => {
        val p1 = HotcellUtils.Point(x1, y1, z1)
        val p2 = HotcellUtils.Point(x2, y2, z2)
        HotcellUtils.ST_Within(p1, p2)
      }
    )
    val dfNeighbors = spark.sql(
      "SELECT " +
        "    A.x AS x, A.y AS y, A.z AS z, " +
        "    COUNT(B.attr) AS weight, SUM(B.attr) AS attrSum " +
        "FROM tmp_result A, tmp_result B " +
        "WHERE ST_Within(B.x, B.y, B.z, A.x, A.y, A.z) " +
        "GROUP BY A.x, A.y, A.z")
    dfNeighbors.createOrReplaceTempView("tmp_result")

    // Compute the G Score
    spark.udf.register("getGScore", (attrVal: Int, n: Int, mean: Double, s: Double, weight: Double) =>
      (attrVal.toDouble - (mean * weight)) / (s * math.sqrt(((n.toDouble * weight) - (weight * weight)) / (n.toDouble - 1.0)))
    )
    val dfScores = spark.sql(
      s"SELECT x, y, z, getGScore(attrSum, $numCells, $xBar, $sd, weight) AS g FROM tmp_result")
    dfScores.createOrReplaceTempView("tmp_result")

    // Sort cells by the G Scores
    var finalRes = spark.sql(s"SELECT x, y, z, g FROM tmp_result ORDER BY g DESC")
    finalRes = finalRes.drop("g")
    finalRes
  }
}
