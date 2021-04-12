package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

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
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

    // YOU NEED TO CHANGE THIS PART
    pickupInfo.createOrReplaceTempView("pickupView")

    val filterCellDf = spark.sql(s"""
                                SELECT *
                                FROM  tempView
                                WHERE x >= $minX AND x <= $maxX
                                  AND y >= $minY AND y <= $maxY
                                  AND z >= $minZ AND z <= $maxZ""")
    filterCellDf.createOrReplaceTempView("filterView")

    val countDf = spark.sql("SELECT x, y, z, COUNT(*) AS pointNum FROM filterView GROUP BY x, y, z")
    countDf.createOrReplaceTempView("countView")

    val sumDf = spark.sql("SELECT SUM(pointNum) FROM countView")
    sumDf.createOrReplaceTempView("sumView")

    val squareSumDf = spark.sql("SELECT SUM(SQUARE(pointNum)) FROM countView")
    squareSumDf.createOrReplaceTempView("squareSumView")

    val avgX = sumDf.first().getDouble(0) / numCells
    var S = math.sqrt(squareSumDf.first().getDouble(0) / numCells - math.pow(avgX, 2.0))

    spark.udf.register("sumSpatialWeight", (x: Int, y: Int, z: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int) =>
      HotcellUtils.getNeighbors(x: Int, y: Int, z: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int))

    val neighborDf = spark.sql(s"""
                              SELECT countView2.x AS x, countView2.y AS y, countView2.z AS z,
                                     SUM(countView1.pointNum) AS sigma_WijXj,
                                     sumSpatialWeight(countView2.x, countView2.y, countView2.z, $minX, $maxX, $minY, $maxY, $minZ, $maxZ) AS sigma_Wij
                              FROM countView AS countView1, countView AS countView2
                              WHERE (countView1.x = countView2.x - 1 OR countView1.x = countView2.x OR countView1.x = countView2.x + 1)
                                AND (countView1.y = countView2.y - 1 OR countView1.y = countView2.y OR countView1.y = countView2.y + 1)
                                AND (countView1.z = countView2.z - 1 OR countView1.z = countView2.z OR countView1.z = countView2.z + 1)
                              GROUP BY countView2.z, countView2.y, countView2.x
                              ORDER BY countView2.z, countView2.y, countView2.x""")
    neighborDf.createOrReplaceTempView("neighborView")

    spark.udf.register("G_Score", (avgX: Double, S: Double, sigma_WijXj: Double, sigma_Wij: Double, numCells: Double) =>
      HotcellUtils.getGScore(avgX: Double, S: Double, sigma_WijXj: Double, sigma_Wij: Double, numCells: Double))

    val GScoreDf = spark.sql(s"""
                            SELECT x, y, z,
                                   G_Score($avgX, $S, sigma_WijXj, sigma_Wij, $numCells) AS G
                            FROM neighborView
                            ORDER BY G DESC""")
    GScoreDf.createOrReplaceTempView("GScoreView")

    val res = spark.sql("SELECT x, y, z FROM GScoreView")
    res.createOrReplaceTempView("resView")

    res // YOU NEED TO CHANGE THIS PART
  }
}
