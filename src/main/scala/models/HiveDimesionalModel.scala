import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object HiveDimensionalModel {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("NYC Taxi Dimensional Model")
      .master("local[*]")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .enableHiveSupport()
      .getOrCreate()
    
    try {
      // Load processed taxi data
      val processedDF = spark.read.parquet("hdfs://localhost:9000/nyc_taxis_data/processed")
      
      println("Available Columns:")
      processedDF.printSchema()
      
      // Create Time Dimension
      val timeDimension = createTimeDimension(processedDF)
      
      // Create Distance Dimension
      val distanceDimension = createDistanceDimension(processedDF)
      
      // Create Fact Table
      val factTable = createFactTable(processedDF, timeDimension, distanceDimension)
      
      // Save Dimensions and Fact Table
      timeDimension.write
        .mode(SaveMode.Overwrite)
        .parquet("hdfs://localhost:9000/nyc_taxis_data/time_dimension")
      
      distanceDimension.write
        .mode(SaveMode.Overwrite)
        .parquet("hdfs://localhost:9000/nyc_taxis_data/distance_dimension")
      
      factTable.write
        .mode(SaveMode.Overwrite)
        .partitionBy("time_id")
        .parquet("hdfs://localhost:9000/nyc_taxis_data/fact_table")
      
      println("Dimensional model created successfully!")
    } catch {
      case e: Exception => 
        println(s"Error creating dimensional model: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
  
  def createTimeDimension(df: DataFrame): DataFrame = {
    val timeDim = df.select(

      col("pickup_datetime"),
      col("pickup_date"),

      hour(col("pickup_datetime")).as("hour_of_day"),
      dayofweek(col("pickup_datetime")).as("day_of_week"),

      month(col("pickup_datetime")).as("month"),
      year(col("pickup_datetime")).as("year"),

      // Time-based categorizations
      when(hour(col("pickup_datetime")) >= 6 && hour(col("pickup_datetime")) < 12, "morning")
        .when(hour(col("pickup_datetime")) >= 12 && hour(col("pickup_datetime")) < 18, "afternoon")
        .when(hour(col("pickup_datetime")) >= 18 && hour(col("pickup_datetime")) < 22, "evening")
        .otherwise("night").as("time_of_day"),

      when(dayofweek(col("pickup_datetime")).isin(6, 7), "weekend")
        .otherwise("weekday").as("day_type")

    ).distinct()
    
    val windowSpec = Window.orderBy("pickup_datetime")
    timeDim.withColumn("time_id", row_number().over(windowSpec))
  }
  
  def createDistanceDimension(df: DataFrame): DataFrame = {
    // Distance categories
    val distanceDim = df.select(
      col("trip_distance"),
      when(col("trip_distance") < 1, "Very Short")
        .when(col("trip_distance") >= 1 && col("trip_distance") < 3, "Short")
        .when(col("trip_distance") >= 3 && col("trip_distance") < 5, "Medium")
        .when(col("trip_distance") >= 5 && col("trip_distance") < 10, "Long")
        .otherwise("Very Long").as("distance_category")
    ).distinct()
    
    val windowSpec = Window.orderBy("trip_distance")
    distanceDim.withColumn("distance_id", row_number().over(windowSpec))
  }
  
  def createFactTable(
    processedDF: DataFrame, 
    timeDim: DataFrame, 
    distanceDim: DataFrame
  ): DataFrame = {
    // Join with time dimension to get time_id
    val factWithTimeId = processedDF.join(
      timeDim.select("time_id", "pickup_datetime"),
      Seq("pickup_datetime"),
      "left"
    )
    
    // Join with distance dimension
    val factWithDistanceId = factWithTimeId.join(
      distanceDim.select("distance_id", "trip_distance", "distance_category"),
      Seq("trip_distance"),
      "left"
    )
    
    // Select relevant columns
    factWithDistanceId.select(
      col("time_id"),
      col("distance_id"),
      col("trip_distance"),
      col("trip_duration_minutes"),
      col("fare_amount"),
      col("cost_per_mile"),
      monotonically_increasing_id().as("trip_id")
    )
  }
}