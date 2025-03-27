import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object DataProcessor {
  def main(args: Array[String]): Unit = {
    // More flexible path handling
    val inputPath = if (args.length > 0) args(0) else "hdfs://localhost:9000/nyc_taxis_data/raw"
    val outputPath = if (args.length > 1) args(1) else "hdfs://localhost:9000/nyc_taxis_data/processed"

    // Initialize Spark session with memory optimization
    val spark = SparkSession.builder()
        .appName("NYC Taxi Data Processor")
        // 2 cores
        .master("local[2]")
        
        // Memory Tuning
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.memory.fraction", "0.6")
        .config("spark.memory.storageFraction", "0.1")
        
        // Memory Management Configurations
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "512m")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        
        // Shuffle and Performance Optimizations
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        
        // Compression and Performance
        .config("spark.sql.parquet.compression.codec", "snappy")
        
        // Logging
        .config("spark.hadoop.dfs.client.log.severity", "ERROR")
        
        .getOrCreate()

    try {

      // Load taxi data with sampling to reduce memory pressure
      val taxiDF = loadTaxiData(spark, inputPath)
      val sampledDF = taxiDF.sample(withReplacement = false, fraction = 0.1)
      
      // Process the sampled data
      val processedDF = processTaxiData(sampledDF)
      
      // Write with minimal partitioning
      processedDF.write
        .mode(SaveMode.Overwrite)
        .option("maxRecordsPerFile", 10000)
        .partitionBy("pickup_date")
        .parquet(outputPath)
        
      println(s"Data processing complete! Results saved to $outputPath")
    } catch {
      case e: Exception => 
        println(s"Error processing data: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
  
  def loadTaxiData(spark: SparkSession, inputPath: String): DataFrame = {
    println(s"Loading taxi data from $inputPath")
    
    try {
      val taxiDF = spark.read
        .option("mergeSchema", "true")
        .parquet(inputPath)
      
      val rowCount = taxiDF.count()
      println(s"Loaded $rowCount taxi records")
      taxiDF
    } catch {
      case e: Exception =>
        println(s"Error loading data: ${e.getMessage}")
        throw e
    }
  }
  
  def processTaxiData(df: DataFrame): DataFrame = {
    println("Processing taxi data...")
    
    // Avoid caching to reduce memory pressure
    val processedDF = df
      .select(
        "tpep_pickup_datetime", 
        "tpep_dropoff_datetime", 
        "trip_distance", 
        "fare_amount"
      )
      .withColumn("pickup_datetime", to_timestamp(col("tpep_pickup_datetime")))
      .withColumn("dropoff_datetime", to_timestamp(col("tpep_dropoff_datetime")))
      .withColumn("pickup_date", to_date(col("pickup_datetime")))
      .withColumn("trip_duration_minutes", 
        round((unix_timestamp(col("dropoff_datetime")) - 
               unix_timestamp(col("pickup_datetime"))) / 60, 2))
      .withColumn("cost_per_mile", 
        when(col("trip_distance") > 0, 
             round(col("fare_amount") / col("trip_distance"), 2))
        .otherwise(0))
      .filter(col("trip_distance") >= 0 && col("fare_amount") >= 0)
    
    println(s"Processed data has ${processedDF.count()} records")
    processedDF
  }
}