import org.apache.spark.sql.SparkSession
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.io.File
import scala.collection.mutable.ListBuffer

object ParquetIngestion {
  def main(args: Array[String]): Unit = {
    // Initialize SparkSession
    val spark = SparkSession.builder()
      .appName("ParquetIngestion")
      .master("local[*]")
      .config("spark.hadoop.dfs.client.block.write.replace-datanode-on-failure.enable", "false")
      .config("spark.hadoop.dfs.replication", "1")
      .getOrCreate()

    // Parse command line arguments
    val sourceDir = if (args.length > 0) args(0) else "nyc_taxis_data/raw"
    val hdfsTargetDir = if (args.length > 1) args(1) else "hdfs://localhost:9000/nyc_taxis_data/raw"
    val metadataDir = if (args.length > 2) args(2) else "hdfs://localhost:9000/nyc_taxis_data/metadata"
    
    // Run ingestion
    ingestParquetFiles(spark, sourceDir, hdfsTargetDir, metadataDir)
  }
  
  def ingestParquetFiles(
    spark: SparkSession,
    sourceDir: String, 
    hdfsTargetDir: String, 
    metadataDir: String
  ): Unit = {
    println(s"Starting ingestion from $sourceDir to $hdfsTargetDir")
    
    // HDFS configuration
    val conf = spark.sparkContext.hadoopConfiguration
    val hdfsUri = hdfsTargetDir.split("/").take(3).mkString("/")
    conf.set("fs.defaultFS", hdfsUri)
    val hdfs = FileSystem.get(conf)
    
    // Create target directory if it doesn't exist
    val targetPath = new Path(hdfsTargetDir)
    if (!hdfs.exists(targetPath)) {
      hdfs.mkdirs(targetPath)
      println(s"Created directory $hdfsTargetDir")
    }
    
    // Create metadata directory if it doesn't exist
    val metaDir = new File(metadataDir)
    if (!metaDir.exists()) {
      metaDir.mkdirs()
      println(s"Created metadata directory $metadataDir")
    }
    
    // Track ingestion metadata
    val ingestedFilesPath = new Path(s"$metadataDir/ingested_files.txt")
    val ingestedFiles = if (hdfs.exists(ingestedFilesPath)) {
      val stream = hdfs.open(ingestedFilesPath)
      try {
        scala.io.Source.fromInputStream(stream).getLines().toSet
      } finally {
        stream.close()
      }
    } else {
      Set[String]()
    }
    
    // List all parquet files in source directory
    val sourceDirectory = new File(sourceDir)
    val parquetFiles = sourceDirectory.listFiles().filter(_.getName.endsWith(".parquet"))
    
    println(s"Found ${parquetFiles.length} parquet files")
    
    // Process each file that hasn't been ingested yet
    val newlyIngestedFiles = ListBuffer[String]()
    var filesIngested = 0
    
    for (file <- parquetFiles) {
      if (!ingestedFiles.contains(file.getName)) {
        // Copy file to HDFS
        val sourcePath = new Path(file.getAbsolutePath)
        val targetFilePath = new Path(s"$hdfsTargetDir/${file.getName}")
        
        hdfs.copyFromLocalFile(sourcePath, targetFilePath)
        
        println(s"Ingested file ${file.getName} to $hdfsTargetDir")
        
        // Track this file as ingested
        newlyIngestedFiles += file.getName
        filesIngested += 1
      }
    }
    
    // Update ingestion metadata
    if (newlyIngestedFiles.nonEmpty) {
      val outputStream = hdfs.create(ingestedFilesPath)
      val writer = new java.io.PrintWriter(outputStream)
      try {
        newlyIngestedFiles.foreach(writer.println)
      } finally {
        writer.close()
        outputStream.close()
      } 
    }   
    
    // Log ingestion summary
    val timestamp = LocalDateTime.now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
    val summaryPath = new Path(s"$metadataDir/ingestion_summary.txt")
    val summaryOutputStream = if (hdfs.exists(summaryPath)) {
      hdfs.append(summaryPath)
    } else {
      hdfs.create(summaryPath)
    }
    val summaryWriter = new java.io.PrintWriter(summaryOutputStream)
    try {
      summaryWriter.println(s"[$timestamp] Ingested $filesIngested new files. " +
        s"Total files available in HDFS: ${ingestedFiles.size + filesIngested}")
    } finally {
      summaryWriter.close()
      summaryOutputStream.close()
    }
    
    println(s"Ingestion completed. Ingested $filesIngested new files.")
  }
  
  def validateParquetWithSpark(filePath: String): Boolean = {
    // Create a Spark session to validate parquet files
    val spark = SparkSession.builder()
      .appName("Parquet Validation")
      .master("local[*]")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .getOrCreate()
    
    try {
      // Try to read the parquet file
      val df = spark.read.parquet(filePath)
      val count = df.count()
      println(s"Validated parquet file with $count rows")
      true
    } catch {
      case e: Exception => 
        println(s"Invalid parquet file: ${e.getMessage}")
        false
    } finally {
      spark.stop()
    }
  }
}