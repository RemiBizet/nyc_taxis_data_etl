import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

// If one only wants to init the HDFS Storage
object HDFSStorage {
  def main(args: Array[String]): Unit = {
    
    val hdfsUri = if (args.length > 0) args(0) else "hdfs://localhost:9000/"
    val dirPath = if (args.length > 1) args(1) else "nyc_taxis_data"
    setupStorage(hdfsUri, dirPath)
  }
  
  def setupStorage(hdfsUri: String, dirPath: String): Unit = {
    val conf = new Configuration()
    conf.set("fs.defaultFS", hdfsUri)
    val fs = FileSystem.get(conf)
    
    // Create directories for raw data, processed data, etc.
    val rawDataPath = new Path(s"$dirPath/raw")
    val processedDataPath = new Path(s"$dirPath/processed")
    
    println(s"Setting up HDFS storage for raw and processed data at $dirPath")
    if (!fs.exists(rawDataPath)) fs.mkdirs(rawDataPath)
    if (!fs.exists(processedDataPath)) fs.mkdirs(processedDataPath)
    println(s"HDFS storage setup complete at $dirPath")
  }
}