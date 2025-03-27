#!/bin/bash

LOG_FILE="pipeline_$(date +%Y%m%d_%H%M%S).log"
{
    echo "=== Pipeline started at $(date) ==="
    
    # Function to check command success
    function check_success() {
        if [ $? -ne 0 ]; then
            echo "!!! ERROR: $1 failed !!!"
            echo "=== Pipeline failed at $(date) ==="
            exit 1
        fi
        echo "$1 completed successfully"
    }

    # 1. Start HDFS
    echo "--- Starting HDFS ---"
    start-dfs.sh
    check_success "HDFS startup"
    sleep 5

    # 2. Initialize HDFS Storage
    echo "--- Initializing HDFS Storage ---"
    sbt "runMain HDFSStorage"
    check_success "HDFS storage initialization"

    # 3. Run Parquet Ingestion
    echo "--- Running Parquet Ingestion ---"
    sbt "runMain ParquetIngestion"
    check_success "Parquet ingestion"

    # 4. Process Data
    echo "--- Processing Data ---"
    sbt -J--add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
        -J--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
        "runMain DataProcessor"
    check_success "Data processing"

    # 5. Create Hive Dimensional Model
    echo "--- Creating Hive Dimensional Model ---"

    sbt -J--add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
        -J--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
        "runMain HiveDimensionalModel"
    check_success "Hive dimensional model creation"

    echo "=== Pipeline completed successfully at $(date) ==="

} | tee -a "$LOG_FILE" 2>&1

echo "Log saved to $LOG_FILE"