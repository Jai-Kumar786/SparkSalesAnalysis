import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_timestamp}
import org.apache.spark.sql.types.TimestampType

object TaskSetA {

  def main(args: Array[String]): Unit = {
    // Create a SparkSession - the entry point to any Spark functionality
    val spark = SparkSession.builder()
      .appName("Spark Sales Analysis - Task A")
      .master("local[*]") // Use all available cores on the local machine
      .getOrCreate()

    // Set log level to ERROR to avoid verbose output
    spark.sparkContext.setLogLevel("ERROR")

    println("------------- Task A Started -------------")

    // ----------------------------------------------------------------
    // Task 1: Load the dataset into a Spark DataFrame.
    // ----------------------------------------------------------------
    // We read the CSV file, specifying that it has a header and asking Spark
    // to infer the schema (data types) of the columns automatically.
    val salesDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true") // Infers data types, good for initial analysis
      .csv("data/sales_data.csv")

    println("1. Dataset loaded successfully. Schema and sample data:")
    salesDF.printSchema()
    salesDF.show(5, truncate = false)

    // ----------------------------------------------------------------
    // Task 2: Handle missing values if any (drop rows with any nulls).
    // ----------------------------------------------------------------
    // For simplicity, we will use the drop strategy. The .na.drop() function
    // removes any row that contains one or more null or NaN values.
    val cleanedDF = salesDF.na.drop()
    val originalCount = salesDF.count()
    val cleanedCount = cleanedDF.count()

    println(s"2. Handled missing values. Original rows: $originalCount, Cleaned rows: $cleanedCount")

    // ----------------------------------------------------------------
    // Task 3: Convert timestamp column to Spark TimestampType.
    // ----------------------------------------------------------------
    // The `withColumn` transformation is used to add or replace a column.
    // We use the `to_timestamp` function to parse the string timestamp.
    // Note: The original column is replaced with the newly typed one.
    val timestampDF = cleanedDF.withColumn(
      "timestamp",
      to_timestamp(col("timestamp"), "M/d/yyyy H:mm") // <-- This is the correct format
    )

    println("3. Converted 'timestamp' column to TimestampType. New schema:")
    timestampDF.printSchema()

    // ----------------------------------------------------------------
    // Task 4: Add a new column total_amount = quantity * price.
    // ----------------------------------------------------------------
    // We create the 'total_amount' by multiplying the 'quantity' and 'price' columns.
    // Spark handles the element-wise multiplication for us.
    val finalDF = timestampDF.withColumn("total_amount", col("quantity") * col("price"))

    println("4. Added 'total_amount' column. Final DataFrame sample:")
    finalDF.show(10, truncate = false)

    println("------------- Task A Completed -------------")

    // Stop the SparkSession to release resources
    spark.stop()
  }
}