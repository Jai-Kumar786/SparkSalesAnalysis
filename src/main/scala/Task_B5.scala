// Import necessary classes from the Spark SQL library.
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum, desc}
import org.apache.spark.sql.types.DecimalType

// Define a Scala object. In Scala, an object is a singleton instance of a class,
// and it's a common place to put the main method for an application.
object Task_B5 {

  // The main entry point for the Spark application.
  def main(args: Array[String]): Unit = {
    // This line points Spark to your Hadoop installation directory,
    // which is necessary for running Spark on Windows locally.
    System.setProperty("hadoop.home.dir", "C:\\hadoop")

    // STEP 1: INITIALIZE SPARK SESSION
    // The SparkSession is the entry point for Spark applications.
    // We use the builder pattern to configure and create the session.
    val spark = SparkSession.builder()
      .appName("Total Revenue Per Category") // Sets a name for the application.
      .master("local[*]") // Tells Spark to run locally using all available CPU cores.
      .getOrCreate()     // Gets an existing session or creates a new one.

    // Set the log level to "ERROR" to reduce console output and keep it clean.
    spark.sparkContext.setLogLevel("ERROR")

    // STEP 2: LOAD THE CLEANED DATA
    // We read the data directly from the Parquet file created in Task Set A.
    // Parquet is an efficient, columnar storage format that preserves the schema.
    println("Reading the cleaned sales data from Parquet file...")
    val cleanedDF = spark.read.parquet("data/cleaned_sales_data.parquet")

    // STEP 3: PERFORM THE ANALYSIS
    // Calculate the total revenue for each category.
    println("Calculating total revenue per category...")
    val revenueByCategory = cleanedDF
      // .groupBy("category") groups all rows with the same category value together.
      .groupBy("category")
      // .agg() performs an aggregation on each group.
      // sum("total_amount") calculates the sum of the 'total_amount' column for each category.
      // .alias("total_revenue") gives a new name to the resulting sum column.
      .agg(sum("total_amount").alias("total_revenue"))
      // .withColumn() is used here to format the number for better readability.
      // We cast the result to a DecimalType with 2 decimal places.
      .withColumn("total_revenue", col("total_revenue").cast(DecimalType(10, 2)))
      // .sort(desc("total_revenue")) sorts the final result from highest to lowest revenue.
      .sort(desc("total_revenue"))

    // STEP 4: DISPLAY THE RESULTS
    // The .show() action triggers the execution of all the transformations and prints the result to the console.
    println("Analysis complete. Displaying total revenue per category:")
    revenueByCategory.show()

    // STEP 5: STOP THE SPARK SESSION
    // It's crucial to stop the SparkSession to release the cluster resources.
    spark.stop()
  }
}