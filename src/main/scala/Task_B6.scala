// Import necessary classes from the Spark SQL library.
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum, desc}
import org.apache.spark.sql.types.DecimalType

// Define the main object for this task.
object Task_B6 {

  // The main entry point for the Spark application.
  def main(args: Array[String]): Unit = {
    // Set Hadoop home directory for local execution on Windows.
    System.setProperty("hadoop.home.dir", "C:\\Hadoop")

    // STEP 1: INITIALIZE SPARK SESSION
    // Create a SparkSession with a relevant application name.
    val spark = SparkSession.builder()
      .appName("Top 5 Customers by Purchase Amount")
      .master("local[*]")
      .getOrCreate()

    // Reduce log verbosity for a cleaner output.
    spark.sparkContext.setLogLevel("ERROR")

    // STEP 2: LOAD THE CLEANED DATA
    // Read the pre-processed data from the Parquet file.
    println("Reading the cleaned sales data...")
    val cleanedDF = spark.read.parquet("data/cleaned_sales_data.parquet")

    // STEP 3: PERFORM THE ANALYSIS
    // Find the top 5 customers by their total purchase amount.
    println("Finding top 5 customers with the highest purchase amount...")
    val topCustomers = cleanedDF
      // .groupBy("customer_id") groups the data so that all transactions for a single customer are together.
      .groupBy("customer_id")
      // .agg(sum("total_amount")) calculates the total amount spent for each customer.
      // .alias("total_spent") renames the aggregated column to "total_spent".
      .agg(sum("total_amount").alias("total_spent"))
      // .sort(desc("total_spent")) sorts the customers in descending order based on their spending.
      .sort(desc("total_spent"))
      // .limit(5) restricts the output to only the top 5 rows from the sorted result.
      .limit(5)
      // .withColumn() is used to format the monetary value nicely.
      .withColumn("total_spent", col("total_spent").cast(DecimalType(10, 2)))

    // STEP 4: DISPLAY THE RESULTS
    // The .show() action computes and displays the final DataFrame.
    println("Analysis complete. Displaying top 5 customers:")
    topCustomers.show()

    // STEP 5: STOP THE SPARK SESSION
    // Release all allocated resources.
    spark.stop()
  }
}
