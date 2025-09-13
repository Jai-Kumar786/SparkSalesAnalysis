/**
 * This script addresses Task Set D, which involves classifying customers into
 * segments based on their purchase frequency.
 *
 * The business goal is to understand customer loyalty and engagement by
 * categorizing them as 'Low', 'Medium', or 'High' frequency buyers.
 *
 * The workflow is as follows:
 * 1. Initialize a SparkSession.
 * 2. Load the cleaned sales data from the Parquet file.
 * 3. Calculate the total number of transactions for each customer.
 * 4. Apply conditional logic to assign a segment ('Low', 'Medium', 'High')
 * based on the transaction count.
 * 5. Display the resulting customer classifications.
 * 6. Provide a summary count of customers within each segment.
 * 7. Terminate the SparkSession.
 */

// Import necessary classes and functions from the Spark SQL library.
// - SparkSession: The entry point to Spark functionality.
// - functions: Provides a rich set of standard functions for DataFrame operations,
//   including 'col', 'count', and 'when'.
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, when}

// Define the main object for this Spark application.
object Task_D13 {

  /**
   * The main entry point for the application.
   * @param args Command-line arguments (not utilized here).
   */
  def main(args: Array[String]): Unit = {
    // Set the Hadoop home directory property for local execution on Windows.
    System.setProperty("hadoop.home.dir", "C:\\Hadoop")

    // STEP 1: INITIALIZE SPARK SESSION
    // Build the SparkSession with a descriptive application name and configure it
    // for local execution using all available CPU cores.
    val spark = SparkSession.builder()
      .appName("Customer Purchase Frequency Classification")
      .master("local[*]")
      .getOrCreate()

    // Set the log level to ERROR to maintain a clean and focused console output.
    spark.sparkContext.setLogLevel("ERROR")

    // STEP 2: LOAD THE CLEANED DATA
    // Load the DataFrame from the Parquet file. This data is the clean, structured
    // output from Task Set A.
    println("Reading the cleaned sales data...")
    val cleanedDF = spark.read.parquet("data/cleaned_sales_data.parquet")

    // STEP 3: CALCULATE PURCHASE FREQUENCY PER CUSTOMER
    // This transformation determines how many separate transactions each customer has made.
    println("Calculating purchase frequency per customer...")
    val customerFrequency = cleanedDF
      // .groupBy("customer_id") groups all rows with the same customer_id together.
      .groupBy("customer_id")
      // .agg() performs an aggregation on each group. Here, we count the number of
      // transactions and name the resulting column 'purchase_count'.
      .agg(count("transaction_id").alias("purchase_count"))

    // STEP 4: CLASSIFY CUSTOMERS INTO SEGMENTS
    // This step adds a new column 'segment' based on the 'purchase_count'.
    println("Classifying customers into loyalty segments...")
    val customerSegments = customerFrequency.withColumn("segment",
      // The when() function is Spark's equivalent of a SQL CASE WHEN statement.
      // It evaluates conditions in order and returns a value for the first one that is true.
      when(col("purchase_count") <= 2, "Low")
        // The second condition checks for the 'Medium' segment.
        .when(col("purchase_count") >= 3 && col("purchase_count") <= 5, "Medium")
        // .otherwise() provides a default value for any rows that do not meet the
        // preceding 'when' conditions. This covers the 'High' segment.
        .otherwise("High")
    )

    // STEP 5: DISPLAY THE RESULTS
    // Show the final classification for each customer.
    println("Analysis complete. Displaying customer segments:")
    customerSegments.show()

    // As a bonus, provide a summary view to see the distribution of customers across the segments.
    // This is a valuable quick insight for business stakeholders.
    println("\nSummary: Customer count per segment:")
    customerSegments.groupBy("segment").count().show()

    // STEP 6: STOP THE SPARK SESSION
    // Release all the cluster resources used by the application.
    spark.stop()
  }
}
