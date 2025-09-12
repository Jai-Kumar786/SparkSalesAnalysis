// Import necessary classes, including Window for advanced analytics.
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, row_number, desc}

// Define the main object for this task.
object Task_B7 {

  // The main entry point for the Spark application.
  def main(args: Array[String]): Unit = {
    // Set Hadoop home directory for local execution on Windows.
    System.setProperty("hadoop.home.dir", "C:\\Hadoop")

    // STEP 1: INITIALIZE SPARK SESSION
    // Create a SparkSession.
    val spark = SparkSession.builder()
      .appName("Most Popular Payment Mode by Region")
      .master("local[*]")
      .getOrCreate()

    // Set a quieter log level.
    spark.sparkContext.setLogLevel("ERROR")

    // STEP 2: LOAD THE CLEANED DATA
    // Read the data from the Parquet file.
    println("Reading the cleaned sales data...")
    val cleanedDF = spark.read.parquet("data/cleaned_sales_data.parquet")

    // STEP 3: PERFORM THE ANALYSIS (in two stages)
    println("Finding the most popular payment mode by region...")

    // Stage 3a: Count the usage of each payment mode within each region.
    val paymentCounts = cleanedDF
      // Group by both region and payment_mode to get unique counts for each combination.
      .groupBy("region", "payment_mode")
      // .agg(count("*")) counts the number of occurrences for each group.
      .agg(count("*").alias("usage_count"))

    // Stage 3b: Use a window function to find the top payment mode in each region.
    // A window function performs a calculation across a set of rows that are somehow related.
    // First, define the "window" or the group of rows to work with.
    val windowSpec = Window
      .partitionBy("region") // It breaks the data into partitions by region.
      .orderBy(desc("usage_count")) // Within each region, it orders payment modes by their usage count.

    // Next, apply the window function to find and rank all modes within each region.
    val rankedPaymentModes = paymentCounts
      // .withColumn("rank", ...) adds a new column named "rank".
      // row_number().over(windowSpec) assigns a rank (1, 2, 3...) to each payment mode within its region based on usage count.
      .withColumn("rank", row_number().over(windowSpec))

    // STEP 4: FILTER FOR TOP TWO AND DISPLAY THE RESULTS
    println("Analysis complete. Displaying top two popular payment modes per region:")

    // Instead of filtering for rank 1 and rank 2 separately, we filter for both at once.
    // .filter(col("rank") <= 3) keeps only the rows where the rank is 1 or 2or3.
    // We also sort by region and then rank for a more organized final table.
    rankedPaymentModes
      .filter(col("rank") <= 3)
      .sort("region", "rank")
      .select("region", "payment_mode", "usage_count", "rank") // Select the columns to display
      .show()

    // STEP 5: STOP THE SPARK SESSION
    spark.stop()
  }
}
