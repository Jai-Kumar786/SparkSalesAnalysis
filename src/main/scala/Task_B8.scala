// Import necessary classes from the Spark SQL library.
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_date, sum, asc, max, lit, udf}
import org.apache.spark.sql.types.DecimalType

// Define the main object for this task.
object Task_B8 {

  // The main entry point for the Spark application.
  def main(args: Array[String]): Unit = {
    // Set Hadoop home directory for local execution on Windows.
    System.setProperty("hadoop.home.dir", "C:\\Hadoop")

    // STEP 1: INITIALIZE SPARK SESSION
    // Create a SparkSession.
    val spark = SparkSession.builder()
      .appName("Daily Revenue Trends")
      .master("local[*]")
      .getOrCreate()

    // Reduce log output.
    spark.sparkContext.setLogLevel("ERROR")

    // STEP 2: LOAD THE CLEANED DATA
    // Read the pre-processed data from the Parquet file.
    println("Reading the cleaned sales data...")
    val cleanedDF = spark.read.parquet("data/cleaned_sales_data.parquet")

    // STEP 3: PERFORM THE ANALYSIS
    println("Calculating daily revenue trends...")
    val dailyRevenue = cleanedDF
      // .groupBy(...) groups the DataFrame.
      // to_date(col("timestamp")) extracts only the date part from the full timestamp.
      // .alias("date") gives this new date column a name.
      .groupBy(to_date(col("timestamp")).alias("date"))
      // .agg(sum("total_amount")) calculates the total revenue for each day (each group).
      .agg(sum("total_amount").alias("daily_revenue"))
      // .sort(asc("date")) sorts the results chronologically to make the trend easy to see.
      .sort(asc("date"))
      // .withColumn(...) formats the daily_revenue column for clean presentation.
      .withColumn("daily_revenue", col("daily_revenue").cast(DecimalType(12, 2)))


    // --- VISUALIZATION STEP ---
    // To create a scaled bar chart, we first need to find the maximum daily revenue.
    val maxRevenue = dailyRevenue.agg(max("daily_revenue")).first().getDecimal(0).doubleValue()

    // Define a User-Defined Function (UDF) to generate the visual bar.
    // The UDF takes a revenue value and scales it against the max revenue to determine the bar length.
    val createTrendBar = (revenue: Double) => {
      val barLength = 50 // The maximum width of our bar chart in characters.
      val scaledValue = (revenue / maxRevenue * barLength).toInt
      "*" * scaledValue // Returns a string of '*' characters representing the bar.
    }
    // Register the UDF so we can use it with DataFrames.
    val trendBarUDF = udf(createTrendBar)

    // Add the visualization column to our dailyRevenue DataFrame.
    val dailyRevenueWithTrend = dailyRevenue.withColumn("trend", trendBarUDF(col("daily_revenue")))


    // STEP 4: DISPLAY THE RESULTS
    // .show(30) displays up to 30 rows of the result, which is useful for time-series data.
    // truncate = false ensures our visual trend bar isn't cut short.
    println("Analysis complete. Displaying daily revenue with visual trend:")
    dailyRevenueWithTrend.show(30, truncate = false)

    // STEP 5: STOP THE SPARK SESSION
    // Release all cluster resources.
    spark.stop()
  }
}