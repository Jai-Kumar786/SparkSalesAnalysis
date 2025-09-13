/**
 * This script performs a series of analytical queries on pre-cleaned sales data
 * using Spark SQL. It covers tasks 9 through 12 of the assignment.
 *
 * The workflow is as follows:
 * 1. Initialize a SparkSession.
 * 2. Load the cleaned sales data from a Parquet file.
 * 3. Register the DataFrame as a temporary SQL view named 'sales'.
 * 4. Execute SQL queries to derive business insights:
 * - Task 10: Calculate Average Order Value (AOV) per region.
 * - Task 11: Identify the most sold product category by quantity.
 * - Task 12: Compute monthly revenue, aggregated by category.
 * 5. Display the results of each query to the console.
 * 6. Terminate the SparkSession to release resources.
 */

// Import the necessary SparkSession class, which is the entry point for Spark SQL functionality.
import org.apache.spark.sql.SparkSession

// Define a Scala object. In Scala, the 'object' keyword creates a singleton instance,
// making it an ideal container for the main method of an application.
object Task_C9_to_12 {

  /**
   * The main entry point for the Spark application.
   * @param args Command-line arguments (not used in this application).
   */
  def main(args: Array[String]): Unit = {
    // Set the 'hadoop.home.dir' system property. This is a common requirement for running
    // Spark on a Windows machine to locate the necessary Hadoop binaries (like winutils.exe).
    System.setProperty("hadoop.home.dir", "C:\\Hadoop")

    // STEP 1: INITIALIZE SPARK SESSION
    // The SparkSession provides a unified entry point for programming Spark with the
    // Dataset and DataFrame APIs.
    val spark = SparkSession
      .builder()
      // .appName() sets a user-friendly name for the application, which appears in the Spark UI.
      .appName("SparkSQL Queries (Tasks C9-C12)")
      // .master("local[*]") configures Spark to run in local mode, using all available CPU cores.
      // This is suitable for development and testing on a single machine.
      .master("local[*]")
      // .getOrCreate() retrieves an existing SparkSession or creates a new one if none exists.
      .getOrCreate()

    // It is a best practice to set the log level to ERROR for applications to avoid flooding
    // the console with verbose INFO and DEBUG messages, making the output easier to read.
    spark.sparkContext.setLogLevel("ERROR")

    // STEP 2: LOAD THE CLEANED DATA
    // Read the pre-processed data from the Parquet file generated in Task Set A.
    // Parquet is a highly efficient, columnar storage format that preserves the data schema
    // and is optimized for performance in Spark.
    println("Reading the cleaned sales data from Parquet file...")
    val cleanedDF = spark.read.parquet("data/cleaned_sales_data.parquet")

    // --- Task 9: Register the DataFrame as a temporary SQL view ---
    // This action allows developers to query the DataFrame's data using standard SQL.
    // The view is temporary, meaning its lifecycle is tied to the SparkSession that created it.
    cleanedDF.createOrReplaceTempView("sales")
    println("Task 9 complete: DataFrame has been registered as temporary view 'sales'.")

    // --- Task 10: Get average order value (AOV) per region ---
    println("\n--- Executing Task 10: Average Order Value (AOV) per Region ---")
    val aovByRegion = spark.sql("""
      -- This query calculates the average 'total_amount' for each transaction, grouped by region.
      -- It provides insight into the spending habits of customers in different geographical areas.
      SELECT
        region,
        -- The AVG() aggregate function computes the average of the 'total_amount' column.
        -- We alias the result as 'average_order_value' for clarity in the output.
         CAST(AVG(total_amount) AS DECIMAL(10, 2)) AS average_order_value
      FROM sales
      -- GROUP BY region instructs Spark to perform the AVG() aggregation separately for each distinct region.
      GROUP BY region
      -- ORDER BY region sorts the final output alphabetically by region name for consistent presentation.
      ORDER BY region
    """)
    aovByRegion.show()

    // --- Task 11: Get the most sold product category overall ---
    println("\n--- Executing Task 11: Most Sold Product Category (by total quantity) ---")
    val mostSoldCategory = spark.sql("""
      -- This query identifies the product category with the highest total number of items sold.
      -- This is a key metric for inventory and marketing focus.
      SELECT
        category,
        -- SUM(quantity) calculates the total number of items sold for each category.
        SUM(quantity) AS total_quantity_sold
      FROM sales
      -- The data is grouped by 'category' to aggregate sales quantities accordingly.
      GROUP BY category
      -- ORDER BY ... DESC sorts the results from the highest quantity sold to the lowest.
      ORDER BY total_quantity_sold DESC
      -- LIMIT 1 restricts the output to only the top row, which represents the most sold category.
      LIMIT 1
    """)
    mostSoldCategory.show()

    // --- Task 12: Calculate monthly revenue grouped by category ---
    println("\n--- Executing Task 12: Monthly Revenue by Category ---")
    val monthlyRevenueByCategory = spark.sql("""
      -- This query provides a detailed breakdown of revenue performance over time for each category.
      SELECT
        -- DATE_FORMAT(timestamp, 'yyyy-MM') extracts and formats the year and month from the
        -- timestamp column, allowing for temporal aggregation (e.g., '2023-01').
        DATE_FORMAT(timestamp, 'yyyy-MM') AS month,
        category,
        -- SUM(total_amount) calculates the total revenue for the specific month and category.
        SUM(total_amount) AS monthly_revenue
      FROM sales
      -- We group by both 'month' and 'category' to create the desired hierarchical aggregation.
      GROUP BY month, category
      -- Ordering by month and then category ensures the final report is chronological and well-organized.
      ORDER BY month, category
    """)
    monthlyRevenueByCategory.show()

    // STEP 5: STOP THE SPARK SESSION
    // This is a critical final step. It terminates the Spark context and releases all resources
    // (like memory and CPU cores) back to the cluster or operating system.
    spark.stop()
  }
}

