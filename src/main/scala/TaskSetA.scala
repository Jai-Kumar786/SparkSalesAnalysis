// Import necessary classes and functions from the Spark SQL library.
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, to_timestamp}

// Define a Scala object. In Scala, an object is a singleton instance of a class,
// and it's a common place to put the main method for an application.
object TaskSetA {

  // The main entry point for the Spark application.
  def main(args: Array[String]): Unit = {
    // This line points Spark to your Hadoop installation directory.
    // Note the double backslashes \\ which are required in Java/Scala strings.
    System.setProperty("hadoop.home.dir", "C:\\Hadoop")


    // STEP 1: INITIALIZE SPARK SESSION
    // The SparkSession is the entry point to programming Spark with the Dataset and DataFrame API.
    // We use the builder pattern to configure the session.
    val spark = SparkSession.builder()
      // .appName sets a name for the application, which will appear in the Spark UI.
      .appName("Task A - Clean and Save with Explanations")
      // .master("local[*]") tells Spark to run locally using all available CPU cores.
      // This is ideal for development and testing on a single machine.
      .master("local[*]")
      // .getOrCreate() gets an existing SparkSession or, if there is none, creates a new one.
      .getOrCreate()

    // This line reduces the amount of log chatter in the console, making the output easier to read.
    // It's set to "ERROR", so we only see important error messages.
    spark.sparkContext.setLogLevel("ERROR")

    // STEP 2: LOAD THE DATA
    // We use spark.read to access the DataFrameReader API.
    val salesDF = spark.read
      // .option("header", "true") tells Spark that the first line of the CSV is the header row.
      .option("header", "true")
      // .option("inferSchema", "true") asks Spark to automatically guess the data type of each column.
      // While convenient, for production code, it's often better to define the schema explicitly for performance.
      .option("inferSchema", "true")
      // .csv() specifies the path to the input file.
      .csv("data/sales_data.csv")

    // STEP 3: HANDLE MISSING VALUES
    // First, let's get a count of rows before we do anything.
    // .count() is a Spark "action" that triggers a job to count all rows in the DataFrame.
    val originalCount = salesDF.count()
    println(s"Number of rows before cleaning: $originalCount")

    // The .na property provides methods for handling missing data (null values).
    // .drop() will remove any row that contains one or more null values in any column.
    val cleanedDF = salesDF.na.drop()

    // Now, let's count the rows again on the new DataFrame to see the result.
    val cleanedCount = cleanedDF.count()
    println(s"Number of rows after cleaning: $cleanedCount")

    // We can also calculate and print the number of rows that were removed.
    val rowsDropped = originalCount - cleanedCount
    println(s"Total rows with missing values dropped: $rowsDropped")

    // STEP 4: CONVERT TIMESTAMP COLUMN
    // .withColumn() is a transformation that adds a new column or replaces an existing one.
    // Here, we are replacing the existing 'timestamp' column.
    val timestampDF = cleanedDF.withColumn(
      "timestamp", // The name of the column to update.
      // The `to_timestamp` function converts a string column to a Spark TimestampType.
      // We pass it the column we want to convert (`col("timestamp")`) and the specific format
      // of the date-time string in our CSV file ("M/d/yyyy H:mm").
      // Providing the correct format is crucial for a successful conversion.
      to_timestamp(col("timestamp"), "M/d/yyyy H:mm")
    )

    // STEP 5: CREATE A NEW COLUMN FOR TOTAL AMOUNT
    // We use .withColumn() again, this time to add a new column named "total_amount".
    val finalDF = timestampDF.withColumn(
      "total_amount",
      // The value of the new column is calculated by multiplying the 'quantity' and 'price' columns.
      // Spark performs this calculation for every row in the DataFrame.
      col("quantity") * col("price")
    )

    // --- Final Output and Save Operation ---

    println("Data cleaning complete. Schema of the final DataFrame:")
    // .printSchema() is an action that displays the column names and their data types.
    finalDF.printSchema()
    finalDF.show(10)

    println("Saving the cleaned DataFrame as a Parquet file...")

    // STEP 6: SAVE THE CLEANED DATAFRAME
    // .write provides access to the DataFrameWriter API.
    // .mode("overwrite") specifies that if the output directory already exists, it should be deleted and replaced.
    // .parquet() saves the DataFrame in the highly efficient, columnar Parquet format.
    // Parquet is the standard format for storing data in Spark pipelines because it's fast and preserves the schema.
    finalDF.write.mode("overwrite").parquet("data/cleaned_sales_data.parquet")

    println("Successfully saved cleaned data to 'data/cleaned_sales_data.parquet'")

    // STEP 7: STOP THE SPARK SESSION
    // It's important to stop the SparkSession to release all the cluster resources it was using.
    spark.stop()
  }
}