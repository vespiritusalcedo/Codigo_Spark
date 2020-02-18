package Job;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.types.StructType;
import domain.Person;


public final class StructuredStreaming {

    private static String INPUT_DIRECTORY = "/home/veronica/Documentos/Codigo_Spark_FastData/Spark/input_file";

    public static void main(String[] args) throws Exception {
        System.out.println("Starting StructuredStreamingAverage job...");


        //1 - Start the Spark session
        SparkSession spark = SparkSession
                .builder()
                .appName("StructuredStreaming")
                .config("spark.master", "local")
                .config("spark.eventLog.enabled", "false")
                .config("spark.driver.memory", "2g")
                .config("spark.executor.memory", "2g")
                .getOrCreate();

        //2- Define the input data schema
        StructType personSchema = new StructType()
                .add("firstName", "string")
                .add("lastName", "string")
                .add("sex", "string")
                .add("age", "long");

        //3 - Create a Dataset representing the stream of input files
        Dataset<Person> personStream = spark
                .readStream()
                .schema(personSchema)
                .json(INPUT_DIRECTORY)
                .as(Encoders.bean(Person.class));

        //4 - Create a temporary table so we can use SQL queries
        personStream.createOrReplaceTempView("people");
        String sql = "SELECT AVG(age) as average_age, sex FROM people GROUP BY sex";
        Dataset<Row> ageAverage = spark.sql(sql);

        //5 - Write the the output of the query to the consold
        StreamingQuery query = ageAverage.writeStream()
                .outputMode("complete")
                .format("console")
                .start();

        query.awaitTermination();

    }
}
