package com.ms;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


@Path("/hello")
public class ExampleResource {

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    public String hello() {

        SparkSession spark = SparkSession
            .builder()
            .appName("Spark SQL Example")
            .config("spark.master", "local")
            .getOrCreate();

        Dataset<Row> ds = spark.read().format("org.neo4j.spark.DataSource")
            .option("url", "bolt://localhost:53935")
            .option("authentication.basic.username", "neo4j")
            .option("authentication.basic.password", "neo4j4dev")
            .option("query", "MATCH (d:SalesEquipmentType) RETURN d")
            .option("partitions", 2)
            .load();

        ds.show();
        return ds.first().toString();
    }
}
