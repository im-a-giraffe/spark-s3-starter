package com.sulkyloops.bigdata;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class Main {

    private static Logger logger = Logger.getLogger("MainLogger");

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("S3TestApp");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> log = sc.textFile("s3a://sca-tool/guava.arff");

        logger.info("Log Count: "+log.count());

        sc.close();
    }
}
