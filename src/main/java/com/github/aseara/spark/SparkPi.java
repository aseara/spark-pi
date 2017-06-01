package com.github.aseara.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by aseara on 2017/6/1.
 *
 */
public class SparkPi {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Spark Pi").setMaster("spark://192.168.8.192:7077");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.addJar("E:\\study\\spark\\spark-pi\\out\\artifacts\\spark_pi_jar\\spark-pi.jar");

        int slices = 200;
        int n = (int)Math.min(100000L * slices, Integer.MAX_VALUE);
        List<Integer> l = new ArrayList<>(n);
        for (int i = 0; i < n; i++) {
            l.add(i);
        }

        long count = sc.parallelize(l, slices).filter(i -> {
            double x = Math.random();
            double y = Math.random();
            return x*x + y*y < 1;
        }).count();

        System.out.println("Pi is roughly " + 4.0 * count / n);

        sc.stop();
    }

}
