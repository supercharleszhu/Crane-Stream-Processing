import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.examples.StreamingExamples;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class JavaNetworkWordCount {
    private static final Pattern SPACE = Pattern.compile(" ");
    private static final Pattern RESOURCES = Pattern.compile("([a-z]|\\d)+\\.[a-z]+");
    private static Map<String, Integer> map;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: JavaNetworkWordCount <hostname> <port>");
            System.exit(1);
        }

        // Create the context with a 1 second batch size
        SparkConf conf = new SparkConf().setAppName("JavaNetworkWordCount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(180));

        // Create a JavaReceiverInputDStream on target ip:port and count the
        // words in input stream of \n delimited text (eg. generated by 'nc')
        // Note that no duplication in storage level only for running locally.
        // Replication necessary in distributed scenario for fault tolerance.
        JavaReceiverInputDStream<String> lines = ssc.socketTextStream(
                args[0], Integer.parseInt(args[1]), StorageLevels.MEMORY_AND_DISK_SER);


        // with 200 status code
        JavaDStream<String> successLines = lines.filter(s -> s.contains("200"));
//        JavaDStream<String> successLines = lines.filter(new Function<String, Boolean>() {
//            public Boolean call(String line) {
//                return line.contains("200");
//            }
//        });

        JavaDStream<String> words = successLines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
        JavaPairDStream<String, Integer> wordCounts = words.
                mapToPair(s -> {
                    Matcher matcher = RESOURCES.matcher(s);
                    if(matcher.find()) {
                        return new Tuple2<>(s, 1);
                    } else {
                        return new Tuple2<>("", 0);
                    }})
                .reduceByKey((i1, i2) -> i1 + i2);

        JavaPairDStream<Integer, String> sorted = wordCounts.mapToPair(x -> x.swap()).
                transformToPair(
                    new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer, String>>() {
                        @Override
                        public JavaPairRDD<Integer, String> call(JavaPairRDD<Integer, String> jPairRDD) throws Exception {
                            return jPairRDD.sortByKey(false);
                        }
                    }
                );

//        words.print();
//        successLines.print();
        sorted.print(5);
        ssc.start();
        ssc.awaitTermination();
    }
}