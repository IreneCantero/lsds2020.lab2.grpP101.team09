package upf.edu;


import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import upf.edu.parser.SimplifiedTweet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TwitterLanguageFilterApp {
    /*Auxiliar function to be able to serialize*/
    public static String stringParser(SimplifiedTweet myObj){
        Gson parser = new Gson();
        return parser.toJson(myObj);
    }
    public static void main(String[] args) throws IOException {
        /*Accumulator of time*/
        float total_time = 0;
        List<String> argsList = Arrays.asList(args);

        String language = argsList.get(0);
        String outputFile = argsList.get(1);

        SparkConf conf =new SparkConf().setAppName("Filter Language");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        List<SimplifiedTweet> accumulator = new ArrayList<SimplifiedTweet>();

        System.out.println("Language: " + language + ". Output file: " + outputFile);
        for (String inputFile : argsList.subList(2, argsList.size())) {
            long startTimePartial = System.nanoTime();
            System.out.println("Processing: " + inputFile);
            /*For each JSON, we collect only the tweets that fits in a SimplifiedTweet and the language concides with the
            * input language*/
            JavaRDD<String> tweets = sparkContext.textFile(inputFile);
            JavaRDD<SimplifiedTweet> tst = tweets
                    .filter(t -> t.length() > 0 && SimplifiedTweet.fromJson(t).isPresent())
                    .map(s -> SimplifiedTweet.fromJson(s).get())
                    .filter(r -> r.get_language().equals("\"" + language + "\""));
            List <SimplifiedTweet> local_result = tst.collect();
            accumulator.addAll(local_result);
            /*We collect the time spent to process a file, and then we add it to the total time as well.*/
            float partial_time = (float) (System.nanoTime() - startTimePartial) /1000000000;
            System.out.println("Partial time for "+ inputFile+ ": " + partial_time + " s");
            total_time += partial_time;
        }
        System.out.println("Simplified tweets:" + accumulator.size());
        /*Transform SimplifiedTweet to JSON string and store everything in an output file*/
        JavaRDD<SimplifiedTweet> result = sparkContext.parallelize(accumulator);
        JavaRDD<String> content = result.map(s->stringParser(s));
        content.saveAsTextFile(outputFile);

        System.out.println("Total time: " + total_time + " s");
    }
}