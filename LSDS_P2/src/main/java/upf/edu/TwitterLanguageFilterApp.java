package upf.edu;


import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import upf.edu.filter.FileLanguageFilter;
import upf.edu.parser.SimplifiedTweet;
import upf.edu.uploader.S3Uploader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TwitterLanguageFilterApp {
    /*Auxiliar function to serialize*/
    public static String stringParser(SimplifiedTweet myObj){
        Gson parser = new Gson();
        return parser.toJson(myObj);
    }
    public static void main(String[] args) throws IOException {

        //Benchmark Total
        long startTimeTotal = System.nanoTime();
        //

        Gson parser = new Gson();

        //Variable for adding number of tweets per file
        int total_tweets = 0;

        List<String> argsList = Arrays.asList(args);
        String language = argsList.get(0);
        String outputFile = argsList.get(1);
        String bucket = argsList.get(2);
        SparkConf conf =new SparkConf().setAppName("Filter Language");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        JavaRDD<SimplifiedTweet> tst = sparkContext.emptyRDD();
        JavaRDD<SimplifiedTweet> result = sparkContext.emptyRDD();
        System.out.println("Language: " + language + ". Output file: " + outputFile + ". Destination bucket: " + bucket);
         for (String inputFile : argsList.subList(3, argsList.size())) {
             long startTimePartial = System.nanoTime();
             System.out.println("Processing: " + inputFile);

             JavaRDD<String> tweets = sparkContext.textFile(inputFile);
              tst = tweets
                     .filter(t -> t.length() > 0 && SimplifiedTweet.fromJson(t).isPresent())
                     .map(s -> SimplifiedTweet.fromJson(s).get())
                     .filter(r -> r.get_language().equals("\"" + language + "\""));
             result = sparkContext.union(tst);
             System.out.println("Partial time for "+ inputFile+ ": " + (float) (System.nanoTime() - startTimeTotal) / 1000000000 + " s");
         }
        System.out.println("Simplified tweets:" + result.count());
        JavaRDD<String> content = result.map(s->stringParser(s));
        content.saveAsTextFile(outputFile);

        System.out.println("Total time: " + (float) (System.nanoTime() - startTimeTotal) / 1000000000 + " s");
    }
}