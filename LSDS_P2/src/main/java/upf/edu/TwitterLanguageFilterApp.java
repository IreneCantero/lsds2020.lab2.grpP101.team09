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

        Gson parser = new Gson();

        SparkConf conf =new SparkConf().setAppName("Filter Language");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        List<String> argsList = Arrays.asList(args);
        String language = argsList.get(0);
        String outputFile = argsList.get(1);
        String bucket = argsList.get(2);

        String input = "";
        for (int i = 3 ; i < argsList.size() ; i++){
            if(i == argsList.size()-1) input+=argsList.get(i);
            else input+=(argsList.get(i)+",");
        }
        long startTimeTotal = System.nanoTime();
        JavaRDD<String> tweets = sparkContext.textFile(input);
        System.out.println("Language: " + language + ". Output file: " + outputFile + ". Destination bucket: " + bucket);
        JavaRDD<SimplifiedTweet>tst = tweets
                .filter(t -> t.length() > 0 && SimplifiedTweet.fromJson(t).isPresent())
                .map(s -> SimplifiedTweet.fromJson(s).get())
                .filter(r -> r.get_language().equals("\"" + language + "\""));

        System.out.println("Simplified tweets:" + tst.count());
        System.out.println("Total time: " + (float) (System.nanoTime() - startTimeTotal) / 1000000000 + " s");
        JavaRDD<String> content = tst.map(s->stringParser(s));
        content.saveAsTextFile(outputFile);


    }
}