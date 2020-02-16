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
import java.util.Arrays;
import java.util.List;

public class TwitterLanguageFilterApp {

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


        System.out.println("Language: " + language + ". Output file: " + outputFile + ". Destination bucket: " + bucket);
        for (String inputFile : argsList.subList(3, argsList.size())) {
            System.out.println("Processing: " + inputFile);
            //final FileLanguageFilter filter = new FileLanguageFilter(inputFile, outputFile);

            //Load input
            JavaRDD<String> tweets = sparkContext.textFile(inputFile);
            JavaRDD<String> ft = tweets.filter(t->t.length()>0 && SimplifiedTweet.fromJson(t).isPresent()).filter(t2->t2.contains("\"lang\":"+"\""+language+"\""));
            //JavaRDD<String> ft = tweets.filter(t->t.length()>0 && SimplifiedTweet.fromJson(t).isPresent());//.filter(t2->t2.equals(new JsonObject()));
          //  JavaRDD<JsonObject> jsons = tweets
            //        .filter(t->t.length()>0 && SimplifiedTweet.fromJson(t).isPresent()).;
            System.out.println("efewfwef:" + ft.count());

            //The function filterLanguage returns an int variable containing the number of tweets in one language and in one file
            //total_tweets += filter.filterLanguage(language);
        }

        //final S3Uploader uploader = new S3Uploader(bucket, "output.json", "upf");
        //uploader.upload(Arrays.asList(outputFile));

        //Print total number of tweets in the specified language
        //System.out.println("Total Tweets in " + language + ": " + total_tweets);
        //Print total time
        System.out.println("Total time: " + (float) (System.nanoTime() - startTimeTotal) / 1000000000 + " s");
    }
}