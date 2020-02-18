package upf.edu.filter;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import upf.edu.parser.SimplifiedTweet;
import java.io.*;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Optional;

public class FileLanguageFilter implements LanguageFilter {
    private final FileReader input;
    private final BufferedReader reader;
    private final FileWriter output;
    private final BufferedWriter writer;
    private final String TitleFile;
    public FileLanguageFilter(String input, String output) throws IOException {
        this.input = new FileReader(input);
        this.reader = new BufferedReader(this.input);
        this.output = new FileWriter(output, true);
        this.writer = new BufferedWriter(this.output);
        this.TitleFile = input;
    }

    @Override
    public int filterLanguage(String language) throws IOException {

        Gson parser = new Gson();

        //Create a SparkContext to initialize

        //JavaPairRDD<String, String> t ;
        //JavaRDD<String> ft = tweets.filter(t->t.contains("\"lang\":"+"\""+language+"\""));
        //JavaRDD<String> ft = tweets.filter(t->t.contains(new JsonObject().get("lang").toString()));
        //JavaRDD<String> ft = tweets.filter(t->t.length()>0 && SimplifiedTweet.fromJson(t).isPresent()).filter(t2->t2.contains("\"lang\":"+"\""+language+"\""));
        //tweets.foreach(t->System.out.println(t));
        //System.out.println("efewfwef:" + ft.count());



        //JavaPairRDD<String, String> tweet  = tweets.flatMap(s -> Arrays.asList())
        //JavaRDD<String> valid_tweets;


        //JavaRDD<SimplifiedTweet> jg = SimplifiedTweet.fromJson().get();

        /*tweets.foreach(checker->{if(SimplifiedTweet.fromJson(checker).isPresent()){

                                    } });*/


        //JavaPairRDD<String, String> tl = tweets.mapToPair(tweet->new Tuple2<>(tweet, parser.fromJson(tweet, JsonObject.class).get("lang").toString()));
        //System.out.println(tl.take(1));

        //this.writer.flush();
        //this.writer.close();



        /*
                // Load input
        JavaRDD<String> sentences = sparkContext.textFile(input);

        JavaPairRDD<String, Integer> counts = sentences
            .flatMap(s -> Arrays.asList(s.split("[ ]")).iterator())
            .map(word -> normalise(word))
            .mapToPair(word -> new Tuple2<>(word, 1))
            .reduceByKey((a, b) -> a + b);
        System.out.println("Total words: " + counts.count());
        counts.saveAsTextFile(outputDir);
        */

        return 0;
    }

    /*@Override
    public int filterLanguage(String language) throws IOException {

        //Benchmark Filter
        long startTimeFilter = System.nanoTime();
        //
        int count_tweets=0;
        String str;



        while((str=this.reader.readLine())!=null){ //While loop stops when it arrives to the end of the file
            if (str.length()>0) { //If str is an empty line it skips the if
                Optional<SimplifiedTweet> current_tweet;
                current_tweet = SimplifiedTweet.fromJson(str);
                if (current_tweet.isPresent()) { //If the tweet is valid, it continues with the if
                    //Get the language of the tweet
                    SimplifiedTweet my_tweet = current_tweet.get();
                    String tweet_lang = StringUtils.substringBetween(my_tweet.get_language(), "\"", "\"");
                    //If the language of the tweet is the same as the one that we want, we write the tweet in the output file
                    if (tweet_lang.equals(language)) {
                        this.writer.write(str);
                        this.writer.newLine();
                        this.writer.newLine();
                        count_tweets++;
                    }
                }
            }
        }



        System.out.println("file written successfully");
        this.writer.flush();
        this.input.close();
        this.reader.close();
        this.output.close();
        this.writer.close();
        //Print time for filtering
        System.out.println("Filter time: " + (float)(System.nanoTime() - startTimeFilter) / 1000000000 + " s");
        return count_tweets;
    }*/

}

