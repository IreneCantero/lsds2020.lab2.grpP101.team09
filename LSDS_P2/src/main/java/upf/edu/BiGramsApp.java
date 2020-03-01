package upf.edu;

import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import upf.edu.parser.ExtendedSimplifiedTweet;
import upf.edu.parser.SimplifiedTweet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;


public class BiGramsApp {

    /*function to create bigrams of a given JavaRDD input*/
    public static List<String> getBiGrams(JavaRDD<String> input) {
        List<String> input_bigrams = input.collect();
        List <String> bigrams = new ArrayList<String>();
        for(String str: input_bigrams) {
            StringTokenizer itr = new StringTokenizer(str);
            if (itr.countTokens() > 1) {
                String s1 = "";
                String s2 = "";
                String s3 = "";
                while (itr.hasMoreTokens()) {
                    if (s1.isEmpty())
                        s1 = itr.nextToken();
                    s2 = itr.nextToken();
                    if(s2.isEmpty()==false) {
                        s3 = s1 + " " + s2;
                        s3.replace("\"", "");
                        bigrams.add(s3.toString());
                        s1 = s2;
                        s2 = "";
                    }
                }
            }
        }
        return bigrams;
    }

    public static void main(String[] args){
        List<String> argsList = Arrays.asList(args);
        String language = argsList.get(0);
        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("BiGram Count");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        List<SimplifiedTweet> all_original_tweets = new ArrayList<SimplifiedTweet>();

        for (String inputFile : argsList.subList(1, argsList.size())) {
            System.out.println("Processing: " + inputFile);

            JavaRDD<String> tweets = sparkContext.textFile(inputFile);
            /*We get only the original tweets, not the retweeted ones. Note that, this JavaRDD is of SimplifiedTweet,
            * because we created an attribute storing the "retweet_status" (the original tweet) of each retweet. We also
            * filter by language, to only get the most popular bigrams of a given language.
            *
            * NOTE: we realized the creation of a SimplifiedTweet is redundant, but we find appropiated that in case
            * isRetweeted is true, it stores the original tweet instead of the retweet, since the purpose of the
            * ExtendedSimplifiedTweet is to collect the original tweets.*/
            JavaRDD<SimplifiedTweet> collector = tweets
                    .filter(t -> t.length() > 0 && ExtendedSimplifiedTweet.fromJson(t).isPresent())
                    .map(s -> ExtendedSimplifiedTweet.fromJson(s).get())
                    .filter(r -> r.get_isRetweeted()==false)
                    .filter(g -> g.get_language().equals("\"" + language + "\""))
                    .map(g -> g.get_original_tweet());
            List <SimplifiedTweet> aux = collector.collect();
            all_original_tweets.addAll(aux);
        }

        // Load filtered original tweets
        JavaRDD<SimplifiedTweet> result = sparkContext.parallelize(all_original_tweets);
        /*Get the text of each original tweet*/
        JavaRDD<String> content = result
                .map(s->s.get_text().trim().toLowerCase())
                .distinct();
        /*Get the bigrams of all texts*/
        List<String> StringBiGrams = getBiGrams(content);
        JavaRDD<String> biGrams = sparkContext.parallelize(StringBiGrams);
        /*Do a count of each bigram to get the most popular ones. It is sorted from the most popular to the less popular*/
        JavaPairRDD<Tuple2, Integer> counts = biGrams
                .map(s -> s.split("\\s+"))
                .mapToPair(word -> new Tuple2<>(new Tuple2<>(word[0],word[1]), 1))
                .reduceByKey((a, b) -> a + b)
                .mapToPair(myWord -> new Tuple2<>((Integer)myWord._2, (Tuple2)myWord._1))
                .sortByKey(false)
                .mapToPair(myResult -> new Tuple2<>((Tuple2)myResult._2, (Integer)myResult._1));

        System.out.println("Most popular bigrams: " + counts.take(10));
    }

}