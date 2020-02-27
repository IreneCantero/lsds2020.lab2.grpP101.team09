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
    public static String stringParser(ExtendedSimplifiedTweet myObj){
        Gson parser = new Gson();
        return parser.toJson(myObj);
    }
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

    //IT DOESNT TAKE INTO ACCOUNT LANGUAGES!!!!!!! CHANGE IT!!!!!!!!!!!
    //CHECK INPUTS ARGUMENTS
    public static void main(String[] args){
        List<String> argsList = Arrays.asList(args);
        String language = argsList.get(0);
        String outputFile = argsList.get(1);
        String bucket = argsList.get(2);

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("BiGram Count");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        List<SimplifiedTweet> efs = new ArrayList<SimplifiedTweet>();

        for (String inputFile : argsList.subList(3, argsList.size())) {
            System.out.println("Processing: " + inputFile);

            JavaRDD<String> tweets = sparkContext.textFile(inputFile);
            JavaRDD<SimplifiedTweet> tst = tweets
                    .filter(t -> t.length() > 0 && ExtendedSimplifiedTweet.fromJson(t).isPresent())
                    .map(s -> ExtendedSimplifiedTweet.fromJson(s).get())
                    .filter(r -> r.get_isRetweeted()==false)
                    .filter(g -> g.get_language().equals("\"" + language + "\""))
                    .map(g -> g.get_original_tweet());
            List <SimplifiedTweet> aux = tst.collect();
            efs.addAll(aux);
        }

        // Load filtered original tweets
        JavaRDD<SimplifiedTweet> result = sparkContext.parallelize(efs);
        JavaRDD<String> content = result
                .map(s->s.get_text().trim().toLowerCase())
                .distinct();
        List<String> StringBiGrams = getBiGrams(content);
        JavaRDD<String> biGrams = sparkContext.parallelize(StringBiGrams);

        JavaPairRDD<Tuple2, Integer> counts = biGrams
                .map(s -> s.split("\\s+"))
                .mapToPair(word -> new Tuple2<>(new Tuple2<>(word[0],word[1]), 1))
                .reduceByKey((a, b) -> a + b)
                .mapToPair(myWord -> new Tuple2<>((Integer)myWord._2, (Tuple2)myWord._1))
                .sortByKey(false)
                .mapToPair(myResult -> new Tuple2<>((Tuple2)myResult._2, (Integer)myResult._1));

        System.out.println("Total words: " + counts.take(10));
    }

    private static String normalise(String word) {
        return word.trim().toLowerCase();
    }
}