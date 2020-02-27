package upf.edu;

import com.google.gson.Gson;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import upf.edu.parser.ExtendedSimplifiedTweet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;


public class BigGramsApp{
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
                        //System.out.println("MY BIGRAM: "+ s3);
                        bigrams.add(s3.toString());
                       // System.out.println(s3);
                        //bigrams.add("fgiuefbwiebfvipewbfpquebfpuwiefb");
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
        String input = argsList.get(0);
        String outputDir = argsList.get(1);

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("BiGram Count");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        List<ExtendedSimplifiedTweet> efs = new ArrayList<ExtendedSimplifiedTweet>();

        for (String inputFile : argsList.subList(3, argsList.size())) {
            System.out.println("Processing: " + inputFile);

            JavaRDD<String> tweets = sparkContext.textFile(inputFile);
            JavaRDD<ExtendedSimplifiedTweet> tst = tweets
                    .filter(t -> t.length() > 0 && ExtendedSimplifiedTweet.fromJson(t).isPresent())
                    .map(s -> ExtendedSimplifiedTweet.fromJson(s).get())
                    .filter(r -> r.get_isRetweeted()==false);
            List <ExtendedSimplifiedTweet> aux = tst.collect();
            efs.addAll(aux);
        }

        // Load filtered original tweets
        JavaRDD<ExtendedSimplifiedTweet> result = sparkContext.parallelize(efs);
        JavaRDD<String> content = result.map(s->s.get_text().toLowerCase());

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
        //counts.saveAsTextFile(outputDir);
    }

    private static String normalise(String word) {
        return word.trim().toLowerCase();
    }
}