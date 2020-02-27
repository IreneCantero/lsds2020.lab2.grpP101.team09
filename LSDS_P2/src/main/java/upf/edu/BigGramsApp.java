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
        List<String> bigrams = input.collect();
        for(String str: bigrams) {
            StringTokenizer itr = new StringTokenizer(str);
            if (itr.countTokens() > 1) {
                System.out.println("String array size : " + itr.countTokens());
                String s1 = "";
                String s2 = "";
                String s3 = "";
                while (itr.hasMoreTokens()) {
                    if (s1.isEmpty())
                        s1 = itr.nextToken();
                    s2 = itr.nextToken();
                    s3 = s1 + " " + s2;
                    bigrams.add(s3);
                    s1 = s2;
                    s2 = "";
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
        JavaRDD<String> content = result.map(s->s.get_text());
        List<String> StringBiGrams = getBiGrams(content);
        JavaRDD<String> biGrams = sparkContext.parallelize(StringBiGrams);
        System.out.println("partial words: " + biGrams.count());
        JavaPairRDD<String, Integer> counts = biGrams
                .flatMap(s -> Arrays.asList(s.split("[ ]")).iterator())
                .map(word -> normalise(word))
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b)
                .sortByKey(false);
        System.out.println("Total words: " + counts.count());
        //counts.saveAsTextFile(outputDir);
    }

    private static String normalise(String word) {
        return word.trim().toLowerCase();
    }
}