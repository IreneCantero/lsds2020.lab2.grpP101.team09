import java.io.IOException;
import java.util.StringTokenizer;

import upf.edu.parser.ExtendedSimplifiedTweet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class BiGram {
    String first;
    String right;
}

public class BigGramsApp{

    public List<String> getBiGrams(JavaRDD<String> input) {
        String str = input.toString();
        List<String> bigrams = new ArrayList<String>();
        StringTokenizer itr = new StringTokenizer(str);
        if(itr.countTokens() > 1)
        {
            System.out.println("String array size : " + itr.countTokens());
            String s1 = "";
            String s2 = "";
            String s3 = "";
            while (itr.hasMoreTokens())
            {
                if(s1.isEmpty())
                    s1 = itr.nextToken();
                s2 = itr.nextToken();
                s3 = s1 + " " + s2;
                bigrams.add(s3);
                s1 = s2;
                s2 = "";
            }
        }
        return bigrams
    }

    public static void main(String[] args){
        List<String> argsList = Arrays.asList(args);
        String input = argsList.get(0);
        String outputDir = argsList.get(1);

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("Bi Gram Count");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        List<ExtendedSimplifiedTweet> efs = new ArrayList<ExtendedSimplifiedTweet>();

        for (String inputFile : argsList.subList(3, argsList.size())) {
            System.out.println("Processing: " + inputFile);

            JavaRDD<String> tweets = sparkContext.textFile(inputFile);
            JavaRDD<ExtendedSimplifiedTweet> tst = tweets
                    .filter(t -> t.length() > 0 && ExtendedSimplifiedTweet.fromJson(t).isPresent())
                    .map(s -> ExtendedSimplifiedTweet.fromJson(s).get())
                    .filter(r -> r.get_isRetweeted().equals(false));
            List <ExtendedSimplifiedTweet> aux = tst.collect();
            efs.addAll(aux);
        }

        // Load filtered original tweets
        JavaRDD<ExtendedSimplifiedTweet> result = sparkContext.parallelize(efs);
        JavaRDD<String> content = result.map(s->stringParser(s));
        List<String> StringBiGrams = getBiGrams(content);
        JavaRDD<String> biGrams = sparkContext.parallelize(StringBiGrams);

        JavaPairRDD<String, Integer> counts = biGrams
                .flatMap(s -> Arrays.asList(s.split("[ ]")).iterator())
                .map(word -> normalise(word))
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey((a, b) -> a + b);
        System.out.println("Total words: " + counts.count());
        counts.saveAsTextFile(outputDir);
    }

    private static String normalise(String word) {
        return word.trim().toLowerCase();
    }
}