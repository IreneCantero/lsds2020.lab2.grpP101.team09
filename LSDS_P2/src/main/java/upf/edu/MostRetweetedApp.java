package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import upf.edu.parser.ExtendedSimplifiedTweet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MostRetweetedApp {



    public static void main(String[] args) {

        List<String> argsList = Arrays.asList(args);
        String language = argsList.get(0);
        String outputFile = argsList.get(1);
        String bucket = argsList.get(2);

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("Most Retweeted");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        //List<MoreExtendedSimplifiedTweet> tw = new ArrayList<ExtendedSimplifiedTweet>();

        for (String inputFile : argsList.subList(3, argsList.size())) {
            System.out.println("Processing: " + inputFile);

            JavaRDD<String> tweets = sparkContext.textFile(inputFile);

            JavaRDD<ExtendedSimplifiedTweet> tst = tweets
                    .filter(t -> t.length() > 0 && ExtendedSimplifiedTweet.fromJson(t).isPresent())
                    .map(s -> ExtendedSimplifiedTweet.fromJson(s).get())
                    .filter(x -> x.get_isRetweeted()==true);
            System.out.println("count tst: " + tst.count());
            System.out.println(tst.take(1));

            /*JavaPairRDD<Long, Long> tot_rt_per_user = tst.mapToPair(t -> new Tuple2<>(t.get_userId(), t.get_retweet_count()))
                    .reduceByKey((a,b) -> a + b)
                    .mapToPair(mw -> new Tuple2<>(mw._2, mw._1))
                    .sortByKey(false)
                    .mapToPair(mr -> new Tuple2<>(mr._2, mr._1));*/
            //System.out.println("TOP 10 USERS: " + tot_rt_per_user.take(10));

        }
    }

}
