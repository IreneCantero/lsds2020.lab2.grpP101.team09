package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple2$mcCC$sp;
import upf.edu.parser.MoreExtendedSimplifiedTweet;

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

            JavaRDD<MoreExtendedSimplifiedTweet> tst = tweets
                    .filter(t->t.length()>0 && MoreExtendedSimplifiedTweet.fromJson(t).isPresent())
                    .map(s -> MoreExtendedSimplifiedTweet.fromJson(s).get());

            JavaPairRDD<Long,Long> ugh = tst.mapToPair(w -> new Tuple2<>(w.get_retweetedUserId(), w.get_retweet_count()))
                    .reduceByKey((a, b) -> a+b)
                    .mapToPair(mw -> new Tuple2<>(mw._2, mw._1))
                    .sortByKey(false)
                    .mapToPair(mr -> new Tuple2<>(mr._2, mr._1))
                    .distinct();

            JavaRDD<Long> TOP10users = ugh.map(x -> x._1);
            List<Long> users = TOP10users.take(10);

            JavaRDD<MoreExtendedSimplifiedTweet> tt10rt = tst.filter(t -> users.contains(t.get_retweetedUserId()));

            JavaPairRDD<Long, Tuple2> most_rt_tweets = tt10rt
                .mapToPair(x -> new Tuple2<Long, Tuple2>(x.get_retweetedUserId(), new Tuple2<Long, Long>(x.get_retweetedTweetId(),x.get_retweet_count())));
                    //.max((a,b)-> a._2._2 )

            System.out.println("uiwehfiuewbhf: "+most_rt_tweets.take(5));

            //System.out.println("TOP 10 USERS: " + tot_rt_per_user.take(10));
            //Halooooooooooooooo: [(24679473,3756979), (38381308,2127042), (1501434991,1791122), (3143260474,1269714), (3480478035,1225842)]
        }
    }

}
