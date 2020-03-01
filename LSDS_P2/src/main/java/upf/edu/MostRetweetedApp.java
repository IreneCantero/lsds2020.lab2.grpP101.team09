package upf.edu;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.*;
import upf.edu.parser.MoreExtendedSimplifiedTweet;
import java.lang.Long;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

public class MostRetweetedApp {


    /*NOTE: we created a new class called MoreExtendedSimplifiedTweet to get the original tweet counts and the original
    * text*/
    public static void main(String[] args) {

        List<String> argsList = Arrays.asList(args);

        //Create a SparkContext to initialize
        SparkConf conf = new SparkConf().setAppName("Most Retweeted");
        JavaSparkContext sparkContext = new JavaSparkContext(conf);
        /*Little piece of code to concatenate inputs*/
        String input = "";
        for (int i = 0 ; i < argsList.size() ; i++){
            if(i == argsList.size()-1) input+=argsList.get(i);
            else input+=(argsList.get(i)+",");
        }
        long startTimeTotal = System.nanoTime();

        System.out.println("Processing: " + input);
        /*We take all inputs as they were only one and do the following steps:*/
        JavaRDD<String> tweets = sparkContext.textFile(input);
        /*STEP 1: Getting all retweeted tweets*/
        JavaRDD<MoreExtendedSimplifiedTweet> retweeted_tweets = tweets
                .filter(t->t.length()>0 && MoreExtendedSimplifiedTweet.fromJson(t).isPresent())
                .map(s -> MoreExtendedSimplifiedTweet.fromJson(s).get());
        /*STEP 2: From those, we get the most recent retweets, because when there is a retweet, the original tweet
        gets updated in the "retweet_status" part*/
        JavaPairRDD<Tuple3<Long, Long, Long>, Integer> max_timestmp= retweeted_tweets
                .mapToPair(x-> new Tuple2<>(new Tuple2<>(x.get_retweetedTweetId(),x.get_retweetedUserId()), x.get_timestamp()))
                .reduceByKey((y,z)->Math.max(y,z)).map(q-> (new Tuple3<Long,Long,Long>(q._1._1, q._1._2, q._2)))
                .mapToPair(r -> new Tuple2(r,1));
        /*STEP 3: Transforming the data to get the relevant information alongside the retweet of all retweeted tweets*/
        JavaPairRDD<Tuple3<Long, Long, Long>, MoreExtendedSimplifiedTweet> rt_info = retweeted_tweets
                .mapToPair(x->new Tuple2<>(new Tuple3<>(x.get_retweetedTweetId(),x.get_retweetedUserId(),x.get_timestamp()),x));
        /*STEP 4: Getting the most recent rt from each original tweet.Then, getting the counts of the original tweets
        (info taken from the retweets)*/
        JavaPairRDD<Tuple3<Long, Long, Long>, Tuple2<MoreExtendedSimplifiedTweet, Integer>> recent = rt_info.join(max_timestmp);
        JavaPairRDD<Tuple3<Long,Long,Long>, Long> with_count = recent.mapToPair(x->new Tuple2<>(x._1 ,x._2._1.get_retweet_count()));

        /*STEP 5: Sort in descending order the users with maximum counts*/
        JavaPairRDD<Long, Long> topUsers = with_count
                .mapToPair(x-> new Tuple2 <>(x._1._2(), x._2))
                .reduceByKey((y,z)->y+z)
                .mapToPair(f-> new Tuple2<> (f._2,f._1))
                .sortByKey(false)
                .mapToPair(g->new Tuple2<>(g._2,g._1));
        /*STEP 6: get the top 10 users*/
        List<Long> users = topUsers.map(x->x._1).take(10);
        /*STEP 7: get the tweet of each user that has maximum retweet_count*/
        JavaPairRDD<Long,Long> most_rt_t = with_count
                .mapToPair(x-> new Tuple2<>(x._1._2(), x._2))
                .filter(y->users.contains(y._1))
                .reduceByKey((a,b)->Math.max(a,b));

        List<Tuple2<Long,Long>> most_rt_counts= most_rt_t.collect();
        /*STEP 8: Put the original tweet user ID, the original tweet ID, the original tweet count and the original text*/
        JavaRDD<Tuple4<Long,Long,Long, String>> result = retweeted_tweets
                .map(x -> new Tuple4<>(x.get_retweetedUserId(),x.get_retweetedTweetId(), x.get_retweet_count(), x.get_original_text()))
                .filter(m -> most_rt_counts.contains(new Tuple2<>(m._1(), m._3())))
                .sortBy(y->y._3(),false,1);

        System.out.println("Final Result:  " + result.collect());

    }

}
