package upf.edu.parser;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.Serializable;
import java.util.Optional;

public class MoreExtendedSimplifiedTweet implements Serializable{
    private static Gson parser = new Gson();

    private final long tweetId;              // the id of the tweet ('id')
    private final String text;              // the content of the tweet ('text')
    private final long userId;              // the user id ('user->id')
    private final String userName;          // the user name ('user'->'name')
    private final String language;          // the language of a tweet ('lang')
    private final boolean isRetweeted;      // is it a retweet? (the object ’retweeted_status’ exists?)
    private final long retweetedUserId;     // [if retweeted] (’retweeted_status’->’user’->’id’)
    private final long retweetedTweetId;     // [if retweeted] (’retweeted_status’->’id’)
    private final long timestampMs;          // seconds from epoch ('timestamp_ms')
    private final long retweet_count;
    private final String original_text;

    /*Additional class to deal with exercise 4. The difference is that it also stores in an instance the retweet_count
    * and the text of an original tweet. Additionally, it stores the retweets and some meaningful information of the
    * original tweet.*/
    public MoreExtendedSimplifiedTweet(long tweetId, String text, long userId, String userName,
                                       String language, boolean isRetweeted,
                                       long retweetedUserId, long retweetedTweetId, long timestampMs,
                                       long retweet_count, String original_text) {

        this.tweetId = tweetId;
        this.text = text;
        this.userId = userId;
        this.userName = userName;
        this.language = language;
        this.isRetweeted = isRetweeted;
        this.retweetedUserId = retweetedUserId;
        this.retweetedTweetId = retweetedTweetId;
        this.timestampMs = timestampMs;
        this.retweet_count = retweet_count;
        this.original_text = original_text;

    }


    //Getters to get the value of any instance
    public long get_tweetId() {
        return this.tweetId;
    }

    public String get_text() {
        return this.text;
    }

    public long get_userId() {
        return this.userId;
    }

    public String get_username() {
        return this.userName;
    }

    public String get_language() {
        return this.language;
    }

    public boolean get_isRetweeted() {
        return this.isRetweeted;
    }

    public long get_retweetedUserId() {
        return this.retweetedUserId;
    }

    public long get_retweetedTweetId() { return this.retweetedTweetId;}

    public long get_timestamp() {
        return this.timestampMs;
    }

    public long get_retweet_count() {
        return this.retweet_count;
    }

    public String get_original_text() {
        return this.original_text;
    }

    /**
     * Returns a {@link SimplifiedTweet} from a JSON String.
     * If parsing fails, for any reason, return an {@link Optional#empty()}
     *
     * @param jsonStr
     * @return an {@link Optional} of a {@link SimplifiedTweet}
     */
    public static Optional<MoreExtendedSimplifiedTweet> fromJson(String jsonStr) {
        //Takes the string and parses it to a JSON
        JsonObject object = parser.fromJson(jsonStr, JsonObject.class);
        //JSON object that takes the information of the user that made the tweet
        JsonObject tweet_user = object.getAsJsonObject("user");
        JsonObject retweetedTweet;
        JsonObject retweetedUser;
        Optional<JsonObject> retweeted_status = Optional.ofNullable(object.getAsJsonObject("retweeted_status"));
        Optional<JsonObject> retweeted_status_user;
        /*We check that the tweet is a retweet by checking if the JSON has a 'retweet_status'*/
        if(retweeted_status.isPresent()==false) {
            return Optional.empty();
        }
        else{
            /*We check that the 'retweet_status' has a user.*/
            retweeted_status_user = Optional.ofNullable(retweeted_status.get().getAsJsonObject("user"));
            if(retweeted_status_user.isPresent()==false){
                return Optional.empty();
            }
            /*If the previous conditions are satisfied, we get the information of the 'retweet_status' (original tweet)*/
            else{
                retweetedTweet = retweeted_status.get();
                retweetedUser = retweeted_status_user.get();
            }
        }
        /*We make sure that all attributes exists, and then we create the instance*/
        Optional<MoreExtendedSimplifiedTweet> tweet = Optional.ofNullable(object.get("id"))
                .flatMap(text -> Optional.ofNullable(object.get("text")))
                .flatMap(userId -> Optional.ofNullable(tweet_user.get("id")))
                .flatMap(username -> Optional.ofNullable(tweet_user.get("name")))
                .flatMap(lang -> Optional.ofNullable(object.get("lang")))
                .flatMap(rtUserID -> Optional.ofNullable(retweetedUser.get("id")))
                .flatMap(rttID -> Optional.ofNullable(retweetedTweet.get("id")))
                .flatMap(timeStampMs -> Optional.ofNullable(object.get("timestamp_ms")))
                .flatMap(rt_count -> Optional.ofNullable(retweetedTweet.get("retweet_count")))
                .flatMap(or_text -> Optional.ofNullable(retweetedTweet.get("text")))
                .map(a -> new MoreExtendedSimplifiedTweet(object.get("id").getAsLong(), object.get("text").toString(),
                        tweet_user.get("id").getAsLong(), tweet_user.get("name").toString(), object.get("lang").toString(),
                        Optional.ofNullable(object.get("retweeted_status")).isPresent(),
                        retweetedUser.get("id").getAsLong(), retweetedTweet.get("id").getAsLong(),
                        object.get("timestamp_ms").getAsLong(),
                        retweetedTweet.get("retweet_count").getAsLong(), retweetedTweet.get("text").getAsString()));
        return tweet;
    }
}
