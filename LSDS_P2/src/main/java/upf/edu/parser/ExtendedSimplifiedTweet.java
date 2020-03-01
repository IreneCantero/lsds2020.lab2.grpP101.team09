package upf.edu.parser;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import javax.swing.text.html.Option;
import java.io.Serializable;
import java.util.Optional;

public class ExtendedSimplifiedTweet  implements Serializable{

  private static Gson parser = new Gson();

  private final long tweetId;              // the id of the tweet ('id')
  private final String text;              // the content of the tweet ('text')
  private final long userId;              // the user id ('user->id')
  private final String userName;          // the user name ('user'->'name')
  private final long followersCount; // the number of followers (’user’->’followers_count’)
  private final String language;          // the language of a tweet ('lang')
  private final boolean isRetweeted; // is it a retweet? (the object ’retweeted_status’ exists?)
  private final long retweetedUserId; // [if retweeted] (’retweeted_status’->’user’->’id’)
  private final long retweetedTweetId; // [if retweeted] (’retweeted_status’->’id’)
  private final long timestampMs;          // seconds from epoch ('timestamp_ms')
  private final SimplifiedTweet original_tweet;
  /*We interpreted ExtendedSimplifiedTweet as an original tweets collector. So, given an original tweet or a retweeted
  * tweet, it will alwayw store only the original tweet part.*/
  public ExtendedSimplifiedTweet(long tweetId, String text, long userId, String userName,
                                 long followersCount, String language, boolean isRetweeted,
                                 long retweetedUserId, long retweetedTweetId, long timestampMs) {
    this.tweetId = tweetId;
    this.text = text;
    this.userId = userId;
    this.userName = userName;
    this.followersCount = followersCount;
    this.language = language;
    this.isRetweeted = isRetweeted;
    this.retweetedUserId = retweetedUserId;
    this.retweetedTweetId = retweetedTweetId;
    this.timestampMs = timestampMs;
    /*Storing the original tweet conditions*/
    if(isRetweeted) this.original_tweet = new SimplifiedTweet(retweetedTweetId, text,retweetedUserId, userName, language, timestampMs);
    else
    this.original_tweet = new SimplifiedTweet(tweetId, text,userId, userName, language, timestampMs);
  }


  //Getters to get the attributes of any instance
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

  public long get_followersCount() {
    return this.followersCount;
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

  public SimplifiedTweet get_original_tweet(){ return this.original_tweet; }

  /**
   * Returns a {@link ExtendedSimplifiedTweet} from a JSON String.
   * If parsing fails, for any reason, return an {@link Optional#empty()}
   *
   * @param jsonStr
   * @return an {@link Optional} of a {@link ExtendedSimplifiedTweet}
   */

  /*The function first assings by default the tweet. Then, it checks if it is original or not. If it is, then
  * the variables remains unchanged, but if not it takes the information of 'Retweet_status' JSON. */
  public static Optional<ExtendedSimplifiedTweet> fromJson(String jsonStr) {

    //Takes the string and parses it to a JSON object
    JsonObject object = parser.fromJson(jsonStr, JsonObject.class);
    //JSON object that takes the information of the user that made the tweet
    JsonObject tweet_user = object.getAsJsonObject("user");

    Optional<JsonObject> retweet = Optional.ofNullable(object.getAsJsonObject("retweeted_status"));
    Optional<JsonObject> retweetedUser;
   /*Starting values. If uid and tid are 0 when creating an instance, that means that the tweet is original*/
    long uid = 0;
    long tid = 0;
    String text = "";
    String username="";
    String language ="";
    /*Initializing variables*/
    if(Optional.ofNullable(object).isPresent()) {
      if (Optional.ofNullable(object.get("text")).isPresent()) text = object.get("text").toString();
      if (Optional.ofNullable(object.get("lang")).isPresent()) language = object.get("lang").toString();
    }
    if(Optional.ofNullable(tweet_user).isPresent()) {
      if (Optional.ofNullable(tweet_user.get("name")).isPresent()) username = tweet_user.get("name").toString();
    }

    /*Dealing with the case when the input is a retweet*/
    if(retweet.isPresent()!=false) {
      retweetedUser = Optional.ofNullable(retweet.get().getAsJsonObject("user"));
      if (Optional.ofNullable(retweetedUser.get().get("id")).isPresent()) uid = retweetedUser.get().get("id").getAsLong();
      if (Optional.ofNullable(retweet.get().get("id")).isPresent()) tid = retweet.get().get("id").getAsLong();
      if (Optional.ofNullable(retweet.get().get("text")).isPresent()) text = retweet.get().get("text").getAsString();
      if (Optional.ofNullable(retweet.get().get("name")).isPresent()) username= retweet.get().get("name").getAsString();
    }

    long finalTid = tid;
    long finalUid = uid;
    String finalText = text;
    String finalUsername = username;
    String finalLanguage = language;
    /*Here we make sure that object and tweet_user exists, as well as the fields that were not checked in the
    if statements declared previously. If so, we create the ExtendedSimplifiedTweet*/
    Optional<ExtendedSimplifiedTweet> tweet = Optional.ofNullable(object.get("id"))
            .flatMap(followers -> Optional.ofNullable(tweet_user.get("followers_count")))
            .flatMap(timeStampMs -> Optional.ofNullable(object.get("timestamp_ms")))
            .map(a -> new ExtendedSimplifiedTweet(object.get("id").getAsLong(), finalText, tweet_user.get("id").getAsLong(),
                    finalUsername, tweet_user.get("followers_count").getAsLong(), finalLanguage,
                    Optional.ofNullable(object.get("retweeted_status")).isPresent(), finalTid, finalUid,
                    object.get("timestamp_ms").getAsLong()));
    return tweet;
  }
}
