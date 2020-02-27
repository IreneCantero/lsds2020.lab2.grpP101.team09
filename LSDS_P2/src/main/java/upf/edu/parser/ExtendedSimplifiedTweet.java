package upf.edu.parser;

import com.google.gson.Gson;
import com.google.gson.JsonObject;

import java.io.Serializable;
import java.util.Optional;

public class ExtendedSimplifiedTweet implements Serializable{

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

  public ExtendedSimplifiedTweet(long tweetId, String text, long userId, String userName,
                                 long followersCount, String language, boolean isRetweeted,
                                 long retweetedUserId, long retweetedTweetId, long timestampMs) {

    // PLACE YOUR CODE HERE!
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

  /**
   * Returns a {@link ExtendedSimplifiedTweet} from a JSON String.
   * If parsing fails, for any reason, return an {@link Optional#empty()}
   *
   * @param jsonStr
   * @return an {@link Optional} of a {@link ExtendedSimplifiedTweet}
   */

  public static Optional<ExtendedSimplifiedTweet> fromJson(String jsonStr) {

    //Takes the string and parses it to a JSON object
    JsonObject object = parser.fromJson(jsonStr, JsonObject.class);
    //JSON object that takes the information of the user that made the tweet
    JsonObject jase = object.getAsJsonObject("user");
    JsonObject retweet;
    JsonObject retweetedUser;
    Optional<JsonObject> aux1 = Optional.ofNullable(object.getAsJsonObject("retweeted_status"));
    Optional<JsonObject> aux2 = Optional.empty();
    if(aux1.isPresent()!=false) {
      aux2 = Optional.ofNullable(aux1.get().getAsJsonObject("user"));
    }
    if(aux2.isPresent()){
      retweet = aux1.get();
      retweetedUser = aux2.get();
    }
    else{
      return Optional.empty();
    }

    //Makes sure if all the fields of the tweet exist
    // If everything exists, add information to the variable tweet (SimplifiedTweet) and then return it
    Optional<ExtendedSimplifiedTweet> tweet = Optional.ofNullable(object.get("id"))
            .flatMap(text -> Optional.ofNullable(object.get("text")))
            .flatMap(userId -> Optional.ofNullable(jase.get("id")))
            .flatMap(username -> Optional.ofNullable(jase.get("name")))
            .flatMap(followers -> Optional.ofNullable(jase.get("followers_count")))
            .flatMap(lang -> Optional.ofNullable(object.get("lang")))
            .flatMap(retweetUserId -> Optional.ofNullable(retweetedUser.get("id")))
            .flatMap(retweetTweetId -> Optional.ofNullable(retweet.get("id")))
            .flatMap(timeStampMs -> Optional.ofNullable(object.get("timestamp_ms")))
            .map(a -> new ExtendedSimplifiedTweet(object.get("id").getAsLong(), object.get("text").toString(), jase.get("id").getAsLong(),
                     jase.get("name").toString(), jase.get("followers_count").getAsLong(), object.get("lang").toString(),
                    Optional.ofNullable(object.get("retweeted_status")).isPresent(), retweetedUser.get("id").getAsLong(), retweet.get("id").getAsLong(),
                    object.get("timestamp_ms").getAsLong()));
    return tweet;
  }
}
