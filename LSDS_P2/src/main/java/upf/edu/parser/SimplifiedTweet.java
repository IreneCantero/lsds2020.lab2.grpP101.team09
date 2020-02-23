package upf.edu.parser;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonElement;

import java.io.Serializable;
import java.util.Optional;

public class SimplifiedTweet implements Serializable{

  private static Gson parser = new Gson();

  private final long tweetId;              // the id of the tweet ('id')
  private final String text;              // the content of the tweet ('text')
  private final long userId;              // the user id ('user->id')
  private final String userName;          // the user name ('user'->'name')
  private final String language;          // the language of a tweet ('lang')
  private final long timestampMs;          // seconds from epoch ('timestamp_ms')

  public SimplifiedTweet( long tweetId, String text, long userId, String userName,
                         String language, long timestampMs) {

    // PLACE YOUR CODE HERE!
    this.tweetId = tweetId;
    this.text = text;
    this.userId = userId;
    this.userName = userName;
    this.language = language;
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

  public String get_language() {
    return this.language;
  }

  public long get_timestamp() {
    return this.timestampMs;
  }

  /**
   * Returns a {@link SimplifiedTweet} from a JSON String.
   * If parsing fails, for any reason, return an {@link Optional#empty()}
   *
   * @param jsonStr
   * @return an {@link Optional} of a {@link SimplifiedTweet}
   */
  public static Optional<SimplifiedTweet> fromJson(String jsonStr) {
    //Takes the string and parses it to a JSON object
    JsonObject object = parser.fromJson(jsonStr, JsonObject.class);
    //JSON object that takes the information of the user that made the tweet
    JsonObject jase = object.getAsJsonObject("user");

    //Makes sure if all the fields of the tweet exist
    // If everything exists, add information to the variable tweet (SimplifiedTweet) and then return it
    Optional<SimplifiedTweet> tweet = Optional.ofNullable(object.get("id"))
            .flatMap(text -> Optional.ofNullable(object.get("text")))
            .flatMap(userId -> Optional.ofNullable(jase.get("id")))
            .flatMap(username -> Optional.ofNullable(jase.get("name")))
            .flatMap(lang -> Optional.ofNullable(object.get("lang")))
            .flatMap(timeStampMs -> Optional.ofNullable(object.get("timestamp_ms")))
            .map(a -> new SimplifiedTweet(object.get("id").getAsLong(), object.get("text").toString(), jase.get("id").getAsLong(),
                    jase.get("name").toString(), object.get("lang").toString(), object.get("timestamp_ms").getAsLong()));
    return tweet;

  }
}
