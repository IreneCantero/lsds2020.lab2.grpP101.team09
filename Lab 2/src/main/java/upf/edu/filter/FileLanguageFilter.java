package upf.edu.filter;
import upf.edu.parser.SimplifiedTweet;
import java.io.*;
import org.apache.commons.lang3.StringUtils;
import java.util.Optional;

public class FileLanguageFilter implements LanguageFilter {
    private final FileReader input;
    private final BufferedReader reader;
    private final FileWriter output;
    private final BufferedWriter writer;
    public FileLanguageFilter(String input, String output) throws IOException {

        this.input = new FileReader(input);
        this.reader = new BufferedReader(this.input);
        this.output = new FileWriter(output, true);
        this.writer = new BufferedWriter(this.output);
    }
    @Override
    public int filterLanguage(String language) throws IOException {
        //Benchmark Filter
        long startTimeFilter = System.nanoTime();
        //
        int count_tweets=0;
        String str;

        while((str=this.reader.readLine())!=null){ //While loop stops when it arrives to the end of the file
            if (str.length()>0) { //If str is an empty line it skips the if
                Optional<SimplifiedTweet> current_tweet;
                current_tweet = SimplifiedTweet.fromJson(str);
                if (current_tweet.isPresent()) { //If the tweet is valid, it continues with the if
                    //Get the language of the tweet
                    SimplifiedTweet my_tweet = current_tweet.get();
                    String tweet_lang = StringUtils.substringBetween(my_tweet.get_language(), "\"", "\"");
                    //If the language of the tweet is the same as the one that we want, we write the tweet in the output file
                    if (tweet_lang.equals(language)) {
                        this.writer.write(str);
                        this.writer.newLine();
                        this.writer.newLine();
                        count_tweets++;
                    }
                }
            }
        }
        System.out.println("file written successfully");
        this.writer.flush();
        this.input.close();
        this.reader.close();
        this.output.close();
        this.writer.close();
        //Print time for filtering
        System.out.println("Filter time: " + (float)(System.nanoTime() - startTimeFilter) / 1000000000 + " s");
        return count_tweets;
    }

}

