package upf.edu;

import upf.edu.*;
import org.junit.Test;
//import upf.edu.parser.SimplifiedTweet;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Optional;

import static org.junit.Assert.*;

/**
 * Unit test for simple App.
 */
public class TwitterFilterTest
{
    /**
     * Rigorous Test :-)
     */
   /* @Test
    public void shouldAnswerWithTrue()
    {
        assertTrue( true );
    }

    @Test
    public void shouldGetTwitter() throws IOException {
        //In this test we assure the function fromJson in SimplifiedTweet.java does its work correctly
        //Inorder to do so, we know the that the first line in Eurovision5.json is a valid tweet and we know its text
        FileReader file = new FileReader("/home/ari/Escritorio/Eurovision5.json");
        BufferedReader reader = new BufferedReader(file);
        String line = reader.readLine();
        Optional <SimplifiedTweet> myTweet = SimplifiedTweet.fromJson(line);
        if(myTweet.isPresent()==true) {
            SimplifiedTweet tweet = myTweet.get();
            assertEquals("\"Proud you kept it classy @surieofficial #EUROVISION https://t.co/nzxJ5LT8Xu\"", tweet.get_text());
        }
        else{
            assertTrue(false);
        }
    }
    // Place your code here

    @Test
    public void shouldNotGetEmptyLines() throws IOException {
        //We want to make sure that empty lines are not considered
        //We know the second line read in Eurovision5.json is empty
        FileReader file = new FileReader("/home/ari/Escritorio/Eurovision5.json");
        BufferedReader reader = new BufferedReader(file);
        String line = reader.readLine();
        line = reader.readLine();
        assertFalse(line.length()>0);
    }

    @Test
    public void InvalidTwitter() throws IOException {
        //We want to assure that invalid tweets are not counted as valid
        //We know that in the line 99 of Eurovision5.json there is one, so we read the file until we arrive to
        //the specific line
        FileReader file = new FileReader("/home/ari/Escritorio/Eurovision5.json");
        BufferedReader reader = new BufferedReader(file);
        String line = reader.readLine();
        for(int i=2; i<100; i++){
            line = reader.readLine();
        }
        System.out.println(line);
        Optional <SimplifiedTweet> myTweet = SimplifiedTweet.fromJson(line);
        assertFalse(myTweet.isPresent());
    }*/

}
