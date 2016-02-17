package storm.starter.bolt;
import storm.starter.bolt.TweetToWordSplitter;

import java.util.Map;
import java.util.Set;

import twitter4j.Status;


public class TweetsFilter {

    static void filterTweet(Status s , Set<String> validTags, int friendCount){
        
        /**
         *  Discard when friendCount from current tweet user is less then input MaxLimit
         */
        if(s.getUser().getFriendsCount() < friendCount ){
            return;
        }
        
        /**
         * See if Tweet contains valid Tag in it
         */
        Map<String, Integer> tweetWord2Count = TweetToWordSplitter.getWordsFromTweet(s.getText());
        boolean containsValidTag=false;
        for(String t:validTags){
            if(tweetWord2Count.containsKey(t)){
                containsValidTag=true;
                break;
            }
        }
        
        /**
         * Discard when TWEET doesnt contain valid TAG
         */
        if(containsValidTag==false){
            return;
        }
        
        // TODO: Pass this to another bolt. That will receive generate the RollingCount.
        System.out.println(tweetWord2Count);
    }
    
}
