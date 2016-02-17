package storm.starter.bolt;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;


public class TweetToWordSplitter {
    static Set<String> stopWords = new HashSet<String>();
    
    static {
        Scanner s;
        try {
            s = new Scanner(new File("stopWords.txt"));
            while(s.hasNext()){
                String word=s.nextLine();
                if(word!=null){
                    stopWords.add(word.trim());
                }
            }
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("Loaded file finally : " + stopWords);
    }
    
    static void loadStopWordsFromFile(String fileName){
        Scanner s;
        Set<String> stopWordsTemp = new HashSet<String>();
        try {
            s = new Scanner(new File("stopWords.txt"));
            while(s.hasNext()){
                String word=s.nextLine();
                if(word!=null){
                    stopWordsTemp.add(word.trim());
                }
            }
            stopWords=stopWordsTemp;
            System.out.println("Loaded From External File  : " + fileName + ", words are :"+ stopWordsTemp);
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
    
    public static String cleanUpTweet(String origTweet)
    {
        String cleanedWordNoPunctuations=origTweet.replace(".", " ").replace(",", " ").replace("'", "");

        cleanedWordNoPunctuations = cleanedWordNoPunctuations.replaceAll("[\\t\\n\\r]"," ");
        cleanedWordNoPunctuations = cleanedWordNoPunctuations.replaceAll("[^#\\s]*#\\S*", "");
        cleanedWordNoPunctuations = cleanedWordNoPunctuations.replaceAll("http\\S+\\s?", "");
        cleanedWordNoPunctuations = cleanedWordNoPunctuations.replaceAll("[^a-zA-Z\\s]", "");
        cleanedWordNoPunctuations = cleanedWordNoPunctuations.replaceAll("[^@\\s]*@\\S*", "");       
        cleanedWordNoPunctuations = cleanedWordNoPunctuations.trim();
        cleanedWordNoPunctuations = cleanedWordNoPunctuations.toLowerCase();
        return cleanedWordNoPunctuations;
    }
    
    public static Map<String, Integer> getWordsFromTweet(String tweet){
        Map<String, Integer> word2CountMap= new HashMap<String, Integer>();
        
        if(tweet==null || tweet.trim().length()==0){
            return word2CountMap;
        }
        
        String[] words=cleanUpTweet(tweet).split("\\s+");
        for(String w : words){
            
            String cleanedWordNoPunctuations = cleanUpTweet(w);
            if(stopWords.contains(cleanedWordNoPunctuations) || cleanedWordNoPunctuations.isEmpty() ){
                continue;
            }
            if(!word2CountMap.containsKey(cleanedWordNoPunctuations)){
                word2CountMap.put(cleanedWordNoPunctuations, 1);
            }else{
                word2CountMap.put(cleanedWordNoPunctuations, word2CountMap.get(cleanedWordNoPunctuations)+1);
            }
        }   
        return word2CountMap;
    }
    
    public static void main(String[] args) {
        loadStopWordsFromFile("stopWords.txt");
        System.out.println( getWordsFromTweet("This is main function in TweetToWordSplitter."));
    }

}
