/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter.bolt;

import java.io.FileOutputStream;
import java.io.BufferedWriter;
import java.io.Writer;
import java.io.IOException;
import java.io.OutputStreamWriter;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.HashSet;
import java.io.PrintWriter;
import java.io.FileWriter;
   
import java.io.IOException;
import java.io.OutputStreamWriter;
import storm.starter.bolt.TweetsFilter;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TopWordsBolt extends BaseBasicBolt {
  static long tupleCount = 0;
    Set<String> hashtagSet;
    HashtagEntity[] listOfTweetHashtags;
    Map<String, Integer> frequencyMap = new HashMap<String, Integer>();
    String prefixTweetByInterval = "tweetByTimeInterval_";
    String prefixTopWordsByInterval = "topWordsByTimeInterval_";
    Integer timeSlot = 0;
    int uniqueWordCount = 0;
    /**
     * Returns the top K words from the Word to count map
     * 
     * @param frequencyMap
     * @param k
     * @return
     */
    public String[] topKWordsFromMap(Map<String, Integer> frequencyMap, final int k) 
    {
        System.out.println("\n \t \t inside  topKWordsFromMap: FreMapsize = " + frequencyMap.size());
        
        final class WordFreq implements Comparable<WordFreq> 
        {
            String word;
            int freq;

            public WordFreq(final String w, final int c) {
                word = w;
                freq = c;
            }

            @Override
            public int compareTo(final WordFreq other) {
                return Integer.compare(this.freq, other.freq);
            }
        }
        PriorityQueue<WordFreq> topKHeap = new PriorityQueue<WordFreq>(k);

        // Build the topK heap
        for (java.util.Map.Entry<String, Integer> entry : frequencyMap.entrySet()) 
        {
            if (topKHeap.size() < k) {
                topKHeap.add(new WordFreq(entry.getKey(), entry.getValue()));
            } else if (entry.getValue() > topKHeap.peek().freq) {
                topKHeap.remove();
                topKHeap.add(new WordFreq(entry.getKey(), entry.getValue()));
            }
        }

        // extract the top K
        String[] topK = new String[k];
        int i = 0;
        while (topKHeap.size() > 0) 
        {
            
            String s = topKHeap.remove().word;
            if (s != null)
            {
                topK[i++] = topKHeap.remove().word;
            }
        }
         System.out.println("\n \t \t inside  topKWordsFromMap: topK = " + topK);
        return topK;
    }
  
    @Override  
    public void execute(Tuple tuple, BasicOutputCollector collector) 
    {
        Writer writer = null; 
        Writer writer2 = null; 
        if ( tuple.getSourceComponent().contentEquals("timercount") ) 
        {
            timeSlot = (Integer) tuple.getValueByField("currTimeInterval");
            if (timeSlot >= 1)
            {
                try 
                {
                    String reportFileName= prefixTopWordsByInterval + 
                                        Integer.toString(timeSlot-1) + ".csv";
                    writer = new PrintWriter(
                                new BufferedWriter(
                                new FileWriter(reportFileName, true)));
                    
                    String[] topWords = topKWordsFromMap(frequencyMap, 50);
                    System.out.println("Time Slot range = " + timeSlot + 
                        " Computed Top K words. Initiating print"); 
                    for (int i = 0; i < topWords.length; i++)
                    {
                        
                        if (null != topWords[i])
                        {
                            System.out.println("topWords list : " + topWords[i]);
                            writer.write(topWords[i] + ",");
                        }
                    }
                    writer.close();
                } 
                catch (IOException ioEx) 
                {
                }
            }
            
            frequencyMap = new HashMap<String, Integer>();
            uniqueWordCount = 0;
        }

        if ( tuple.getSourceComponent().contentEquals("print") ) 
        {
            Status status = (Status) tuple.getValueByField("filteredtweets");
            String cleaneduUpTweer = TweetToWordSplitter.cleanUpTweet(status.getText());
            System.out.print("inside prnt");

            Map<String, Integer> tokens = TweetToWordSplitter.getWordsFromTweet(cleaneduUpTweer);
            for (Map.Entry<String, Integer> currMap : tokens.entrySet()) 
            {
                String currWord = currMap.getKey();
                Integer keyCount = currMap.getValue();
              
                if (frequencyMap.containsKey(currWord))
                {
                      Integer oldKeyCount = frequencyMap.get(currWord);
                      frequencyMap.put(currWord, oldKeyCount + keyCount );
                }
                else 
                {
                  frequencyMap.put(currWord, keyCount);
                  uniqueWordCount++;
                }
            }
            System.out.print("before report " + timeSlot +" generation.");
            try 
            {
              String reportFileName= prefixTweetByInterval + Integer.toString(timeSlot) + ".csv";
                writer2 = new PrintWriter(
                            new BufferedWriter(
                            new FileWriter(reportFileName, true)));
                writer2.write(TweetToWordSplitter.cleanUpTweet(cleaneduUpTweer) + "\n");
                writer2.close();
            } 
            catch (IOException ioEx) 
            {
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldDeclarer) 
    {
      outputFieldDeclarer.declare(new Fields("filteredtweets"));
    }

}
