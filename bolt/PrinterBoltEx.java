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
import java.util.Set;
import twitter4j.HashtagEntity;
import twitter4j.Status;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class PrinterBoltEx extends BaseBasicBolt {
  static long tupleCount = 0;
    Set<String> hashtagSet;
    HashtagEntity[] listOfTweetHashtags;
  
  
  
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) {
    
    //System.out.println(tuple);
    boolean containsValidTag=false;
    Integer maxFriendCnt = 500;
    // TODO: Retrieve all 3 values and check the condition specified in the list.
    Status status;
    
    if (tuple.getSourceComponent().contentEquals("friendcount"))
    {
        maxFriendCnt = (Integer) tuple.getValueByField("friendsCnt");
        System.out.println("Max Friend Count = "+maxFriendCnt);
    }
    
    if (tuple.getSourceComponent().contentEquals("hashTagGen")){
            System.out.println("Received hashtag list");
            hashtagSet = (Set<String>) tuple.getValueByField("hashtag");
            //System.out.println(hashtagSet.toString());
        }
      
        if (tuple.getSourceComponent().contentEquals("twitter"))
        {

            status = (Status) tuple.getValueByField("tweet");

            if (status.getUser().getFriendsCount() > maxFriendCnt)
            {
                return;
            }
            
            
            
            listOfTweetHashtags = status.getHashtagEntities();
            if ( listOfTweetHashtags != null ) 
            {
                 containsValidTag=false;
                
                for (int i=0; i<listOfTweetHashtags.length; i++)
                {
                    if ( hashtagSet.contains(listOfTweetHashtags[i].getText()) ) 
                    {
                        containsValidTag=true;
                        break;
                        //System.out.println(listOfTweetHashtags[i].getText());
                        //System.out.println(status.getText());
                    }
                        
                }
            }

            /**
             * Discard when TWEET doesnt contain valid TAG
             */
            if(containsValidTag==false)
            {
            //    return;
            }
            System.out.println("****** Tuple count = " + tupleCount++);
            collector.emit(new Values(status));
            //System.out.println(status.getText());
        }
    
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) {
      ofd.declare(new Fields("filteredtweets"));
  }

}
