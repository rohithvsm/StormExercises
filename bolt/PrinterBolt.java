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
import twitter4j.Status;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;


public class PrinterBolt extends BaseBasicBolt {
  static long tupleCount = 0;
  @Override
  public void execute(Tuple tuple, BasicOutputCollector collector) 
  {
      
    Writer writer = null;
    System.out.println("****** Tuple count = " + tupleCount++);
    // System.out.println(tuple);
    
        if ( tuple.getSourceComponent().contentEquals("twitter") ) 
        {
          Status status = (Status) tuple.getValueByField("tweet");
          
          String tweet = status.getText();

            // Lets print the tweets structure to file.
            try 
            {
                writer = new BufferedWriter(new OutputStreamWriter(
                      new FileOutputStream("TwitterTweetTextDump.txt", true), "utf-8"));
                writer.write(TweetToWordSplitter.cleanUpTweet(tweet) + "\n");
                writer.close();
            } catch (IOException ex) 
            {
              // report
            }
        }
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer ofd) 
  {
  }

}
