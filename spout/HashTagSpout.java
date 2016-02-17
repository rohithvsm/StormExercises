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
package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.starter.spout.HashTags;

import java.util.Map;
import java.util.Random;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;


public class HashTagSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;
  Random _rand;
  HashTags _hashTags;
  String[] listOfHashTags;
  Set<String>  _subsetOfHashTags;
  int _countOfTagsReq;
 static int _index;
 
 
 public HashTagSpout(String[] listOfHashTags, int _index) {
        this.listOfHashTags = listOfHashTags;
        this._index = _index;
        _index = 0;
        _hashTags.setMasterListofTags(this.listOfHashTags);
    }

 
  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    // lets get the hash tags to be returned
    _subsetOfHashTags = _hashTags.getSubsetOfHashTags(_countOfTagsReq);
  }

  @Override
  public void nextTuple() 
  {
    Utils.sleep(100);

    _collector.emit(new Values(_subsetOfHashTags));
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("hashtag"));
  }

}