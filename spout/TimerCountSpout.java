package storm.starter.spout;

import java.util.Map;
import java.util.Random;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TimerCountSpout extends BaseRichSpout {
    SpoutOutputCollector _collector;
    Integer timeout = 1;

    public TimerCountSpout(int timeout)
    {
        this.timeout = timeout;
    }

    @Override
      public void nextTuple() {
        _collector.emit(new Values(timeout));
        Utils.sleep(30000);
        timeout++;
        
      }
    

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("currTimeInterval"));
        
    }
    
    @Override
      public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
      }
}
