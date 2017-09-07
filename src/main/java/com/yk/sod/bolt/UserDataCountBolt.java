package com.yk.sod.bolt;

import com.yk.sod.entity.Record;
import com.yk.sod.redis.RedisOperation;
import com.yk.sod.util.SerializeUtil;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

/**
 * Created by dengtianjia on 2017/9/7.
 */
public class UserDataCountBolt  implements IRichBolt {

    private OutputCollector collector;
    private SerializeUtil serializeUtil;
    private RedisOperation redisOperation;
    private static Map<String,Record> userRecord;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector  = collector;
        serializeUtil = new SerializeUtil();
        redisOperation = new RedisOperation();
    }

    @Override
    public void execute(Tuple input) {





    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void cleanup() {

    }






}
