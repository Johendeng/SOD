package com.yk.sod.bolt;

import com.yk.sod.entity.Record;
import com.yk.sod.redis.RedisOperation;
import com.yk.sod.util.SerializeUtil;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
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
        Jedis jedis = redisOperation.jedis;
        String userId = input.getStringByField("userId");
        List<String> data = (List<String>) input.getValueByField("data");
        //定时持久化数据
        if (userRecord.containsKey(userId)){
            if (userRecord.get(userId).getNameRecord().containsKey(data.get(0))){
                userRecord.get(userId).getNameRecord().put(data.get(0),
                        userRecord.get(userId).getNameRecord().get(data.get(0))+1);
            }else {
                userRecord.get(userId).getNameRecord().put(data.get(0),1);
            }

            if (userRecord.get(userId).getTypeRecord().containsKey(data.get(1))){
                userRecord.get(userId).getTypeRecord().put(data.get(1),
                        userRecord.get(userId).getTypeRecord().get(data.get(1))+1);
            }else {
                userRecord.get(userId).getTypeRecord().put(data.get(1),1);
            }

            if (userRecord.get(userId).getPriceRecord().containsKey(Double.valueOf(data.get(2)))){
                userRecord.get(userId).getPriceRecord().put(Double.valueOf(data.get(2)),
                        userRecord.get(userId).getPriceRecord().get(Double.valueOf(data.get(2)))+1);
            }else {
                userRecord.get(userId).getPriceRecord().put(Double.valueOf(data.get(2)),1);
            }

        }else {
            Record record_temp = new Record();
            Map<String,Integer> nameRecord = new HashMap<>();
            Map<String,Integer> typeRecord = new HashMap<>();
            Map<Double,Integer> priceRecord = new HashMap<>();
            nameRecord.put(data.get(0),1);
            typeRecord.put(data.get(1),1);
            priceRecord.put(Double.valueOf(data.get(2)),1);
            record_temp.setNameRecord(nameRecord);
            record_temp.setTypeRecord(typeRecord);
            record_temp.setPriceRecord(priceRecord);
            userRecord.put(userId,record_temp);
        }
        if(input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)){
            for(String key : userRecord.keySet()){
                if (jedis.exists(key)){
                    Record record  = redisOperation.getRecord(key);
                    record.changeRecord(userRecord.get(key));
                    jedis.set(key.getBytes(),serializeUtil.serialize(record));
                }else {
                    jedis.set(key.getBytes(),serializeUtil.serialize(userRecord.get(key)));
                }
            }
        }

    }

    //*************************************************************//
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
