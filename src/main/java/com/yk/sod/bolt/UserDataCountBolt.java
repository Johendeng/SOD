package com.yk.sod.bolt;

import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.yk.sod.entity.Record;
import com.yk.sod.redis.RedisOperation;
import com.yk.sod.util.SerializeUtil;
import redis.clients.jedis.Jedis;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by dengtianjia on 2017/9/7.
 */
public class UserDataCountBolt  implements IRichBolt {

    private OutputCollector collector;
    private RedisOperation redisOperation;
    private  Jedis jedis;
    private  Map<String,Record> userRecord;


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector  = collector;
        redisOperation = new RedisOperation();
        userRecord = new HashMap<>();
        jedis = redisOperation.getJedis();
    }

    @Override
    public void execute(Tuple input) {
        //定时持久化数据
        if(input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)){
            for(String key : userRecord.keySet()){
                System.out.println("+++++++++++++++++++++++++++user统计获取的数据为++++++++++++++++++++++++++++++++++++");
                System.out.println(key);
                System.out.println(userRecord.get(key).getNameRecord().toString());
                System.out.println(userRecord.get(key).getNameRecord().toString());
                System.out.println(userRecord.get(key).getNameRecord().toString());
                System.out.println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++");
                if (jedis.exists(key)){
                    Record record  = redisOperation.getRecord(key);
                    record.changeRecord(userRecord.get(key));
                    jedis.set(key.getBytes(),SerializeUtil.serialize(record));
                }else {
                    jedis.set(key.getBytes(),SerializeUtil.serialize(userRecord.get(key)));
                }
            }
            userRecord = new HashMap<>();
        }else {
            String userId = input.getStringByField("userId");
            List<String> data = (List<String>) input.getValueByField("data");
            if (userRecord.containsKey(userId)) {
                if (userRecord.get(userId).getNameRecord().containsKey(data.get(0))) {
                    userRecord.get(userId).getNameRecord().put(data.get(0),
                            userRecord.get(userId).getNameRecord().get(data.get(0)) + 1);
                } else {
                    userRecord.get(userId).getNameRecord().put(data.get(0), 1);
                }

                if (userRecord.get(userId).getTypeRecord().containsKey(data.get(1))) {
                    userRecord.get(userId).getTypeRecord().put(data.get(1),
                            userRecord.get(userId).getTypeRecord().get(data.get(1)) + 1);
                } else {
                    userRecord.get(userId).getTypeRecord().put(data.get(1), 1);
                }

                if (userRecord.get(userId).getPriceRecord().containsKey(Double.valueOf(data.get(2)))) {
                    userRecord.get(userId).getPriceRecord().put(Double.valueOf(data.get(2)),
                            userRecord.get(userId).getPriceRecord().get(Double.valueOf(data.get(2))) + 1);
                } else {
                    userRecord.get(userId).getPriceRecord().put(Double.valueOf(data.get(2)), 1);
                }

            } else {
                Record record_temp = new Record();
                Map<String, Integer> nameRecord = new HashMap<>();
                Map<String, Integer> typeRecord = new HashMap<>();
                Map<Double, Integer> priceRecord = new HashMap<>();
                nameRecord.put(data.get(0), 1);
                typeRecord.put(data.get(1), 1);
                priceRecord.put(Double.valueOf(data.get(2)), 1);
                record_temp.setNameRecord(nameRecord);
                record_temp.setTypeRecord(typeRecord);
                record_temp.setPriceRecord(priceRecord);
                userRecord.put(userId, record_temp);
            }
        }

    }


    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
    @Override
    public void cleanup() {
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
