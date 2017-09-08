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
import net.sf.json.JSONObject;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * Created by dengtianjia on 2017/9/7.
 */
public class DataTotalCountBolt  implements IRichBolt {
    private Jedis jedis;
    private Record record;
    private RedisOperation redis;


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        record = new Record();
        redis = new RedisOperation();
        jedis = redis.getJedis();
    }

    @Override
    public void execute(Tuple tuple) {
//定时持久化数据
        if(tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)){
            if (jedis.exists("totalRecord")){
                Record record_temp  = redis.getRecord("totalRecord");
                record_temp.changeRecord(record);
                jedis.set("totalRecord".getBytes(),SerializeUtil.serialize(record_temp));
            }else {
                jedis.set("totalRecord".getBytes(),SerializeUtil.serialize(record));
            }
            record = new Record();
            System.out.println("进行了总统计持久化=========");
        }else {
            System.out.println("-------------------" + tuple);
            String data = tuple.getString(0);
            JSONObject json = JSONObject.fromObject(data);
            String name = json.getString("name");
            String type = json.getString("type");
            Double price = json.getDouble("price");
            if (record.getNameRecord().containsKey(name)) {
                record.getNameRecord().put(name, record.getNameRecord().get(name) + 1);
            } else {
                record.getNameRecord().put(name, 1);
            }

            if (record.getTypeRecord().containsKey(type)) {
                record.getTypeRecord().put(type, record.getTypeRecord().get(type) + 1);
            } else {
                record.getTypeRecord().put(type, 1);
            }

            if (record.getPriceRecord().containsKey(Double.valueOf(price))) {
                record.getPriceRecord().put(Double.valueOf(price),
                        record.getPriceRecord().get(Double.valueOf(price)) + 1);
            } else {
                record.getPriceRecord().put(Double.valueOf(price), 1);
            }
        }

    }


    @Override
    public void cleanup() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
