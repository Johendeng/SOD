package com.yk.sod.bolt;

import net.sf.json.JSONObject;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by dengtianjia on 2017/9/7.
 */
public class UserDataInitBolt extends BaseBasicBolt {

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String msg = tuple.getString(0);
        JSONObject obj = JSONObject.fromObject(msg);
        String userId = obj.getString("userId");
        List<String> data = new ArrayList<>();
        data.add((String) obj.get("name"));
        data.add((String) obj.get("type"));
        data.add((String) obj.get("price"));
        collector.emit(new Values(userId,data));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("userId","data"));
    }
}
