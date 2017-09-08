package com.yk.sod.bolt;

import backtype.storm.Constants;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import net.sf.json.JSONObject;


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
        if(tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)){
        }else {
            String msg = tuple.getString(0);
            JSONObject obj = JSONObject.fromObject(msg);
            String userId = obj.getString("userId");
            List<String> data = new ArrayList<>();
            data.add((String) obj.get("name"));
            data.add((String) obj.get("type"));
            data.add(""+obj.get("price"));
            collector.emit(new Values(userId, data));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("userId","data"));
    }
}
