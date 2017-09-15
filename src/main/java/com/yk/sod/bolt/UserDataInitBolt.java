package com.yk.sod.bolt;

import backtype.storm.Constants;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.yk.sod.entity.MetaData;
import com.yk.sod.hbase.HbaseOperation;
import com.yk.sod.hbase.impl.HbaseOperationImpl;
import net.sf.json.JSONObject;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by dengtianjia on 2017/9/7.
 */
public class UserDataInitBolt extends BaseBasicBolt {
    private List<MetaData> dataArr ;
    private HbaseOperation hbaseOperation;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        dataArr = new ArrayList<>();
        hbaseOperation = new HbaseOperationImpl();
        List<String> colums = new ArrayList<>();
        colums.add("name");colums.add("type");colums.add("price");
        hbaseOperation.createTable("userMetaData", colums);
        super.prepare(stormConf, context);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        if(tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)){
            if(dataArr!=null){
                for(MetaData md : dataArr){
                    hbaseOperation.insertData("userMetaData",md);
                }
                dataArr = new ArrayList<>();
            }

        }else {
            String msg = tuple.getString(0);
            JSONObject obj = JSONObject.fromObject(msg);
            MetaData metaData = new MetaData(
                    obj.getString("userId"),obj.getString("name"),
                    obj.getString("type"),obj.getString("price"));
            String userId = obj.getString("userId");
            List<String> data = new ArrayList<>();
            data.add((String) obj.get("name"));
            data.add((String) obj.get("type"));
            data.add(""+obj.get("price"));
            dataArr.add(metaData);
            collector.emit(new Values(userId, data));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("userId","data"));
    }
}
