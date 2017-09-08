package com.yk.sod.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.yk.sod.util.PropertiesUtil;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;


import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by dengtianjia on 2017/9/7.
 */


public class DataSpout implements IRichSpout{
    SpoutOutputCollector collector;
    private ConsumerConnector consumer;
    private String topic;

    public DataSpout(String topic) {
        this.topic = topic;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {
        this.consumer = Consumer.createJavaConsumerConnector(createConsumerConfig());
        Map<String, Integer> topickMap = new HashMap<String, Integer>();
        topickMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = consumer.createMessageStreams(topickMap);
        KafkaStream<byte[], byte[]> stream = streamMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            String value = new String(it.next().message());
            System.out.println("数据:"+value);
            collector.emit(new Values(value), value);
        }
    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {

    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("data"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    private static ConsumerConfig createConsumerConfig() {
        Properties prop = new Properties();
        try {
            String path = PropertiesUtil.class.getClassLoader().getResource("kafka.properties").getPath();
            FileInputStream in = new FileInputStream(path);
            prop.load(in);
        }catch (Exception e){
            e.printStackTrace();
        }
        return new ConsumerConfig(prop);
    }

}
