package com.yk.sod.spout;

import com.yk.sod.util.PropertiesUtil;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by dengtianjia on 2017/9/7.
 */


public class DataSpout extends BaseRichSpout{
    private SpoutOutputCollector collector;
    private ConsumerConnector consumerConnector;
    private String topic;
    private PropertiesUtil propertiesUtil;

    public DataSpout(){
        Map props = propertiesUtil.getProperties("kafka.properties");
        this.topic = (String) props.get("topic");
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

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector controller) {
        this.collector = controller;
        propertiesUtil = new PropertiesUtil();
    }

    @Override
    public void nextTuple() {
        this.consumerConnector = Consumer.createJavaConsumerConnector(createConsumerConfig());
        Map<String , Integer> topicMap = new HashMap<>();
        topicMap.put(topic,new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = consumerConnector.createMessageStreams(topicMap);
        KafkaStream<byte[], byte[]> stream = streamMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()){
            String value = new String(it.next().message());
            collector.emit(new Values(value),value);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("data"));
    }
}
