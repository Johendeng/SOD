package com.yk.sod.topology;

import com.yk.sod.bolt.DataTotalCountBolt;
import com.yk.sod.bolt.UserDataCountBolt;
import com.yk.sod.bolt.UserDataInitBolt;
import com.yk.sod.spout.DataSpout;
import com.yk.sod.util.PropertiesUtil;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Map;

/**
 * Created by dengtianjia on 2017/9/7.
 */


public class Topology {

    public static void main(String[] args) {
        PropertiesUtil propertiesUtil = new PropertiesUtil();
        Map props = propertiesUtil.getProperties("zookeeper.properties");
        // zookeeper hosts for the Kafka cluster
        ZkHosts zkHosts = new ZkHosts((String) props.get("zk.hosts"));
        // Create the KafkaSpout configuartion
        // Second argument is the topic name
        // Third argument is the zookeeper root for Kafka
        // Fourth argument is consumer group id
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts,(String) props.get("topic"), "", "id7");
        // Specify that the kafka messages are String
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        // We want to consume all the first messages in the topic everytime
        // we run the topology to help in debugging. In production, this
        // property should be false
//        kafkaConfig.forceFromStart = true;



        //组装topology
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("DataSpout", new DataSpout());
        //.shuffleGrouping("spout1"); 表示让MyBolt接收MySpout发射出来的tuple
        topologyBuilder.setBolt("UserDataInitBolt", new UserDataInitBolt(),1).shuffleGrouping("DataSpout");
        topologyBuilder.setBolt("UserDataCountBolt", new UserDataCountBolt(),2)
                .fieldsGrouping("UserDataInitBolt",new Fields("userId")) ;
        topologyBuilder.setBolt("DataTotalCountBolt", new DataTotalCountBolt(),2).shuffleGrouping("DataSpout");


        //创建本地storm集群
        LocalCluster localCluster = new LocalCluster();
        Config config = new Config();
        //下面这样设置就是一个全局的定时任务  还有局部的定时任务.
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 5);
        localCluster.submitTopology("Topology", config, topologyBuilder.createTopology());

        try {
            // Wait for some time before exiting
            System.out.println("Waiting to consume from kafka");
            Thread.sleep(10000);
        } catch (Exception exception) {
            System.out.println("Thread interrupted exception : " + exception);
        }
    }

}
