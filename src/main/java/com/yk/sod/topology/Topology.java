package com.yk.sod.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.yk.sod.bolt.DataTotalCountBolt;
import com.yk.sod.bolt.UserDataCountBolt;
import com.yk.sod.bolt.UserDataInitBolt;
import com.yk.sod.spout.DataSpout;
import com.yk.sod.util.PropertiesUtil;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;


import java.util.Map;

/**
 * Created by dengtianjia on 2017/9/7.
 */


public class Topology {
    public static void main(String[] args) {
        PropertiesUtil propertiesUtil = new PropertiesUtil();
        Map props = propertiesUtil.getProperties("zookeeper.properties");
        ZkHosts zkHosts = new ZkHosts((String) props.get("zk.hosts"));
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts,(String) props.get("topic"), "", "id1");
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.forceFromStart = true;
        //组装topology
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("DataSpout", new DataSpout((String) props.get("topic")));
        //.shuffleGrouping("spout1"); 表示让MyBolt接收MySpout发射出来的tuple
        topologyBuilder.setBolt("UserDataInitBolt", new UserDataInitBolt(),1).shuffleGrouping("DataSpout");
        topologyBuilder.setBolt("UserDataCountBolt", new UserDataCountBolt(),2)
                .fieldsGrouping("UserDataInitBolt",new Fields("userId")) ;
        topologyBuilder.setBolt("DataTotalCountBolt", new DataTotalCountBolt(),1).shuffleGrouping("DataSpout");
        //创建本地storm集群
        LocalCluster localCluster = new LocalCluster();
        Config config = new Config();
        //下面这样设置就是一个全局的定时任务  还有局部的定时任务.
        config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 10);
        localCluster.submitTopology("Topology", config, topologyBuilder.createTopology());
        try {
            // Wait for some time before exiting
            Thread.sleep(10000);
        } catch (Exception exception) {
            System.out.println("Thread interrupted exception : " + exception);
        }
    }

}
