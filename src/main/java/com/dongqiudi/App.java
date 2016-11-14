package com.dongqiudi;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.*;
import org.apache.storm.shade.org.jboss.netty.channel.local.LocalAddress;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Hello world!
 */
public class App {


    public static void main(String[] args) {
        //kafka spout
        BrokerHosts hosts = new ZkHosts("dpf1:2181");
        String topic = "test_kafka_storm";
        String zkRoot = "/storm_zk";
        String taskId = "test_kafka_task";
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, taskId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout spout = new KafkaSpout(spoutConfig);

        //topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("in_spout", spout, 2);
        builder.setBolt("work_bolt", new SimpleExampleBolt(), 2).shuffleGrouping("in_spout");

        Config config = new Config();
        List<String> nimbusSeed = new ArrayList<String>();
        nimbusSeed.add("dpf1");
        config.put(Config.NIMBUS_SEEDS,nimbusSeed);
        try {
            StormSubmitter.submitTopologyWithProgressBar("test_kafka_storm_topology", config, builder.createTopology());
        }catch (Exception ex){
            ex.printStackTrace();
        }
    }
}
