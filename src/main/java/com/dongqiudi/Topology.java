package com.dongqiudi;

import com.dongqiudi.utils.HBaseClient;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by Joshua on 16/11/20.
 */
public abstract class Topology {

    protected String name;

    protected TopologyBuilder topologyBuilder;

    protected Config config;

    protected KafkaSpout kafkaSpout;

    protected HBaseClient hBaseClient;

    protected List<String> nimbusSeeds;

    protected int workNum;

    public Topology(Properties properties) {

        name = properties.getProperty("task_name");

        topologyBuilder = new TopologyBuilder();

        config = new Config();

        //set nimbus seed
        nimbusSeeds = new ArrayList<String>();
        for (String nimbus : StringUtils.split(properties.getProperty("nimbus_seeds"), ",")) {
            nimbusSeeds.add(nimbus);
        }
        config.put(Config.NIMBUS_SEEDS, nimbusSeeds);


        //set work numbers
        workNum = Integer.valueOf(properties.getProperty("work_num"));
        config.setNumWorkers(workNum);

        //hbase client
        hBaseClient = initHBaseClient(properties);
        config.put("hbaseClient", hBaseClient);

        initSpoutBolt(properties);
    }

    protected KafkaSpout initKafkaSpout(Properties properties) {
        BrokerHosts hosts = new ZkHosts(properties.getProperty("zk_hosts_port"));
        String kafkaTopic = properties.getProperty("kafka_topic");
        String zkRoot = properties.getProperty("zk_root");
        String taskName = properties.getProperty("task_name");
        SpoutConfig spoutConfig = new SpoutConfig(hosts, kafkaTopic, zkRoot, taskName);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        return new KafkaSpout(spoutConfig);
    }

    protected HBaseClient initHBaseClient(Properties properties) {
        String zkHosts = properties.getProperty("zk_hosts");
        String zkPort = properties.getProperty("zk_port");
        String hMaster = properties.getProperty("hmaster");
        return new HBaseClient(zkHosts, zkPort, hMaster);
    }

    public void submit(String mode) {
        if (StringUtils.equals("local", mode)) {
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology(name, config, topologyBuilder.createTopology());
        }
        if (StringUtils.equals("cluster", mode)) {
            try {
                StormSubmitter.submitTopologyWithProgressBar(name, config, topologyBuilder.createTopology());
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

    protected abstract void initSpoutBolt(Properties properties);


}
