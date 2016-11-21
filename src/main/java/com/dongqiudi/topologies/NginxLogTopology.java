package com.dongqiudi.topologies;

import com.dongqiudi.Topology;
import com.dongqiudi.bolts.CounterBolt;
import com.dongqiudi.bolts.ParseBolt;
import org.apache.storm.hbase.bolt.HBaseBolt;

import java.util.Properties;

/**
 * Created by Joshua on 16/11/20.
 */
public class NginxLogTopology extends Topology {


    @Override
    protected void initSpoutBolt(Properties properties) {
        kafkaSpout = initKafkaSpout(properties);
        topologyBuilder.setSpout("kafka_spout", kafkaSpout, 3);
        topologyBuilder.setBolt("parse_bolt", new ParseBolt(), 3).shuffleGrouping("kafka_spout");
        topologyBuilder.setBolt("save_bolt", new CounterBolt(), 3).shuffleGrouping("parse_bolt");
    }

    public NginxLogTopology(Properties properties) {
        super(properties);
    }


}
