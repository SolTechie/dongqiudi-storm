package com.dongqiudi.topologies;

import com.dongqiudi.Topology;
import com.dongqiudi.bolts.ArticleChannelBolt;

import java.util.Properties;

/**
 * Created by Joshua on 16/11/20.
 */
public class ArticleChannelTopology extends Topology {

    @Override
    protected void initSpoutBolt(Properties properties) {
        kafkaSpout = initKafkaSpout(properties);
        hdfsBolt = initHdfsBolt(properties);
        topologyBuilder.setSpout("kafka_spout", kafkaSpout, 3);
        topologyBuilder.setBolt("article_channel",new ArticleChannelBolt(),3).shuffleGrouping("kafka_spout");
        topologyBuilder.setBolt("save_bolt",hdfsBolt).shuffleGrouping("article_channel");
    }

    public ArticleChannelTopology(Properties properties) {
        super(properties);
    }

}
