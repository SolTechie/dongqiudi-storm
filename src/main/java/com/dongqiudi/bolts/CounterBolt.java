package com.dongqiudi.bolts;

import com.dongqiudi.utils.HBaseClient;
import com.dongqiudi.utils.ParseUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.Map;

/**
 * Created by Joshua on 16/11/16.
 */
public class CounterBolt extends BaseRichBolt {

    private HBaseClient hBaseClient;
    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        hBaseClient = (HBaseClient) stormConf.get("hbaseClient");
        this.collector = collector;
    }

    public void execute(Tuple input) {
        String uri = input.getString(0);
        String token = input.getString(1);
        String commentAid = ParseUtil.getInstance().parseUri(uri, ParseUtil.COMMENT);
        try {
            if (StringUtils.equals("-", token)) {
                hBaseClient.incrementValue("test_article_pv", commentAid, "d_g", "11-11", 1L);
            } else {
                hBaseClient.incrementValue("test_article_pv", commentAid, "d_u", "11-11", 1L);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
