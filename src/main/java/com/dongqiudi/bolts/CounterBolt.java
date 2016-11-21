package com.dongqiudi.bolts;

import com.dongqiudi.utils.DateTimeUtil;
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
public class CounterBolt extends DqdBaseBolt {



    public void execute(Tuple input) {
        String uri = input.getString(0);
        String token = input.getString(1);
        String articleId = ParseUtil.getInstance().parseUri(uri, ParseUtil.ARTICLE);
        if(StringUtils.isNotBlank(articleId)) {
            try {
                if (StringUtils.equals("-", token)) {
                    hBaseClient.incrementValue("test_article_pv", articleId, "d_g", "11-22", 1L);
                } else {
                    hBaseClient.incrementValue("test_article_pv", articleId, "d_u", "11-22", 1L);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        collector.ack(input);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
