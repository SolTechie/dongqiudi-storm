package com.dongqiudi.bolts;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

/**
 * Created by Joshua on 16/11/16.
 */
public class ArticleClassBolt extends DqdBaseBolt {

    public void execute(Tuple input) {
        String rawLine = input.getString(0);
        try {
            String[] strArr = StringUtils.split(rawLine, ",");
            String aId = strArr[0];
            String cId = strArr[1];
            if (StringUtils.isNumeric(aId) && StringUtils.isNumeric(cId)) {
                hBaseClient.writeStringData("article_class_channel", aId, "classes", cId, "1");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            collector.emit(new Values(rawLine));
            collector.ack(input);
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("rawLine"));
    }
}
