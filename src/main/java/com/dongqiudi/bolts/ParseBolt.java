package com.dongqiudi.bolts;

import com.dongqiudi.utils.ParseUtil;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

/**
 * Created by Joshua on 16/11/16.
 */
public class ParseBolt extends DqdBaseBolt {


    public void execute(Tuple input) {
        String rawLine = input.getString(0);
        try {
            ParseUtil parseUtil = ParseUtil.getInstance();
            Map<String, String> parseRes = parseUtil.parseNgxAccLog(rawLine);
            String uri = parseRes.get("uri");
            String dateTime = parseRes.get("@timestamp");
            String clientIp = parseRes.get("remote_addr");
            String scheme = parseRes.get("scheme");
            String host = parseRes.get("host");
            String method = parseRes.get("method");
            String protocol = parseRes.get("protocol");
            String status = String.valueOf(parseRes.get("status"));
            String reqSize = String.valueOf(parseRes.get("size"));
            String requestTime = String.valueOf(parseRes.get("request_time"));
            String upstreamTime = String.valueOf(parseRes.get("upstream_time"));
            String referer = parseRes.get("referer");
            String agent = parseRes.get("agent");
            String uuid = parseRes.get("uuid");
            String authorization = parseRes.get("authorization");
            hBaseClient.incrementValue("test_processed", "2016", "parsed", "11-22", 1L);
            collector.emit(new Values(uri, authorization));
        } catch (Exception e) {
            e.printStackTrace();
            try {
                hBaseClient.incrementValue("test_processed", "2016", "unparsed", "11-22", 1L);
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
            long ts = new Date().getTime();
            String rowkey = MD5Hash.getMD5AsHex(rawLine.getBytes()) + "_" + ts;
            try {
                hBaseClient.writeStringData("test_un_parse", rowkey, "content", "1", rawLine);
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        } finally {
            collector.ack(input);
        }

    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("uri", "token"));
    }
}
