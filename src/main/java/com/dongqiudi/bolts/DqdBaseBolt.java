package com.dongqiudi.bolts;

import com.dongqiudi.utils.HBaseClient;
import com.dongqiudi.utils.ParseUtil;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.util.Date;
import java.util.Map;

/**
 * Created by Joshua on 16/11/22.
 */
public abstract class DqdBaseBolt extends BaseRichBolt{

    protected OutputCollector collector;

    protected HBaseClient hBaseClient;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        Map<String, String> hbaseConfig = (Map<String, String>) stormConf.get("hbaseConfig");
        String zkHosts = hbaseConfig.get("zk_hosts");
        String zkPort = hbaseConfig.get("zk_port");
        String hMaster = hbaseConfig.get("hmaster");
        hBaseClient = new HBaseClient(zkHosts, zkPort, hMaster);
        this.collector = collector;
    }

}
