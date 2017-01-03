package com.dongqiudi.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by Joshua on 16/11/15.
 */
public class HBaseClient {

    public static String UTF8 = "UTF-8";

    private Configuration configuration;

    public HBaseClient(String zkHosts, String zkPort, String hMaster) {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort", zkPort);
        configuration.set("hbase.zookeeper.quorum", zkHosts);
        configuration.set("hbase.master", hMaster);
    }

    public void writeStringData(String tableName, String rowKey, String family, String qualifier, String value) throws IOException {
        Connection connection = ConnectionFactory.createConnection(this.configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));
        try {
            Put put = new Put(rowKey.getBytes(UTF8));
            put.addColumn(family.getBytes(UTF8), qualifier.getBytes(UTF8), value.getBytes(UTF8));
            table.put(put);
        } catch (IOException ex) {
            throw ex;
        } finally {
            table.close();
            connection.close();
        }
    }

    public void writeStringDatas(String tableName, List<Put> puts) throws IOException {
        Connection connection = ConnectionFactory.createConnection(this.configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));
        try {
            table.put(puts);
        } catch (IOException ex) {
            throw ex;
        } finally {
            table.close();
            connection.close();
        }
    }

    public long incrementValue(String tableName, String rowKey, String family, String qualifier, long value) throws IOException {
        Connection connection = ConnectionFactory.createConnection(this.configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));
        try {
            return table.incrementColumnValue(rowKey.getBytes(UTF8), family.getBytes(UTF8), qualifier.getBytes(UTF8), value);
        } catch (IOException ex) {
            throw ex;
        } finally {
            table.close();
            connection.close();
        }
    }


    public String readStringData(String tableName, String rowKey, String family, String qualifier) throws IOException {
        Connection connection = ConnectionFactory.createConnection(this.configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));
        try {
            Get get = new Get(rowKey.getBytes(UTF8));
            Result result = table.get(get);
            return new String(result.getValue(family.getBytes(UTF8), qualifier.getBytes(UTF8)), UTF8);
        } catch (IOException ex) {
            throw ex;
        } finally {
            connection.close();
            table.close();
        }
    }

    public Map<String, Object> readColumFamily(String tableName, String rowKey, String family) throws IOException {
        Connection connection = ConnectionFactory.createConnection(this.configuration);
        Table table = connection.getTable(TableName.valueOf(tableName));
        Map<String, Object> ret = new HashMap<String, Object>();
        try {
            Get get = new Get(rowKey.getBytes(UTF8));
            Result result = table.get(get);
            Map map = result.getFamilyMap(family.getBytes(UTF8));
            Iterator iter = map.keySet().iterator();
            while (iter.hasNext()) {
                byte[] keyBytes = (byte[]) iter.next();
                String key = new String(keyBytes, UTF8);
                String value = new String((byte[]) map.get(keyBytes), UTF8);
                ret.put(key, value);
            }
            return ret;
        } catch (IOException ex) {
            throw ex;
        } finally {
            connection.close();
            table.close();
        }
    }

}
