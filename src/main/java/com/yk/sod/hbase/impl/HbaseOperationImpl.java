package com.yk.sod.hbase.impl;

import com.yk.sod.entity.MetaData;
import com.yk.sod.hbase.HbaseOperation;
import com.yk.sod.util.PropertiesUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.List;

/**
 * Created by dengtianjia on 2017/9/15.
 */
public class HbaseOperationImpl implements HbaseOperation {
    private static Configuration configuration;
    private static Connection connection;

    static {
        String path = PropertiesUtil.class.getClassLoader().getResource("hbase-site.xml").getPath();
        System.setProperty("hadoop.home.dir", "E:\\hadoop-2.7.4");
        configuration = HBaseConfiguration.create();
        configuration.addResource(path);
        try {
            connection = ConnectionFactory.createConnection(configuration);
        }catch (Exception e){e.printStackTrace();}
    }

    @Override
    public void createTable(String tableName, List<String> fieldFamily) {
        try {
            HBaseAdmin hBaseAdmin = new HBaseAdmin(configuration);
            if(hBaseAdmin.tableExists(tableName)){
                System.out.println("数据库中已经存在该数据表");
                return;
            }else {
                HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
                for(String s : fieldFamily){
                    tableDescriptor.addFamily(new HColumnDescriptor(s));
                }
                hBaseAdmin.createTable(tableDescriptor);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    @Override
    public void insertData(String tableName, MetaData data) {
        TableName tn = TableName.valueOf(tableName);
        try{
            HTable hTable = (HTable) connection.getTable(tn);
            Put put = new Put(data.getUserId().getBytes());
            put.addColumn("name".getBytes(),null,data.getName().getBytes());
            put.addColumn("type".getBytes(),null,data.getType().getBytes());
            put.addColumn("price".getBytes(),null,data.getPrice().getBytes());
            hTable.put(put);
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    @Override
    public void getAllData(String tableName) {
        TableName tn = TableName.valueOf(tableName);
        try {
            HTable hTable = (HTable) connection.getTable(tn);
            ResultScanner rs = hTable.getScanner(new Scan());
            for (Result r : rs) {
                System.out.println("rowkey: " + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println("列：" + new String(keyValue.getFamily())
                            + "====值:" + new String(keyValue.getValue()));
                }
            }
            System.out.println("over");
        } catch (IOException e) {
            e.printStackTrace();
        }



    }
}
