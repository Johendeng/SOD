package com.yk.sod.hbase;

import com.yk.sod.entity.MetaData;

import java.util.List;

/**
 * Created by dengtianjia on 2017/9/15.
 */

public interface HbaseOperation {


    void createTable(String tableName, List<String> fieldFamily);

    void insertData(String tableName,MetaData data);

    void getAllData(String tableName);

}
