package com.yk.sod.redis;

import com.yk.sod.entity.Record;
import com.yk.sod.util.PropertiesUtil;
import com.yk.sod.util.SerializeUtil;
import redis.clients.jedis.Jedis;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by dengtianjia on 2017/9/7.
 */
public class RedisOperation {

    private static Jedis jedis;

    public Record getRecord(String keyId){
        if(jedis == null) {
            String path = PropertiesUtil.class.getClassLoader().getResource("jdbc.properties").getPath();
            Properties props = new Properties();
            try {
                FileInputStream in = new FileInputStream(path);
                props.load(in);
                in.close();
                String host = props.getProperty("redis.host");        //地址
                String port = props.getProperty("redis.port");        //端口
                String pass = props.getProperty("redis.pass");        //密码
                jedis = new Jedis(host, Integer.parseInt(port));
                jedis.auth(pass);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        byte[] byteRecord = jedis.get(keyId.getBytes());
        return (Record) SerializeUtil.unserialize(byteRecord);
    }
}
