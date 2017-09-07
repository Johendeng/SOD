package com.yk.sod.util;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

/**
 * Created by JohenTeng on 2017/7/12.
 */
public class PropertiesUtil {

    public Map getProperties(String propertiesName){
        Properties prop = new Properties();
        Map<String,String> propertiesItems = new HashMap<>();
        try {
            String path = PropertiesUtil.class.getClassLoader().getResource(propertiesName).getPath();
            FileInputStream in = new FileInputStream(path);
            prop.load(in);
            Iterator<String> it = prop.stringPropertyNames().iterator();
            while (it.hasNext()){
                String key = it.next();
                String value = prop.getProperty(key);
                propertiesItems.put(key,value);
            }
            in.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return propertiesItems;
    }



}
