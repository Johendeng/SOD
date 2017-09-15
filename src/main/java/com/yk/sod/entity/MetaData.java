package com.yk.sod.entity;

import java.io.Serializable;

/**
 * Created by dengtianjia on 2017/9/15.
 */
public class MetaData implements Serializable {

    private String userId;
    private String name;
    private String type;
    private String price;

    public MetaData(){};

    public MetaData(String userId, String name, String type, String price) {
        this.userId = userId;
        this.name = name;
        this.type = type;
        this.price = price;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }
}
