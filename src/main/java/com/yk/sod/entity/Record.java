package com.yk.sod.entity;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by JohenTeng on 2017/8/30.
 */
public class Record implements Serializable {
    private Map<String,Integer> nameRecord ;
    private Map<String,Integer> typeRecord ;
    private Map<Double,Integer> priceRecord;

    public Record(){
        nameRecord = new HashMap<>();
        typeRecord = new HashMap<>();
        priceRecord = new HashMap<>();
    }

    public Map<String, Integer> getNameRecord() {
        return nameRecord;
    }

    public void setNameRecord(Map<String, Integer> nameRecord) {
        this.nameRecord = nameRecord;
    }

    public Map<String, Integer> getTypeRecord() {
        return typeRecord;
    }

    public void setTypeRecord(Map<String, Integer> typeRecord) {
        this.typeRecord = typeRecord;
    }

    public Map<Double, Integer> getPriceRecord() {
        return priceRecord;
    }
    public void setPriceRecord(Map<Double, Integer> priceRecord) {
        this.priceRecord = priceRecord;
    }

    public void changeRecord(Record outRecord){
        Map<String,Integer> outNameRecord = outRecord.getNameRecord();
        Map<String,Integer> outTypeRecord = outRecord.getTypeRecord();
        Map<Double,Integer> outPriceRecord = outRecord.getPriceRecord();
        for(String key : outNameRecord.keySet()){
            if(nameRecord.containsKey(key)){
                nameRecord.put(key,nameRecord.get(key)+outNameRecord.get(key));
            }else {
                nameRecord.put(key,outNameRecord.get(key));
            }
        }
        for(String key : outTypeRecord.keySet()){
            if(typeRecord.containsKey(key)){
                typeRecord.put(key,typeRecord.get(key)+outTypeRecord.get(key));
            }else {
                typeRecord.put(key,outTypeRecord.get(key));
            }
        }
        for(Double key : outPriceRecord.keySet()){
            if(priceRecord.containsKey(key)){
                priceRecord.put(key,priceRecord.get(key)+outPriceRecord.get(key));
            }else {
                priceRecord.put(key,outPriceRecord.get(key));
            }
        }
        outRecord = null ;
    }



}
