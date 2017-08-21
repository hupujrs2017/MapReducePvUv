package com.yangyz.mapreduce;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by yangyz on 2017/8/21.
 */
public class ListValueMapWrapper<K,V> {
    private Map<K,List<V>> map; //此处key为时间 list中存vid

    public Map<K, List<V>> getMap() {
        return map;
    }

    public ListValueMapWrapper(Map<K, List<V>> map) {
        this.map = map;
    }

    public void putForSetValue(K key,V value){  //此处key为时间 value为vid
        List<V> list= map.get(key);
        if(list==null){
            list = new ArrayList<V>();
        }
        list.add(value);
        map.put(key,list);
        return;
    }
}
