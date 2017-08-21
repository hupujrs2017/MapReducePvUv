package com.yangyz.mapreduce;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by yangyz on 2017/8/21.
 */
public class SetValueMapWrapper<K,V> {
    private Map<K,Set<V>> map;

    public Map<K, Set<V>> getMap() {
        return map;
    }

    public SetValueMapWrapper(Map<K, Set<V>> map) {
        this.map = map;
    }

    public void putForSetValue(K key,V value){
        Set<V> set= map.get(key);
        if(set==null){
            set = new HashSet<V>();
        }
        set.add(value);
        map.put(key,set);
        return;
    }
}
