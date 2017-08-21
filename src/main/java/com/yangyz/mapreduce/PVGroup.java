package com.yangyz.mapreduce;

import java.io.Serializable;
import java.util.*;

/**
 * Created by yangyz on 2017/8/21.
 */
public class PVGroup implements Serializable {
    private static final ResourceBundle resourceBundle;

    private static final Map<String, Set<String>> PageIdByProduct = new HashMap<String,Set<String>>();
    private static final String ProdPageIdDelimiter = ":";
    private static final String PageIdDelimiter = ",";



    static {
        resourceBundle = ResourceBundle.getBundle("pvgroup");
        init();


    }

    private static void init() {
        SetValueMapWrapper<String,String> setValueMapWrapper = new SetValueMapWrapper<String,String>(PageIdByProduct);
        for (String k : resourceBundle.keySet()) {
            String v= resourceBundle.getString(k);
            String[] kv = v.split(ProdPageIdDelimiter);
            String prodId = "";
            try {
                prodId = kv[0].trim();
            } catch (Exception e) {
                continue;
            }
            for (String pageId : kv[1].split(PageIdDelimiter)) {
                try {
                    String id = pageId.trim();
                    setValueMapWrapper.putForSetValue(id,prodId);
                } catch (Exception e) {
                    continue;
                }
            }

        }
    }


    public static final Set<String> getProductTypeByPageId(String pageId) {
        Set<String> result = PageIdByProduct.get(pageId);
        if(result==null){
            return Collections.EMPTY_SET;
        }
        return result;
    }
    public static void main(String[] args)
    {
        System.out.print(getProductTypeByPageId("103062"));
    }
}
