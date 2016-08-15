package com.webmovie.bigdata.akka.messages;

import java.util.Map;

/**
 * Created by yeahwell on 16/8/14.
 */
public class ReduceData {

    private Map<String, Integer> reduceDataList;

    public Map<String, Integer> getReduceDataList() {
        return reduceDataList;
    }

    public ReduceData(Map<String, Integer> reduceDataList) {
        this.reduceDataList = reduceDataList;
    }


}
