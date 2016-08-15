package com.webmovie.bigdata.akka.messages;

import java.util.List;

/**
 * Created by yeahwell on 16/8/14.
 */
public class MapData {

    private List<WordCount> dataList;

    public List<WordCount> getDataList(){
        return dataList;
    }

    public MapData(List<WordCount> dataList){
        this.dataList = dataList;
    }

}
