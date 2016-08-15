package com.webmovie.bigdata.akka.actors;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import com.webmovie.bigdata.akka.messages.MapData;
import com.webmovie.bigdata.akka.messages.ReduceData;
import com.webmovie.bigdata.akka.messages.WordCount;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yeahwell on 16/8/13.
 */
public class ReduceActor extends UntypedActor {

    private ActorRef aggregateActor = null;

    public ReduceActor(ActorRef aggregateActor){
        this.aggregateActor = aggregateActor;
    }

    @Override
    public void onReceive(Object message) throws Exception {
        if(message instanceof MapData){
            MapData mapData = (MapData) message;
            //reduce the incomming data
            ReduceData reduceData =reduce(mapData.getDataList());
//            aggregateActor.tell(reduceData);
        }
    }

    private ReduceData reduce(List<WordCount> dataList){
        Map<String, Integer> reduceMap = new HashMap<String, Integer>();
        for(WordCount wordCount : dataList){
            if(reduceMap.containsKey(wordCount.getWord())){
                Integer value = (Integer) reduceMap.get(wordCount.getWord());
                value++;
                reduceMap.put(wordCount.getWord(), value);
            }else{
                reduceMap.put(wordCount.getWord(), Integer.valueOf(1));
            }
        }
        return new ReduceData(reduceMap);
    }

}
