package com.webmovie.bigdata.akka.actors;

import akka.actor.UntypedActor;
import com.webmovie.bigdata.akka.messages.ReduceData;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by yeahwell on 16/8/13.
 */
public class AggregateActor extends UntypedActor{

    private Map<String, Integer> finalReduceMap = new HashMap<String, Integer>();

    @Override
    public void onReceive(Object message) throws Exception {
        if (message instanceof ReduceData){
            ReduceData reduceData = (ReduceData) message;
            aggregateInMemoryReduce(reduceData.getReduceDataList());
        }else if(message instanceof String){
            System.out.println(finalReduceMap.toString());
        }else{
            unhandled(message);
        }

    }

    private void aggregateInMemoryReduce(Map<String, Integer> reducedList){
        Integer count = null;
        for(String key : reducedList.keySet()) {
            if(finalReduceMap.containsKey(key)){
                count = reducedList.get(key) + finalReduceMap.get(key);
                finalReduceMap.put(key, count);
            }else{
                finalReduceMap.put(key, reducedList.get(key));
            }

        }
    }
}
