package com.webmovie.bigdata.akka.actors;

import akka.actor.ActorRef;
import akka.actor.UntypedActor;
import com.webmovie.bigdata.akka.messages.MapData;
import com.webmovie.bigdata.akka.messages.WordCount;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by yeahwell on 16/8/13.
 */
public class MapActor extends UntypedActor {

    private ActorRef reduceActor = null;
    private String[] STOP_WORDS = {"a", "is"};
    private List<String> STOP_WORDS_LIST = Arrays.asList(STOP_WORDS);

    public MapActor(ActorRef reduceActor){
        this.reduceActor = reduceActor;
    }

    @Override
    public void onReceive(Object message) throws Exception {

        if(message instanceof String){
            String word = (String) message;
            MapData data = evaluateExpression(word);
//            reduceActor.tell(data);
        }else{
            unhandled(message);
        }

    }

    private MapData evaluateExpression(String line){
        List<WordCount> dataList = new ArrayList<WordCount>();
        StringTokenizer parser = new StringTokenizer(line);
        while(parser.hasMoreTokens()){
            String word = parser.nextToken().toLowerCase();
            if(!STOP_WORDS_LIST.contains(word)){
                dataList.add(new WordCount(word, Integer.valueOf(1)));
            }
        }
        return new MapData(dataList);
    }
}
