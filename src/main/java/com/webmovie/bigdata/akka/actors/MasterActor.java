package com.webmovie.bigdata.akka.actors;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.actor.UntypedActorFactory;
import com.webmovie.bigdata.akka.messages.Result;

/**
 * Created by yeahwell on 16/8/13.
 */
public class MasterActor extends UntypedActor {

  /*  private ActorRef aggregateActor = getContext().actorOf(
            new Props(AggregateActor.class), "aggregate");

    private ActorRef reduceActor = getContext().actorOf(
            new Props(new UntypedActorFactory() {
                public UntypedActor create() {
                    return new ReduceActor(aggregateActor);
                }
            }),
            "reduce");

    private ActorRef mapActor = getContext().actorOf(new Props(new UntypedActorFactory() {
        public UntypedActor create() {
            return new MapActor(reduceActor);
        }
    }), "map");*/

    @Override
    public void onReceive(Object message) throws Exception {
        /*if (message instanceof String) {
            mapActor.tell(message, mapActor);
        } else if (message instanceof Result) {
            aggregateActor.tell(message, aggregateActor);
        } else {
            unhandled(message);
        }*/
    }
}
