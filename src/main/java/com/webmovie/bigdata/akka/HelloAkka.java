package com.webmovie.bigdata.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.webmovie.bigdata.akka.actors.MasterActor;
import com.webmovie.bigdata.akka.messages.Result;

/**
 * Created by yeahwell on 16/8/13.
 */
public class HelloAkka {

    public static void main(String[] args){
        System.out.println("开始akka示例");
        ActorSystem system = ActorSystem.create("HelloAkka");
//        ActorRef master = system.actorOf(new Props(MasterActor.class), "master");

//        master.tell("Hi My name is Rocky. I'm so so sos happy to be here");
//        master.tell("Today, I'm going to read a new article for you");
//        master.tell("I hope you'll like it");
//
//        Thread.sleep(500);
//
//        master.tell(new Result());
//
//        Thread.sleep(500);

        system.shutdown();

    }

}
