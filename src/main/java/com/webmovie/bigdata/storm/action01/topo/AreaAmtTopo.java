package com.webmovie.bigdata.storm.action01.topo;


import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.webmovie.bigdata.storm.action01.bolt.AreaAmtBolt;
import com.webmovie.bigdata.storm.action01.bolt.AreaFilterBolt;
import com.webmovie.bigdata.storm.action01.bolt.AreaRsltBolt;
import com.webmovie.bigdata.storm.action01.kafka.KafkaPropertiesAction01;
import com.webmovie.bigdata.storm.action01.spout.OrderBaseSpout;

public class AreaAmtTopo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new OrderBaseSpout(KafkaPropertiesAction01.Order_topic), 5);
		builder.setBolt("filter", new AreaFilterBolt() , 5).shuffleGrouping("spout") ;
		builder.setBolt("areabolt", new AreaAmtBolt() , 2).fieldsGrouping("filter", new Fields("area_id")) ;
		builder.setBolt("rsltBolt", new AreaRsltBolt(), 1).shuffleGrouping("areabolt");
		
		Config conf = new Config() ;
		conf.setDebug(false);

		if (args.length > 0) {
			try {
				StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		}else {
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology("mytopology", conf, builder.createTopology());
		}
		
		
	}

}

