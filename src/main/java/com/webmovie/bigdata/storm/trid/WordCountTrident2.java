package com.webmovie.bigdata.storm.trid;

import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordCountTrident2 {

	private static final String FIXED_BATCH_SPOUT_ID = "fixedBatchSpoutId";
	
	public static void main(String[] args) {
		
		//构造Topology
		TridentTopology topology = new TridentTopology();
		
		@SuppressWarnings("unchecked")
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("str", "java"), 5
				, new Values("hadoop yarn storm", "servlet jsp ")
		, new Values("hadoop spout bolt flume", "struts2 springmvc")
		, new Values("hadoop spout trident clojure", "mybatis hibernate")
		);
		spout.setCycle(true);
		
		//构造DAG tream，指定数据采集器
		Stream stream = topology.newStream(FIXED_BATCH_SPOUT_ID, spout);
		
		TridentState state = null;
		LocalDRPC localDRPC = new LocalDRPC();
		topology.newDRPCStream("drpcService", localDRPC)
		.each(new Fields("args"), new SplitFunction(), new Fields("word"))
		.stateQuery(state, new Fields("word"), new MapGet(), new Fields("count"))
		;
		
		
		Config conf = new Config();
		
		if(args == null || args.length <= 0){
			//本地测试
			LocalCluster localCluster= new LocalCluster();
			localCluster.submitTopology("wordcountTrident", conf, topology.build());
			
			while(true){
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				String jsonResult = localDRPC.execute("drpcService", "hadoop mapreduce storm");
				System.out.println(jsonResult);
			}
			
		}else{
			try {
				StormSubmitter.submitTopology(args[0], conf, topology.build());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			}
		}
		
		
	}
	
}
