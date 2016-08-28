package com.webmovie.bigdata.storm.trid;

import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class WordCountTrident {

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
		
		stream
		
		//指定tuple中哪个key value对进行操作
//		.each(new Fields("str"), new HasFlumeFilter())
		
		//.each(new Fields("str"), new PrintTestFilter())
		
		//对tuple中key名称为str的keyvalue对的value之进行splitfunciton操作，产生新的keyvalue对
		.each(new Fields("str"), new SplitFunction(), new Fields("word"))
		
		//设置2个executor来执行split function操作
		.parallelismHint(2)
		
		//.each(new Fields("str", "java", "word"), new PrintTestFilter())
		
		//指定只保留key名称为word的keyvalue对
		.project(new Fields("word"))
//		.partitionBy(new Fields("word"))
		.groupBy(new Fields("word"))
		
		
		//构造聚合链
		.chainedAgg()
		.partitionAggregate(new Fields("word"), new CountAggregator(), new Fields("count"))
		.chainEnd()
		
		//.each(new Fields("str", "java", "word"), new PrintTestFilter())
		
		.each(new Fields("word"), new CountFunction(), new Fields("count"))
		.toStream()
		.parallelismHint(3)
		
		.each(new Fields("word", "count"), new PrintTestFilter())
		
		.groupBy(new Fields("word"))
		.persistentAggregate(new MemoryMapState.Factory(), new Fields("count"),
				new Sum(),
				new Fields("globalCount"))
		.newValuesStream()
		.each(new Fields("word", "globalCount"), new PrintTestFilter())
		;
		
		Config conf = new Config();
		
		if(args == null || args.length <= 0){
			//本地测试
			LocalCluster localCluster= new LocalCluster();
			localCluster.submitTopology("wordcountTrident", conf, topology.build());
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
