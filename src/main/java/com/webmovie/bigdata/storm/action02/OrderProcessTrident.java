package com.webmovie.bigdata.storm.action02;

import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;

import com.webmovie.bigdata.storm.trid.PrintTestFilter;
import com.webmovie.bigdata.storm.trid.SplitFunction;

/**
 * 订单处理入口
 * @author yeahwell
 *
 */
public class OrderProcessTrident {
	
	private static final String KAFKA_SPOUT_ID = "kafkaSpout";

	public static void main(String[] args) {
		
		TridentTopology topology = new TridentTopology();
		
		BrokerHosts hosts = new ZkHosts("beifeng-hadoop-02:2181");
		String topic = "test";
		TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(hosts, topic);
		kafkaConfig.forceFromStart = false;
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		
//		TransactionalTridentKafkaSpout kafkaSpout = new TransactionalTridentKafkaSpout(kafkaConfig);
		OpaqueTridentKafkaSpout kafkaSpout = new OpaqueTridentKafkaSpout(kafkaConfig);
		//tuple {"str", "value"}
		
		Stream hasParseStream = 
		topology.newStream(KAFKA_SPOUT_ID, kafkaSpout)
//		.each(new Fields("str"), new PrintTestFilter())
		.each(new Fields("str"), new OrderParseFunction(), new Fields(
				"timestamp", "yyyyMMddStr", "yyyyMMddHHStr", "yyyyMMddHHmmStr",
				"consumer", "productName", "price", "country", "province", "city"
				))
		.each(new Fields("timestamp", "yyyyMMddStr", "yyyyMMddHHStr", "yyyyMMddHHmmStr",
				"consumer", "productName", "price", "country", "province", "city"), new PrintTestFilter())
		;
		
		//1. 每天电商网站总销售额
		TridentState globalState = 
		//去掉用不到的keyvalue
		hasParseStream.project(new Fields("yyyyMMddStr", "price"))
		
		.shuffle()
		.groupBy(new Fields("yyyyMMddStr"))
		.chainedAgg()
		.partitionAggregate(new Fields("price"), new SaleSumAggregator(), new Fields("saleTotalAmtOfPartDay"))
		.chainEnd()
		.parallelismHint(3)
		
//		.toStream()
//		.each(new Fields("yyyyMMddStr", "saleTotalAmtOfPartDay"), new PrintTestFilter())
		
		//全局统计
		.groupBy(new Fields("yyyyMMddStr"))
		.persistentAggregate(new MemoryMapState.Factory(), 
				new Fields("saleTotalAmtOfPartDay"), new Sum(),
				new Fields("saleGlobalAmtOfDay"))
		;
		
//		globalState.newValuesStream()
//		.each(new Fields("yyyyMMddStr", "saleGlobalAmtOfDay"), new PrintTestFilter())
		;
		
		//构造一个本地drpc服务
		LocalDRPC localDRPC = new LocalDRPC();
		topology.newDRPCStream("saleAmtOfDayDRPC", localDRPC)
		.each(new Fields("args"), new DRPCFunction(), new Fields("requestDate"))
		.stateQuery(globalState, new Fields("requestDate"), new MapGet(), new Fields("saleGlobalAmtOfDay1"))
		;
		
		
		Config conf = new Config();
		
		if(null == args || args.length <= 0){
			LocalCluster localCluster = new LocalCluster();
			//topoloyg名称保证唯一
			localCluster.submitTopology("orderProcessTrident", conf, topology.build());
			
			while(true){
				try {
					Thread.sleep(6000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				String jsonResult = localDRPC.execute("saleAmtOfDayDRPC", "20160828 20160827");
				System.err.println(jsonResult);			
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
