package com.webmovie.bigdata.storm.action01.trident;

import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.webmovie.bigdata.storm.action01.kafka.KafkaPropertiesAction01;
import com.webmovie.bigdata.storm.action01.trident.tools.OrderSplit;
import com.webmovie.bigdata.storm.action01.trident.tools.Split;
import com.webmovie.bigdata.storm.action01.trident.tools.SplitBy;

public class TridentTopo {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		BrokerHosts zkHosts = new ZkHosts(KafkaPropertiesAction01.zkConnect);
		String topic = "track";
		TridentKafkaConfig config = new TridentKafkaConfig(zkHosts, topic);
		config.forceFromStart = false; //测试时用true，上线时必须改为false
		config.scheme = new SchemeAsMultiScheme(new StringScheme());
		config.fetchSizeBytes = 100 ;//batch size
		
		LocalDRPC drpc = new LocalDRPC() ;
		
		TransactionalTridentKafkaSpout spout  = new TransactionalTridentKafkaSpout(config) ;
		
		TridentTopology topology = new TridentTopology() ;
		//销售额
		TridentState amtState = topology.newStream("spout", spout)
		.parallelismHint(3)
		.each(new Fields(StringScheme.STRING_SCHEME_KEY),new OrderSplit("\\t"), new Fields("order_id","order_amt","create_date","province_id"))
		.shuffle()
		.groupBy(new Fields("create_date","province_id"))
		.persistentAggregate(new MemoryMapState.Factory(), new Fields("order_amt"), new Sum(), new Fields("sum_amt"));
		
		topology.newDRPCStream("getOrderAmt", drpc)
		.each(new Fields("args"), new Split(" "), new Fields("arg"))
		.each(new Fields("arg"), new SplitBy("\\:"), new Fields("create_date","province_id"))
		.groupBy(new Fields("create_date","province_id"))
		.stateQuery(amtState, new Fields("create_date","province_id"), new MapGet(), new Fields("sum_amt"))
//		.applyAssembly(new FirstN(5, "sum_amt", true))
		;
		
		//订单数
		TridentState orderState = topology.newStream("orderSpout", spout)
		.parallelismHint(3)
		.each(new Fields(StringScheme.STRING_SCHEME_KEY),new OrderSplit("\\t"), new Fields("order_id","order_amt","create_date","province_id"))
		.shuffle()
		.groupBy(new Fields("create_date","province_id"))
		.persistentAggregate(new MemoryMapState.Factory(), new Fields("order_id"), new Count(), new Fields("order_num"));
		
		topology.newDRPCStream("getOrderNum", drpc)
		.each(new Fields("args"), new Split(" "), new Fields("arg"))
		.each(new Fields("arg"), new SplitBy("\\:"), new Fields("create_date","province_id"))
		.groupBy(new Fields("create_date","province_id"))
		.stateQuery(orderState, new Fields("create_date","province_id"), new MapGet(), new Fields("order_num"))
//		.applyAssembly(new FirstN(5, "order_num", true))
		;
		
		Config conf = new Config() ;
		conf.setDebug(false);
		LocalCluster cluster = new LocalCluster() ;
		cluster.submitTopology("myTopo", conf, topology.build());
		
		while (true) {
//			System.err.println("销售额："+drpc.execute("getOrderAmt", "2014-08-19:1 2014-08-19:2 2014-08-19:3 2014-08-19:4 2014-08-19:5")) ;
			System.err.println("订单数："+drpc.execute("getOrderNum", "2014-08-19:1 2014-08-19:2 2014-08-19:3 2014-08-19:4 2014-08-19:5")) ;
			Utils.sleep(5000);
		}
		/**
		 * [["2014-08-19:1 2014-08-19:2 2014-08-19:3 2014-08-19:4 2014-08-19:5","2014-08-19:1","2014-08-19","1",821.9000000000001],
		 * ["2014-08-19:1 2014-08-19:2 2014-08-19:3 2014-08-19:4 2014-08-19:5","2014-08-19:2","2014-08-19","2",631.3000000000001],
		 * ["2014-08-19:1 2014-08-19:2 2014-08-19:3 2014-08-19:4 2014-08-19:5","2014-08-19:3","2014-08-19","3",240.7],
		 * ["2014-08-19:1 2014-08-19:2 2014-08-19:3 2014-08-19:4 2014-08-19:5","2014-08-19:4","2014-08-19","4",340.4],
		 * ["2014-08-19:1 2014-08-19:2 2014-08-19:3 2014-08-19:4 2014-08-19:5","2014-08-19:5","2014-08-19","5",460.8]]
		 */
		
		
	}

}
