package com.webmovie.bigdata.storm.action02;

import org.apache.storm.hbase.trident.state.HBaseMapState;
import org.apache.storm.hbase.trident.state.HBaseMapState.Options;

import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.FilterNull;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.OpaqueValue;
import storm.trident.state.StateFactory;
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

/**
 * 订单处理入口
 * @author yeahwell
 *
 */
public class OrderProcessTrident {
	
	private static final String KAFKA_SPOUT_ID = "kafkaSpout";
	
	public TridentTopology buildTopology(){
		TridentTopology topology = new TridentTopology();
		return topology;
	}
	
	public Stream bindKafkaSpout(TridentTopology topology){
		BrokerHosts hosts = new ZkHosts("beifeng-hadoop-02:2181");
		String topic = "test";
		TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(hosts, topic);
		kafkaConfig.forceFromStart = false;
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		
//		TransactionalTridentKafkaSpout kafkaSpout = new TransactionalTridentKafkaSpout(kafkaConfig);
		OpaqueTridentKafkaSpout kafkaSpout = new OpaqueTridentKafkaSpout(kafkaConfig);
		//tuple {"str", "value"}
		Stream stream = topology.newStream(KAFKA_SPOUT_ID, kafkaSpout);
		return stream;
	}
	
	public Stream streamParse(Stream stream){
		Stream hasParseStream = 
				stream
//				.each(new Fields("str"), new PrintTestFilter())
				.each(new Fields("str"), new OrderParseFunction(), new Fields(
						"timestamp", "yyyyMMddStr", "yyyyMMddHHStr", "yyyyMMddHHmmStr",
						"consumer", "productName", "price", "country", "province", "city"
						))
				.each(new Fields("timestamp", "yyyyMMddStr", "yyyyMMddHHStr", "yyyyMMddHHmmStr",
						"consumer", "productName", "price", "country", "province", "city"), new PrintTestFilter())
				;
		return hasParseStream;
	}
	
	public Stream streamPartition(Stream stream){
		Stream partitionStream = 
		stream
		//去掉用不到的keyvalue
		.project(new Fields("yyyyMMddStr", "price"))
		.shuffle()
		.groupBy(new Fields("yyyyMMddStr"))
		//================================================================
		.chainedAgg()
		//统计同一批次内各分区中订单金额总和
		.partitionAggregate(new Fields("price"), new SaleSumAggregator(), new Fields("saleTotalAmtOfPartDay"))
		//统计同一批次内各分区订单总笔数
		.partitionAggregate(new Count(), new Fields("numOrderOfPartDay"))
		.chainEnd()
		//================================================================
		.parallelismHint(5)
		.toStream();
		
//		.each(new Fields("yyyyMMddStr", "saleTotalAmtOfPartDay"), new PrintTestFilter())
		return partitionStream;
	}
	
	public TridentState globalSumSaleTotalAmtOfDay(Stream stream){
		TridentState saleTotalAmtState = 
		//全局统计
		stream.groupBy(new Fields("yyyyMMddStr"))
		.persistentAggregate(new MemoryMapState.Factory(), 
				new Fields("saleTotalAmtOfPartDay"), new Sum(),
				new Fields("saleGlobalAmtOfDay"))
		;
		
//		saleTotalAmtState.newValuesStream()
//		.each(new Fields("yyyyMMddStr", "saleGlobalAmtOfDay"), new PrintTestFilter());
		
		return saleTotalAmtState;
	}
	
	public TridentState globalSumNumOrder(Stream stream){
		TridentState numOrderState = 
		stream.groupBy(new Fields("yyyyMMddStr"))
		.persistentAggregate(new MemoryMapState.Factory(), 
				new Fields("numOrderOfPartDay"),new Sum(),
				new Fields("numOrderGlobalOfDay"));
		;
//		numOrderState.newValuesStream()
//		.each(new Fields("yyyyMMddStr", "numOrderGlobalOfDay"), new PrintTestFilter());
		return numOrderState;
	}
	
	public TridentState globalSumTotalAmtOfAddrAndHour(Stream stream){
		@SuppressWarnings("rawtypes")
		Options<OpaqueValue> opts = new HBaseMapState.Options<OpaqueValue>();
		opts.tableName = "saleTotalAmtOfAddrAndHour";
		opts.columnFamily = "cf";
		opts.qualifier = "staOFaah";
		StateFactory factory = HBaseMapState.opaque(opts);
		
		//HBase存储结果
		TridentState saleTotalAmtOfAddrAndHourState = 
		stream
		.project(new Fields("yyyyMMddHHStr", "price", "country", "province", "city"))
		.each(new Fields("yyyyMMddHHStr", "country", "province", "city"), new CombineKeyFunction(), new Fields("addrAndHour"))
		.project(new Fields("addrAndHour", "price"))
		.groupBy(new Fields("addrAndHour"))
		.persistentAggregate(factory, 
				new Fields("price"), 
				new Sum(), 
				new Fields("saleTotalAmtOfAddrAndHour"));
		
		//内存存储结果
//		TridentState saleTotalAmtOfAddrAndHourState = 
//				stream
//				.project(new Fields("yyyyMMddHHStr", "price", "country", "province", "city"))
//				.each(new Fields("yyyyMMddHHStr", "country", "province", "city"), new CombineKeyFunction(), new Fields("addrAndHour"))
//				.project(new Fields("addrAndHour", "price"))
//				.groupBy(new Fields("addrAndHour"))
//				.persistentAggregate(new MemoryMapState.Factory(), 
//						new Fields("price"), 
//						new Sum(), 
//						new Fields("saleTotalAmtOfAddrAndHour"));
		
		saleTotalAmtOfAddrAndHourState
		.newValuesStream()
		.each(new Fields("addrAndHour", "saleTotalAmtOfAddrAndHour"), new PrintTestFilter())
		;
		return saleTotalAmtOfAddrAndHourState;
	}
	
	public static void main(String[] args) {
		OrderProcessTrident opTrident = new OrderProcessTrident();
		TridentTopology topology = opTrident.buildTopology();
		Stream stream = opTrident.bindKafkaSpout(topology);
		Stream parseStream = opTrident.streamParse(stream);
		Stream partitionStream = opTrident.streamPartition(parseStream);
		
		TridentState saleTotalAmtState = opTrident.globalSumSaleTotalAmtOfDay(partitionStream);
		TridentState numOrderState = opTrident.globalSumNumOrder(partitionStream);
		TridentState saleTotalAmtOfAddrAndHourState = opTrident.globalSumTotalAmtOfAddrAndHour(parseStream);
		
		//构造一个本地drpc服务
		LocalDRPC localDRPC = new LocalDRPC();
		
		topology.newDRPCStream("saleAmtOfDayDRPC", localDRPC)
		.each(new Fields("args"), new DRPCFunction(), new Fields("requestDate"))
		.stateQuery(saleTotalAmtState, new Fields("requestDate"), new MapGet(), new Fields("saleGlobalAmtOfDay1"))
		.project(new Fields("requestDate", "saleGlobalAmtOfDay1"))
		.each(new Fields("saleGlobalAmtOfDay1"), new FilterNull())
		;
		
		topology.newDRPCStream("numOrderOfDayDRPC", localDRPC)
		.each(new Fields("args"), new DRPCFunction(), new Fields("requestDate"))
		.stateQuery(numOrderState, new Fields("requestDate"), new MapGet(), new Fields("numOrderOfDay1"))
		.project(new Fields("requestDate", "numOrderOfDay1"))
		.each(new Fields("numOrderOfDay1"), new FilterNull())
		;
		
		topology.newDRPCStream("saleTotalAmtOfAddrAndHourDRPC", localDRPC)
		.each(new Fields("args"), new DRPCFunction(), new Fields("requestDate"))
		.stateQuery(saleTotalAmtOfAddrAndHourState, new Fields("requestDate"), new MapGet(), new Fields("saleTotalAmtOfAddrAndHour"))
		.project(new Fields("requestDate", "saleTotalAmtOfAddrAndHour"))
		.each(new Fields("saleTotalAmtOfAddrAndHour"), new FilterNull())
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
				String jsonResult01 = localDRPC.execute("saleAmtOfDayDRPC", "20160830 20160829");
				System.err.println("saleAmtOfDayDRPC result = " + jsonResult01);			
				
				String jsonResult02 = localDRPC.execute("numOrderOfDayDRPC", "20160830 20160829");
				System.err.println("numOrderOfDayDRPC result = " + jsonResult02);
				
				String jsonResult03 = localDRPC.execute("saleTotalAmtOfAddrAndHourDRPC", "苏州_江苏_中国_2016083009");
				System.err.println("saleTotalAmtOfAddrAndHourDRPC result = " + jsonResult03);
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
