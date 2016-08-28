package com.webmovie.bigdata.storm.wc;

import java.util.Map;
import java.util.UUID;

import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.HBaseMapper;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.google.common.collect.Maps;

/**
 * kafka -> kafkaSpout -> splitBolt -> countBolt -> hbaseBolt -> HBase
 * @author yeahwell
 *
 */
public class WordCountTopology {
	
	private static final String KAFKA_SPOUT = "kafkaSpout";
	
	private static final String SPLIT_BOLT = "splitBolt";
	
	private static final String COUNT_BOLT = "countBolt";
	
	private static final String HBASE_BOLT = "hbaseBolt";
	
	public TopologyBuilder createBuilder(){
		TopologyBuilder builder = new TopologyBuilder();
		return builder;
	}
	
	public TopologyBuilder buildKafkaSpout(TopologyBuilder builder){
		BrokerHosts hosts = new ZkHosts("beifeng-hadoop-02:2181");
		String topic = "test";
		String zkRoot = "/" + topic;
		SpoutConfig spoutConf = new SpoutConfig(hosts, topic, zkRoot, UUID.randomUUID().toString());
		spoutConf.forceFromStart = false;
		//如何解析kafka队列上的数据，以字符串进行解析
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConf);
		
		builder.setSpout(KAFKA_SPOUT, kafkaSpout);
		return builder;
	}
	
	public TopologyBuilder buildSplitBolt(TopologyBuilder builder, String spoutName){
		//bolt由12个Task来运行，12task由3个executor执行
		builder.setBolt(SPLIT_BOLT, new SplitBolt(), 3).setNumTasks(12).shuffleGrouping(spoutName);
		return builder;
	}
	
	public TopologyBuilder buildCountBolt(TopologyBuilder builder, String boltName){
		builder.setBolt(COUNT_BOLT, new CountBolt()).fieldsGrouping(boltName, new Fields("word"));
		return builder;
	}
	
	public TopologyBuilder buildHBaseBolt(TopologyBuilder builder, String boltName){
		// HBase表信息
		String tableName = "wordcount";
		HBaseMapper mapper = new SimpleHBaseMapper()
					.withRowKeyField("word")
					.withColumnFamily("cf")
					.withColumnFields(new Fields("word","count"));
		HBaseBolt hbaseBolt = new HBaseBolt(tableName,  mapper).withConfigKey("hbase.conf");
		builder.setBolt(HBASE_BOLT, hbaseBolt).globalGrouping(boltName);
		return builder;
	}
	
	public static void main(String[] args) {
		
		WordCountTopology topology = new WordCountTopology();
		TopologyBuilder builder = topology.createBuilder();
		topology.buildKafkaSpout(builder);
		topology.buildSplitBolt(builder, KAFKA_SPOUT);
		topology.buildCountBolt(builder, SPLIT_BOLT);
		topology.buildHBaseBolt(builder, COUNT_BOLT);
		
		//指定连接HBase集群的客户端参数
		Map<String,Object> hbaseOpts = Maps.newHashMap();
		hbaseOpts.put("hbase.rootdir", "hdfs://beifeng-hadoop-02:9000");
		hbaseOpts.put("hbase.zookeeper.quorum","beifeng-hadoop-02:2181");
		Config conf = new Config();
		conf.put("aaa", 123);
		conf.put("hbase.conf", hbaseOpts);
		conf.setNumWorkers(2);
		
		conf.setMessageTimeoutSecs(30);
		
		LocalCluster localCluster = new LocalCluster();
		localCluster.submitTopology("wordcountTopo", conf, builder.createTopology());
	}
	
}
