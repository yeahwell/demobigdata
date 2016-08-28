package com.webmovie.bigdata.storm.trid;

import backtype.storm.utils.DRPCClient;

public class WordCountDrpcClient {

	public static void main(String[] args) {
		DRPCClient client = new DRPCClient("beifeng-hadoop-02", 2372);
		
//		client.execute(func, args);
	}
	
}
