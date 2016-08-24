package com.webmovie.bigdata.hbase.weibo;

import java.io.Serializable;

/**
 * 微博内容实体类
 * @author yeahwell
 *
 */
public class Message implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5621401808589451429L;

	private String uid;
	
	private String content;
	
	private String timestamp;

	public String getUid() {
		return uid;
	}

	public void setUid(String uid) {
		this.uid = uid;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("Message [uid=").append(uid).append(", content=")
				.append(content).append(", timestamp=").append(timestamp)
				.append("]");
		return builder.toString();
	}
	
}
