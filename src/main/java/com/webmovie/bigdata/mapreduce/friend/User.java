package com.webmovie.bigdata.mapreduce.friend;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 自定义用户作为key
 * @author webmovie
 *
 */
public class User implements WritableComparable<User>{
	
	private String userName;
	
	private String fof; // 间接好友
	  
	private int weight;

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(userName);
		out.writeUTF(fof);
		out.writeInt(weight);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.userName = in.readUTF();
		this.fof = in.readUTF();
		this.weight = in.readInt();
	}

	/**
	 * 
	 * 分区  reducer
	 * 
	 * 
	 * 自定义key里面的该方法
	 * 
	 * 		1）如果后面没有自定义排序、自定义分组，key的compareTo作为默认排序、分组的逻辑
	 * 		2）如果有自定义排序，compareTo不用考虑，被覆盖，既不能起排序作用，也不能作为分组逻辑
	 * 		3）如果有自定义分组，但是没有自定义排序，则compareTo作为默认排序的逻辑，
	 * 		但对于分组无效
	 * 
	 * 
	 * 
	 */
	@Override
	public int compareTo(User o) {
		
		return 0;
	}

	public String getUserName() {
		return userName;
	}

	public void setUserName(String userName) {
		this.userName = userName;
	}

	public String getFof() {
		return fof;
	}

	public void setFof(String fof) {
		this.fof = fof;
	}

	public int getWeight() {
		return weight;
	}

	public void setWeight(int weight) {
		this.weight = weight;
	}
	
}
