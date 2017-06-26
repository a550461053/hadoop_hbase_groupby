
import java.io.*;
import java.text.DecimalFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.*;

/**
 * This is the main method which 
 * 
 * @author Batista
 * @version 1.0
 * @since 2017-3-25
 *
 */

public class Hw1Grp2 {
	/**
	 * This is the main method which makes
	 * @param args
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	public static void main(String[] args) throws IOException, URISyntaxException {
		
		MainProcess mainProcess = new MainProcess(args);
		mainProcess.setArgs();
		String filePath = mainProcess.getArgs().str1[0];
//		System.out.println("----");
		int groupByKey = mainProcess.getArgs().int1[0];
		int avg_R = mainProcess.getArgs().int1[1];
		int max_R = mainProcess.getArgs().int1[2];
		boolean countFlag = mainProcess.getArgs().bool1[0];
		boolean avgFlag = mainProcess.getArgs().bool1[1];
		boolean maxFlag = mainProcess.getArgs().bool1[2];
		String avg_R_str = mainProcess.getArgs().str1[1];
		String max_R_str = mainProcess.getArgs().str1[2];
		
		MyHdfsFile myHdfsFile = new MyHdfsFile(filePath);
		myHdfsFile.startHdfs();
		List<String[]> list_result = myHdfsFile.readFileToList();
		
		/*
		 * hashtable
		 */
		Hashtable<String, MyGroup> hashtable1 = new Hashtable<String, MyGroup>();
		for (int i = 0; i < list_result.size(); i++) {
			String myKey = list_result.get(i)[groupByKey];
			double myValue_avg = new Double(list_result.get(i)[avg_R]); 
			double myValue_max = Double.valueOf((list_result.get(i)[max_R])); 
//				double myValue2 = double.parsedouble((myResult.get(i)[2])); 
			if (hashtable1.containsKey(myKey)) { // old key
				int count = hashtable1.get(myKey).getCount();
				double sum = hashtable1.get(myKey).getSum();
				double avg = hashtable1.get(myKey).getAvg();
				double max = hashtable1.get(myKey).getMax();
				count++;
				sum += myValue_avg;
				avg = sum / count;
				max = myValue_max > max ? myValue_max : max ;
				hashtable1.put(myKey, new MyGroup(count, sum, avg, max));
			} else { //new key
				hashtable1.put(myKey, new MyGroup(1, myValue_avg, myValue_avg, myValue_max));
			}
		}
		
		/*
		 * Hbase 
		 */
		Logger.getRootLogger().setLevel(Level.WARN);
	
	    // create table descriptor
	    String tableName= "Result";
	    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
	    
	    // create and add column descriptor
	    htd.addFamily(new HColumnDescriptor("res"));
	    
	    // configure HBase
	    Configuration configuration = HBaseConfiguration.create();
	    HBaseAdmin hAdmin = new HBaseAdmin(configuration);
	
	    if (hAdmin.tableExists(tableName)) {
	        System.out.println("Table already exists");
	        System.out.println("Delete...");
	        hAdmin.disableTable(tableName); //close a table
	        hAdmin.deleteTable(tableName); // delete a table
	        System.out.println("delete success!");
	    }
	    hAdmin.createTable(htd);
	    System.out.println("table "+tableName+ " created successfully");
	    
	    hAdmin.close();
	
	    // traverse HashTable and put it to Hbase's table
	    HTable table = new HTable(configuration,tableName);

	 	Enumeration<String> e2 = hashtable1.keys();
		while (e2.hasMoreElements()) {
			String keys = e2.nextElement();
			int count = hashtable1.get(keys).getCount();
			double sum = hashtable1.get(keys).getSum(); 
			double avg = hashtable1.get(keys).getAvg(); 
			double max = hashtable1.get(keys).getMax(); 
			avg = (double)(Math.round(avg*100)/100.0);
			DecimalFormat df = new DecimalFormat("#.00");
			String avg_df = df.format(avg);
//			System.out.println("key=" + keys 
//					+ " count=" + count
//	//				+ " sum=" + sum 
//					+ " avg=" + avg_df
//					+ " max=" + max);
		    Put put = new Put(keys.getBytes()); // key
		    
		    if(countFlag) {
		    	put.add("res".getBytes(), "count".getBytes(), String.valueOf(count).getBytes()); // res:count=3
		    } 
		    if (avgFlag) {
		    	put.add("res".getBytes(), avg_R_str.getBytes(), avg_df.getBytes());
		    }
		    if (maxFlag) {
		    	put.add("res".getBytes(), max_R_str.getBytes(), String.valueOf(max).getBytes());
		    }
		    
		    table.put(put);
		}
	
	    table.close();
	    System.out.println("put successfully");
	
    	myHdfsFile.stopHdfs();
	}
}
/**
 * This class is used to construct generic TripleTuple.
 * It can help a method to return three different types.
 * @author Batista
 *
 * @param <A>
 * @param <B>
 * @param <C>
 */
class TripleTuple<A, B, C> {
	public final A str1;
	public final B int1;
	public final C bool1;
	
	public TripleTuple(A str1, B int1, C bool1) {
		this.str1 = str1;
		this.int1 = int1;
		this.bool1 = bool1;
	}
}
/**
 * This class is used to receice and process the args of the Main() method's input.
 * 
 * @author Batista
 * @version 1.0
 *
 */
class MainProcess {

	String[] args;
	// java Hw1Grp2 R=hdfs://localhost:9000/yu/orders.tbl groupby:R1 res:count,avg(R3),max(R3)
	String filePath;
	int groupByKey = 1;
	int avg_R = 3;
	int max_R = 3;
	
	public boolean countFlag = false;
	public boolean avgFlag = false;
	public boolean maxFlag = false;

	public String avg_R_str = null;
	public String max_R_str = null;
	
	MainProcess(String[] args) {
		this.args = args;
	
	}
	/**
	 * This method is used to getArgs of input.
	 * 
	 * @return TripleTuple
	 * @version 1.0
	 */
	public TripleTuple<String[], int[], boolean[]> getArgs() {
		this.setArgs();
		String[] str = new String[3];
		str[0] = this.filePath;
		str[1] = this.avg_R_str;
		str[2] = this.max_R_str;
		int[] int1 = new int[3];
		int1[0] = this.groupByKey;
		int1[1] = this.avg_R;
		int1[2] = this.max_R;
		boolean[] bool1 = new boolean[3];
		bool1[0] = this.countFlag;
		bool1[1] = this.avgFlag;
		bool1[2] = this.maxFlag;
		return new TripleTuple<String[], int[], boolean[]>(str, int1, bool1);
	}
	
	/**
	 * show args of input
	 * 
	 * @author Batista
	 * @version 1.0
	 */
	public void showArgs() {
		System.out.println("R=file: " + args[0].split("=")[1]);
		System.out.println("groupby: R" + args[1].substring(args[1].length()-1, args[1].length()));
		System.out.println("res: " + args[2].split(":")[1].split(",").length);
	}
	
	/**
	 * set variables by args of input
	 * 
	 * @author Batista
	 * @version 1.0
	 */
	public void setArgs() {
		this.groupByKey = Integer.valueOf(args[1].substring(args[1].length()-1, args[1].length()));
		this.filePath = args[0].split("=")[1];
		String regEx = "[^0-9]"; //
		
		String[] res_in = new String[args[2].split(":")[1].split(",").length];
		for (int i = 0; i < args[2].split(":")[1].split(",").length; i++) {
//			System.out.println(args[2].split(":")[1].split(",")[i]);
			res_in[i] = args[2].split(":")[1].split(",")[i];
			
			if (res_in[i].equals("count")) {
				countFlag = true;
			} 
			if (res_in[i].startsWith("avg")) {
				avgFlag = true;
				// 
				Pattern p1 = Pattern.compile(regEx);
				Matcher m1 = p1.matcher(res_in[i]);
				avg_R = Integer.valueOf(m1.replaceAll("").trim());
				avg_R_str = "avg(R"+ avg_R + ")";
//				System.out.println(avg_R_str);
			} 
			if (res_in[i].startsWith("max")) {
				maxFlag = true;
				Pattern p2 = Pattern.compile(regEx);
				Matcher m2 = p2.matcher(res_in[i]);
				max_R = Integer.valueOf(m2.replaceAll("").trim());
				max_R_str = "max(R" + max_R + ")";
//				System.out.println(max_R_str);
			}
		}
	}
}
/**
 * This class is used to construct generic TwoTuple.
 * It can help a method to return two different types.
 * @author Batista
 *
 * @param <A>
 * @param <B>
 */
class TwoTuple<A, B> {
	public final A fs;
	public final B buf;
	
	public TwoTuple(A fs, B buf) {
		this.fs = fs;
		this.buf = buf;
	}
}

/**
 * This class is used to control HDFS and read a file from HDFS.
 * 
 * @author guest
 * @since 2017-3-25
 * 
 */
class MyHdfsFile {
	private final String filePath;
	private FileSystem fs;
	private BufferedReader buf;
	
	MyHdfsFile(String filePath) {
		this.filePath = filePath;
	}
	/**
	 * This method is used to start HDFS.
	 * 
	 * @return FileSystem
	 * @return BufferedReader
	 * @throws IOException
	 */
	public TwoTuple<FileSystem, BufferedReader> startHdfs() throws IOException {
		
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(this.filePath), conf);
		Path path = new Path(this.filePath);
		FSDataInputStream in_stream = fs.open(path);
	
		BufferedReader buf = new BufferedReader(new InputStreamReader(in_stream));
		System.out.println("hdfs starting...");
		this.fs = fs;
		this.buf = buf;
		return new TwoTuple<FileSystem, BufferedReader>(fs, buf);
	}
	/**
	 * Gets the results of startHdfs().
	 * 
	 * @return FileSystem
	 * @return BufferedReader
	 * @throws IOException
	 */
	public TwoTuple<FileSystem, BufferedReader> getStatus() throws IOException {
		return new TwoTuple<FileSystem, BufferedReader>(this.fs, this.buf);
	}
	/**
	 * This method is used to read a file from HDFS and return list.
	 * 
	 * @return List<String[]>
	 * @throws IOException
	 */
	public List<String[]> readFileToList() throws IOException {
		List<String> list = new ArrayList<String>();
		String readLineData;
		// read buffer from Hdfs
		while(null != (readLineData = this.getStatus().buf.readLine())) {
			list.add(readLineData);
		}
	
		List<String[]> list_result = new ArrayList<String[]>();
		for (int i = 0; i < list.size(); i++) {
			String[] arr_temp = list.get(i).split("\\|");
			String[] arr_result = new String[arr_temp.length];
			int index = 0;
			for (String str : arr_temp) {
				arr_result[index++] = str;
			}
			list_result.add(arr_result);
		}
		return list_result;
	}
	/**
	 * Stops the HDFS.
	 * 
	 * @throws IOException
	 */
	public void stopHdfs() throws IOException {
	    this.getStatus().buf.close();
		this.getStatus().fs.close();
		System.out.println("hdfs stop.");
	}
}

/**
 * This is a class of one group-by data.
 * 
 * @author Batista
 * @version 1.0
 *
 */
class MyGroup {
	int count;
	double sum;
	double avg;
	double max;
	MyGroup(int count, double sum, double avg, double max) {
		this.count = count;
		this.avg = avg;
		this.max = max;
		this.sum = sum;
	}
	public void setCount(int count) {
		this.count = count;
	}
	public void setSum(double sum) {
		this.sum = sum;
	}
	public void setAvg(double avg) {
		this.avg = avg;
	}
	public void setMax(double max) {
		this.max = max;
	}
	public int getCount() {
		return this.count;
	}
	public double getAvg() {
		return this.avg;
	}
	public double getSum() {
		return this.sum;
	}
	public double getMax() {
		return this.max;
	}
}
