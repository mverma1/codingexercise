package mytest;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Scanner;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.io.CharStreams;
import com.treasuredata.client.ExponentialBackOff;
import com.treasuredata.client.TDClient;
import com.treasuredata.client.model.TDJobRequest;
import com.treasuredata.client.model.TDJobSummary;
import com.treasuredata.client.model.TDResultFormat;

public class MyCodingTest {
	private static String db_name;
	private static String table_name;
	private static String col_names;
	private static long min_time;
	private static long max_time;
	private static String queryengine;
	private static String outputformat;
	private static int resultlimit;
	private static String csvfileLocation ="C:/Users/pmve002/result.csv";
	private static String textfileLocation ="C:/Users/pmve002/result.txt";
	protected MyCodingTest() {

	}
	public static void main(String[] args)
	{
		int n= args.length;
		for (int i=0; i<n; i++) {
		//	System.out.println(args[i]);
		}
		if(args.length>0 && args.length==14) {
			if(args[4].contains("hive")) {
				table_name = args[13];
				db_name = args[12];
				resultlimit = Integer.parseInt(args[11]);
				max_time = Integer.parseInt(args[10]);
				min_time = Integer.parseInt(args[8]);
				col_names = args[6];
				queryengine = args[4];
				outputformat = args[2];	
				//Remove ' ' from the column name input argument
				String col_name = col_names.replaceAll("[-+.^:']","");
				//Creation of query based on the requirement
				String hivequery = ("select"+ " "+col_name +" "+"from" +" "+ table_name+" "+ "where TD_TIME_RANGE(time,"+" "+min_time+","+max_time+")"+" "+"LIMIT"+" "+resultlimit);
				if(outputformat.contentEquals("csv")) {
					CSVResultfromQuery(hivequery);
				}
				else if(outputformat.contentEquals("tabular")) {
					TabularResultfromQuery(hivequery);
				}
				else{
					System.out.println("Please check your input query");
				}
				
			}
			else {
				System.out.println("Incorrect Query Entered, please refer sample example: "
						+ "\nquery -f tabular -e hive -c 'col1,col2,...' -m 1146964391 -M 1196964391 100 my_database crime_report");

			}
		}
	}
	private static String CSVResultfromQuery(String query) 
	{
		TDClient td = TDClient.newClient();
		try {

			
			String hivequery = query;
			// Submit a new Hive query
			String jobId = td.submit(TDJobRequest.newHiveQuery(db_name, hivequery));
			// Wait until the query finishes
			ExponentialBackOff backOff = new ExponentialBackOff();
			TDJobSummary job = td.jobStatus(jobId);
			while (!job.getStatus().isFinished()) {
				Thread.sleep(backOff.nextWaitTimeMillis());
				job = td.jobStatus(jobId);
			}
			//Download the result 
			td.jobResult(jobId, TDResultFormat.CSV, new Function<InputStream, String>() 
			{ 
				public String apply(InputStream input) 
				{ 
					try { 
						String result = CharStreams.toString(new InputStreamReader(input)); 
						PrintWriter pw = null;
						pw = new PrintWriter(new File(csvfileLocation));
						StringBuilder builder = new StringBuilder();
						builder.append(result);
						pw.write(builder.toString());
						pw.close();
						return result; 
					} 
					catch (IOException e) { 
						throw Throwables.propagate(e); 
					} 
				} 
			});  	
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			td.close();
		}
		return query;
	}
	private static String TabularResultfromQuery(String query) 
	{
		TDClient td = TDClient.newClient();
		try {

			
			String hivequery = query;
			// Submit a new Hive query
			String jobId = td.submit(TDJobRequest.newHiveQuery(db_name, hivequery));
			// Wait until the query finishes
			ExponentialBackOff backOff = new ExponentialBackOff();
			TDJobSummary job = td.jobStatus(jobId);
			while (!job.getStatus().isFinished()) {
				Thread.sleep(backOff.nextWaitTimeMillis());
				job = td.jobStatus(jobId);
			}
			//Download the result 
			td.jobResult(jobId, TDResultFormat.TSV, new Function<InputStream, String>() 
			{ 
				public String apply(InputStream input) 
				{ 
					try { 
						String result = CharStreams.toString(new InputStreamReader(input)); 
						PrintWriter pw = null;
						pw = new PrintWriter(new File(textfileLocation));
						StringBuilder builder = new StringBuilder();
						builder.append(result);
						pw.write(builder.toString());
						pw.close();
						return result; 
					} 
					catch (IOException e) { 
						throw Throwables.propagate(e); 
					} 
				} 
			});  	
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			td.close();
		}
		return query;
	}

}

