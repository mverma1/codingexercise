package mytest;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;

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
	private static String csvfileLocation = "C:/Users/Public/result.csv";
	private static String textfileLocation = "C:/Users/Public/result.txt";

	protected MyCodingTest() {

	}

	public static void main(String[] args) {
		int n = args.length;
		for (int i = 0; i < n; i++) {
			// System.out.println(args[i]);
		}

		if (args.length > 0 && args.length == 14 && args[13].contentEquals("crime_report")
				&& args[12].contentEquals("my_database")) {
			if (args[1].contentEquals("-f") && args[3].contentEquals("-e") && args[5].contentEquals("-c")
					&& args[7].contentEquals("-m") && args[9].contentEquals("-M") && args[10].matches("^-?\\d+$")
					&& args[8].matches("^-?\\d+$") && args[11].matches("^-?\\d+$")) {
				table_name = args[13];
				db_name = args[12];
				resultlimit = Integer.parseInt(args[11]);
				max_time = Integer.parseInt(args[10]);
				min_time = Integer.parseInt(args[8]);
				col_names = args[6];
				queryengine = args[4];
				outputformat = args[2];
				// Remove ' ' from the column name input argument
				String col_name = col_names.replaceAll("[-+.^:']", "");
				// Creation of query based on the requirement
				String hivequery = ("select" + " " + col_name + " " + "from" + " " + table_name + " "
						+ "where TD_TIME_RANGE(time," + " " + min_time + "," + max_time + ")" + " " + "LIMIT" + " "
						+ resultlimit);
				if (outputformat.contentEquals("csv") && queryengine.contentEquals("hive") && max_time > min_time) {
					CSVResultfromQuery(hivequery);
				}
				if (outputformat.contentEquals("tabular") && queryengine.contentEquals("hive") && max_time > min_time) {
					TabularResultfromQuery(hivequery);
				}
			}
		}

		else {
			if (args.length > 0 && args.length == 3 && args[2].contentEquals("crime_report")
					&& args[1].contentEquals("my_database")) {
				table_name = args[2];
				db_name = args[1];

				String hivequery = ("select * " + " " + "from" + " " + table_name);
				TabularResultfromQuery(hivequery);
			}

			else {
				if (args.length > 0 && args.length == 4 && args[3].contentEquals("crime_report")
						&& args[2].contentEquals("my_database") && args[1].matches("^-?\\d+$")) {
					table_name = args[3];
					db_name = args[2];
					resultlimit = Integer.parseInt(args[1]);
					String hivequery = ("select * " + " " + "from" + " " + table_name + " " + "LIMIT" + " "
							+ resultlimit);
					TabularResultfromQuery(hivequery);
				}

				else {

					if (args.length > 0 && args.length == 5 && args[4].contentEquals("crime_report")
							&& args[3].contentEquals("my_database")) {
						if (args[1].contentEquals("-c")) {
							table_name = args[4];
							db_name = args[3];
							col_names = args[2];
							String col_name = col_names.replaceAll("[-+.^:']", "");
							String hivequery = ("select" + " " + col_name + " " + "from" + " " + table_name);
							TabularResultfromQuery(hivequery);
						}
						if (args[1].contentEquals("-f")) {
							table_name = args[4];
							db_name = args[3];
							outputformat = args[2];

							String hivequery = ("select * " + " " + "from" + " " + table_name);
							if (outputformat.contentEquals("csv")) {
								CSVResultfromQuery(hivequery);
							}
							if (outputformat.contentEquals("tabular")) {
								TabularResultfromQuery(hivequery);
							}
						} else if (args[1].contentEquals("-e")) {
							table_name = args[4];
							db_name = args[3];
							queryengine = args[2];

							String hivequery = ("select * " + " " + "from" + " " + table_name);
							if (queryengine.contentEquals("hive")) {
								TabularResultfromQuery(hivequery);
							}
							if (queryengine.contentEquals("presto")) {
								System.out.println("Error Message: This account only supports hive query");
							}
						}
					} else {
						if (args.length > 0 && args.length == 6 && args[5].contentEquals("crime_report")
								&& args[4].contentEquals("my_database") && args[3].matches("^-?\\d+$")) {
							if (args[1].contains("-c")) {
								table_name = args[5];
								db_name = args[4];
								resultlimit = Integer.parseInt(args[3]);
								col_names = args[2];
								String col_name = col_names.replaceAll("[-+.^:']", "");
								String hivequery = ("select" + " " + col_name + " " + "from" + " " + table_name + " "
										+ "LIMIT" + " " + resultlimit);
								TabularResultfromQuery(hivequery);
							}
							if (args[1].contains("-f")) {
								table_name = args[5];
								db_name = args[4];
								resultlimit = Integer.parseInt(args[3]);
								outputformat = args[2];
								String hivequery = ("select * " + " " + "from" + " " + table_name + " " + "LIMIT" + " "
										+ resultlimit);
								if (outputformat.contentEquals("csv")) {
									CSVResultfromQuery(hivequery);
								}
								if (outputformat.contentEquals("tabular")) {
									TabularResultfromQuery(hivequery);
								}
							}
							if (args[1].contains("-e")) {
								table_name = args[5];
								db_name = args[4];
								resultlimit = Integer.parseInt(args[3]);
								queryengine = args[2];
								String hivequery = ("select * " + " " + "from" + " " + table_name + " " + "LIMIT" + " "
										+ resultlimit);
								if (queryengine.contentEquals("hive")) {
									TabularResultfromQuery(hivequery);
								}
								if (queryengine.contentEquals("presto")) {
									System.out.println("Error Message: This account only supports hive query");
								}
							}

						} else {
							if (args.length > 0 && args.length == 7 && args[6].contentEquals("crime_report")
									&& args[5].contentEquals("my_database")) {
								if (args[1].contains("-f") && args[3].contains("-e")) {
									table_name = args[6];
									db_name = args[5];
									queryengine = args[4];
									outputformat = args[2];
									String hivequery = ("select * " + " " + "from" + " " + table_name);
									if (queryengine.contentEquals("hive") && outputformat.contentEquals("tabular")) {
										TabularResultfromQuery(hivequery);
									}
									if (queryengine.contentEquals("hive") && outputformat.contentEquals("csv")) {
										CSVResultfromQuery(hivequery);
									}
									if (queryengine.contentEquals("presto")) {
										System.out.println("Error Message: This account only supports hive query");
									}
								}
								if (args[1].contains("-f") && args[3].contains("-c")) {
									table_name = args[6];
									db_name = args[5];
									col_names = args[4];
									outputformat = args[2];
									String col_name = col_names.replaceAll("[-+.^:']", "");
									String hivequery = ("select" + " " + col_name + " " + "from" + " " + table_name);
									if (outputformat.contentEquals("tabular")) {
										TabularResultfromQuery(hivequery);
									}
									if (outputformat.contentEquals("csv")) {
										CSVResultfromQuery(hivequery);
									}
								}
								if (args[1].contains("-e") && args[3].contains("-c")) {
									table_name = args[6];
									db_name = args[5];
									col_names = args[4];
									queryengine = args[2];
									String col_name = col_names.replaceAll("[-+.^:']", "");
									String hivequery = ("select" + " " + col_name + " " + "from" + " " + table_name);
									if (queryengine.contentEquals("hive")) {
										TabularResultfromQuery(hivequery);
									}
									if (queryengine.contentEquals("presto")) {
										System.out.println("Error Message: This account only supports hive query");
									}
								}
								if (args[1].contains("-m") && args[3].contains("-M") && args[4].matches("^-?\\d+$")
										&& args[2].matches("^-?\\d+$")) {
									table_name = args[6];
									db_name = args[5];
									max_time = Integer.parseInt(args[4]);
									min_time = Integer.parseInt(args[2]);

									String hivequery = ("select *" + " " + "from" + " " + table_name + " "
											+ "where TD_TIME_RANGE(time," + " " + min_time + "," + max_time + ")");
									if (max_time > min_time) {
										TabularResultfromQuery(hivequery);
									}
								}

							}

							else {
								if (args.length > 0 && args.length == 8 && args[7].contentEquals("crime_report")
										&& args[6].contentEquals("my_database") && args[5].matches("^-?\\d+$")) {
									if (args[1].contentEquals("-f") && args[3].contentEquals("-e")) {
										table_name = args[7];
										db_name = args[6];
										resultlimit = Integer.parseInt(args[5]);
										queryengine = args[4];
										outputformat = args[2];
										String hivequery = ("select * " + " " + "from" + " " + table_name + " "
												+ "LIMIT" + " " + resultlimit);
										if (queryengine.contentEquals("hive")
												&& outputformat.contentEquals("tabular")) {
											TabularResultfromQuery(hivequery);
										}
										if (queryengine.contentEquals("hive") && outputformat.contentEquals("csv")) {
											CSVResultfromQuery(hivequery);
										}
										if (queryengine.contentEquals("presto")) {
											System.out.println("Error Message: This account only supports hive query");
										}

									}
									if (args[1].contentEquals("-f") && args[3].contentEquals("-c")) {
										table_name = args[7];
										db_name = args[6];
										resultlimit = Integer.parseInt(args[5]);
										col_names = args[4];
										outputformat = args[2];
										String col_name = col_names.replaceAll("[-+.^:']", "");
										String hivequery = ("select" + " " + col_name + " " + "from" + " " + table_name
												+ " " + "LIMIT" + " " + resultlimit);
										if (queryengine.contentEquals("csv")) {
											CSVResultfromQuery(hivequery);
										}
										if (queryengine.contentEquals("tabular")) {
											TabularResultfromQuery(hivequery);
										}
									}
									if (args[1].contentEquals("-e") && args[3].contentEquals("-c")) {
										table_name = args[7];
										db_name = args[6];
										resultlimit = Integer.parseInt(args[5]);
										col_names = args[4];
										queryengine = args[2];
										String col_name = col_names.replaceAll("[-+.^:']", "");
										String hivequery = ("select" + " " + col_name + " " + "from" + " " + table_name
												+ " " + "LIMIT" + " " + resultlimit);
										if (queryengine.contentEquals("hive")) {
											TabularResultfromQuery(hivequery);
										}
										if (queryengine.contentEquals("presto")) {
											System.out.println("Error Message: This account only supports hive query");
										}
									}
									if (args[1].contentEquals("-m") && args[3].contentEquals("-M")
											&& args[4].matches("^-?\\d+$") && args[2].matches("^-?\\d+$")) {
										table_name = args[7];
										db_name = args[6];
										resultlimit = Integer.parseInt(args[5]);
										max_time = Integer.parseInt(args[4]);
										min_time = Integer.parseInt(args[2]);

										String hivequery = ("select *" + " " + "from" + " " + table_name + " "
												+ "where TD_TIME_RANGE(time," + " " + min_time + "," + max_time + ")"
												+ " " + "LIMIT" + " " + resultlimit);
										if (max_time > min_time) {
											TabularResultfromQuery(hivequery);
										}
									}

								} else {
									if (args.length > 0 && args.length == 9) {
										System.out.println("Error Message: Please check your inpit query");
									} else {
										if (args.length > 0 && args.length == 10
												&& args[9].contentEquals("crime_report")
												&& args[8].contentEquals("my_database")
												&& args[7].matches("^-?\\d+$")) {
											if (args[1].contentEquals("-f") && args[3].contentEquals("-e")
													&& args[5].contentEquals("-c")) {
												table_name = args[9];
												db_name = args[8];
												resultlimit = Integer.parseInt(args[7]);
												col_names = args[6];
												queryengine = args[4];
												outputformat = args[2];
												String col_name = col_names.replaceAll("[-+.^:']", "");
												String hivequery = ("select" + " " + col_name + " " + "from" + " "
														+ table_name + " " + "LIMIT" + " " + resultlimit);
												if (queryengine.contentEquals("hive")
														&& outputformat.contentEquals("tabular")) {
													TabularResultfromQuery(hivequery);
												}
												if (queryengine.contentEquals("hive")
														&& outputformat.contentEquals("csv")) {
													CSVResultfromQuery(hivequery);
												}
												if (queryengine.contentEquals("presto")) {
													System.out.println(
															"Error Message: This account only supports hive query");
												}
											}
											if (args[1].contentEquals("-f") && args[3].contentEquals("-m")
													&& args[5].contentEquals("-M") && args[6].matches("^-?\\d+$")
													&& args[4].matches("^-?\\d+$")) {
												table_name = args[9];
												db_name = args[8];
												resultlimit = Integer.parseInt(args[7]);
												max_time = Integer.parseInt(args[6]);
												min_time = Integer.parseInt(args[4]);
												outputformat = args[2];
												String hivequery = ("select *" + " " + "from" + " " + table_name + " "
														+ "where TD_TIME_RANGE(time," + " " + min_time + "," + max_time
														+ ")" + " " + "LIMIT" + " " + resultlimit);
												if (outputformat.contentEquals("tabular") && max_time > min_time) {
													TabularResultfromQuery(hivequery);
												}
												if (outputformat.contentEquals("csv") && max_time > min_time) {
													CSVResultfromQuery(hivequery);
												}
											}
											if (args[1].contentEquals("-e") && args[3].contentEquals("-m")
													&& args[5].contentEquals("-M") && args[6].matches("^-?\\d+$")
													&& args[4].matches("^-?\\d+$")) {
												table_name = args[9];
												db_name = args[8];
												resultlimit = Integer.parseInt(args[7]);
												max_time = Integer.parseInt(args[6]);
												min_time = Integer.parseInt(args[4]);
												queryengine = args[2];
												String hivequery = ("select *" + " " + "from" + " " + table_name + " "
														+ "where TD_TIME_RANGE(time," + " " + min_time + "," + max_time
														+ ")" + " " + "LIMIT" + " " + resultlimit);
												if (queryengine.contentEquals("hive") && max_time > min_time) {
													TabularResultfromQuery(hivequery);
												}
												if (queryengine.contentEquals("presto")) {
													System.out.println(
															"Error Message: This account only supports hive query");
												}
											}
											if (args[1].contentEquals("-c") && args[3].contentEquals("-m")
													&& args[5].contentEquals("-M") && args[6].matches("^-?\\d+$")
													&& args[4].matches("^-?\\d+$")) {
												table_name = args[9];
												db_name = args[8];
												resultlimit = Integer.parseInt(args[7]);
												max_time = Integer.parseInt(args[6]);
												min_time = Integer.parseInt(args[4]);
												col_names = args[2];
												String col_name = col_names.replaceAll("[-+.^:']", "");
												String hivequery = ("select" + " " + col_name + " " + "from" + " "
														+ table_name + " " + "where TD_TIME_RANGE(time," + " "
														+ min_time + "," + max_time + ")" + " " + "LIMIT" + " "
														+ resultlimit);
												if (max_time > min_time) {
													TabularResultfromQuery(hivequery);
												}
											}
										} else {
											if (args.length > 0 && args.length == 11
													&& args[10].contentEquals("crime_report")
													&& args[9].contentEquals("my_database")) {
												if (args[1].contentEquals("-f") && args[3].contentEquals("-e")
														&& args[5].contentEquals("-m") && args[7].contentEquals("-M")
														&& args[8].matches("^-?\\d+$") && args[6].matches("^-?\\d+$")) {
													table_name = args[10];
													db_name = args[9];
													max_time = Integer.parseInt(args[8]);
													min_time = Integer.parseInt(args[6]);
													queryengine = args[4];
													outputformat = args[2];
													String hivequery = ("select *" + " " + "from" + " " + table_name
															+ " " + "where TD_TIME_RANGE(time," + " " + min_time + ","
															+ max_time + ")");
													if (queryengine.contentEquals("hive")
															&& outputformat.contentEquals("tabular")
															&& max_time > min_time) {
														TabularResultfromQuery(hivequery);
													}
													if (queryengine.contentEquals("hive")
															&& outputformat.contentEquals("csv")
															&& max_time > min_time) {
														CSVResultfromQuery(hivequery);
													}
													if (queryengine.contentEquals("presto")) {
														System.out.println(
																"Error Message: This account only supports hive query");
													}
												}
												if (args[1].contentEquals("-f") && args[3].contentEquals("-c")
														&& args[5].contentEquals("-m") && args[7].contentEquals("-M")
														&& args[8].matches("^-?\\d+$") && args[6].matches("^-?\\d+$")) {
													table_name = args[10];
													db_name = args[9];
													max_time = Integer.parseInt(args[8]);
													min_time = Integer.parseInt(args[6]);
													col_names = args[4];
													outputformat = args[2];
													String col_name = col_names.replaceAll("[-+.^:']", "");
													String hivequery = ("select" + " " + col_name + " " + "from" + " "
															+ table_name + " " + "where TD_TIME_RANGE(time," + " "
															+ min_time + "," + max_time + ")");
													if (outputformat.contentEquals("tabular") && max_time > min_time) {
														TabularResultfromQuery(hivequery);
													}
													if (outputformat.contentEquals("csv") && max_time > min_time) {
														CSVResultfromQuery(hivequery);
													}
												}
												if (args[1].contentEquals("-e") && args[3].contentEquals("-c")
														&& args[5].contentEquals("-m") && args[7].contentEquals("-M")
														&& args[8].matches("^-?\\d+$") && args[6].matches("^-?\\d+$")) {
													table_name = args[10];
													db_name = args[9];
													max_time = Integer.parseInt(args[8]);
													min_time = Integer.parseInt(args[6]);
													col_names = args[4];
													queryengine = args[2];
													String hivequery = ("select *" + " " + "from" + " " + table_name
															+ " " + "where TD_TIME_RANGE(time," + " " + min_time + ","
															+ max_time + ")");
													if (queryengine.contentEquals("hive") && max_time > min_time) {
														TabularResultfromQuery(hivequery);
													}
													if (queryengine.contentEquals("presto")) {
														System.out.println(
																"Error Message: This account only supports hive query");
													}
												}

											} else {
												if (args.length > 0 && args.length == 12
														&& args[11].contentEquals("crime_report")
														&& args[10].contentEquals("my_database")
														&& args[9].matches("^-?\\d+$")) {
													if (args[1].contentEquals("-f") && args[3].contentEquals("-e")
															&& args[5].contentEquals("-m")
															&& args[7].contentEquals("-M")
															&& args[8].matches("^-?\\d+$")
															&& args[6].matches("^-?\\d+$")) {
														table_name = args[11];
														db_name = args[10];
														resultlimit = Integer.parseInt(args[9]);
														max_time = Integer.parseInt(args[8]);
														min_time = Integer.parseInt(args[6]);
														queryengine = args[4];
														outputformat = args[2];
														String hivequery = ("select *" + " " + "from" + " " + table_name
																+ " " + "where TD_TIME_RANGE(time," + " " + min_time
																+ "," + max_time + ")" + " " + "LIMIT" + " "
																+ resultlimit);
														if (queryengine.contentEquals("hive")
																&& outputformat.contentEquals("tabular")
																&& max_time > min_time) {
															TabularResultfromQuery(hivequery);
														}
														if (queryengine.contentEquals("hive")
																&& outputformat.contentEquals("csv")
																&& max_time > min_time) {
															CSVResultfromQuery(hivequery);
														}
														if (queryengine.contentEquals("presto")) {
															System.out.println(
																	"Error Message: This account only supports hive query");
														}
													}
													if (args[1].contentEquals("-f") && args[3].contentEquals("-c")
															&& args[5].contentEquals("-m")
															&& args[7].contentEquals("-M")
															&& args[8].matches("^-?\\d+$")
															&& args[6].matches("^-?\\d+$")) {
														table_name = args[11];
														db_name = args[10];
														resultlimit = Integer.parseInt(args[9]);
														max_time = Integer.parseInt(args[8]);
														min_time = Integer.parseInt(args[6]);
														col_names = args[4];
														outputformat = args[2];
														String col_name = col_names.replaceAll("[-+.^:']", "");
														String hivequery = ("select" + " " + col_name + " " + "from"
																+ " " + table_name + " " + "where TD_TIME_RANGE(time,"
																+ " " + min_time + "," + max_time + ")" + " " + "LIMIT"
																+ " " + resultlimit);
														if (outputformat.contentEquals("tabular")
																&& max_time > min_time) {
															TabularResultfromQuery(hivequery);
														}
														if (outputformat.contentEquals("csv") && max_time > min_time) {
															CSVResultfromQuery(hivequery);
														}
													}
													if (args[1].contentEquals("-e") && args[3].contentEquals("-c")
															&& args[5].contentEquals("-m")
															&& args[7].contentEquals("-M")
															&& args[8].matches("^-?\\d+$")
															&& args[6].matches("^-?\\d+$")) {
														table_name = args[11];
														db_name = args[10];
														resultlimit = Integer.parseInt(args[9]);
														max_time = Integer.parseInt(args[8]);
														min_time = Integer.parseInt(args[6]);
														col_names = args[4];
														queryengine = args[2];
														String col_name = col_names.replaceAll("[-+.^:']", "");
														String hivequery = ("select" + " " + col_name + " " + "from"
																+ " " + table_name + " " + "where TD_TIME_RANGE(time,"
																+ " " + min_time + "," + max_time + ")" + " " + "LIMIT"
																+ " " + resultlimit);
														if (queryengine.contentEquals("hive") && max_time > min_time) {
															TabularResultfromQuery(hivequery);
														}
														if (queryengine.contentEquals("presto")
																&& max_time > min_time) {
															System.out.println(
																	"Error Message: This account only supports hive query");
														}
													}

												} else {
													if (args.length > 0 && args.length == 13
															&& args[12].contentEquals("crime_report")
															&& args[11].contentEquals("my_database")) {
														if (args[1].contentEquals("-f") && args[3].contentEquals("-e")
																&& args[5].contentEquals("-c")
																&& args[7].contentEquals("-m") && args[9].contains("-M")
																&& args[10].matches("^-?\\d+$")
																&& args[8].matches("^-?\\d+$")) {
															table_name = args[12];
															db_name = args[11];
															max_time = Integer.parseInt(args[10]);
															min_time = Integer.parseInt(args[8]);
															col_names = args[6];
															queryengine = args[4];
															outputformat = args[2];
															String col_name = col_names.replaceAll("[-+.^:']", "");
															String hivequery = ("select" + " " + col_name + " " + "from"
																	+ " " + table_name + " "
																	+ "where TD_TIME_RANGE(time," + " " + min_time + ","
																	+ max_time + ")");
															if (queryengine.contentEquals("hive")
																	&& outputformat.contentEquals("tabular")
																	&& max_time > min_time) {
																TabularResultfromQuery(hivequery);
															}
															if (queryengine.contentEquals("hive")
																	&& outputformat.contentEquals("csv")
																	&& max_time > min_time) {
																CSVResultfromQuery(hivequery);
															}
															if (queryengine.contentEquals("presto")) {
																System.out.println(
																		"Error Message: This account only supports hive query");
															}

														}
													} else {
														System.out.println(
																"Error Message:Incorrect Query Entered, please refer the syntax- "
																		+ "\nquery <-f csv/tabular> <-e hive> <-c 'col1,col2,...'> <-m 1146964391> -<M 1196964391> <100> <DatabaseName> <TableName>"
																		+ "\nExample: query -f csv -e hive -c 'address,crimedescr' -m 1146964391 -M 1196964391 100 my_database crime_report");

													}
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}

	private static String CSVResultfromQuery(String query) {
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
			// Download the result
			td.jobResult(jobId, TDResultFormat.CSV, new Function<InputStream, String>() {
				public String apply(InputStream input) {
					try {
						String result = CharStreams.toString(new InputStreamReader(input));
						PrintWriter pw = null;
						pw = new PrintWriter(new File(csvfileLocation));
						StringBuilder builder = new StringBuilder();
						if (result.isEmpty()) {
							System.out.println("Error Message: Empty Result");
						}
						builder.append(result);
						pw.write(builder.toString());
						pw.close();
						return result;
					} catch (IOException e) {
						throw Throwables.propagate(e);
					}
				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			td.close();
		}
		return query;
	}

	private static String TabularResultfromQuery(String query) {
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
			// Download the result
			td.jobResult(jobId, TDResultFormat.TSV, new Function<InputStream, String>() {
				public String apply(InputStream input) {
					try {
						String result = CharStreams.toString(new InputStreamReader(input));
						PrintWriter pw = null;
						pw = new PrintWriter(new File(textfileLocation));
						StringBuilder builder = new StringBuilder();
						if (result.isEmpty()) {
							System.out.println("Error Message: Empty Result");
						}
						TabularResult tr = new TabularResult();
						builder.append(result);
						pw.write(builder.toString());
						pw.close();
						tr.print(textfileLocation);
						return result;
					} catch (IOException e) {
						throw Throwables.propagate(e);
					}

				}
			});
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			td.close();
		}
		return query;
	}

}
