package mytest;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class TabularResult {
	public void print(String path) throws IOException {
		BufferedReader reader = new BufferedReader(new FileReader(path));
		String line;
		
		while ((line = reader.readLine()) != null) {
			String [] words =  line.split("\t");
			if(words.length==9) {
			String s = String.format("|%35s|%5s|%36s|%36s|%35s|%20s|%20s|%35s|%20s|	\n", words[0], words[1], words[2],
					words[3],words[4],words[5],words[6],words[7],words[8] );	
			System.out.print(s);
			}
			if(words.length==8) {
				String s = String.format("|%35s|%35s|%36s|%36s|%35s|%20s|%20s|%35s|	\n", words[0], words[1], words[2],
						words[3],words[4],words[5],words[6],words[7] );	
				System.out.print(s);
				}
			if(words.length==7) {
				String s = String.format("|%35s|%35s|%36s|%36s|%35s|%35s|%35s|  \n", words[0], words[1], words[2],
						words[3],words[4],words[5],words[6]);	
				System.out.print(s);
				}
			if(words.length==6) {
				String s = String.format("|%35s|%35s|%36s|%36s|%35s|%35s|	\n", words[0], words[1], words[2],
						words[3],words[4],words[5] );	
				System.out.print(s);
				}
			if(words.length==5) {
				String s = String.format("|%36s|%35s|%36s|%36s|%35s|	\n", words[0], words[1], words[2],
						words[3],words[4] );	
				System.out.print(s);
				}
			if(words.length==4) {
				String s = String.format("|%36s|%35s|%36s|%36s|	\n", words[0], words[1], words[2],
						words[3] );	
				System.out.print(s);
				}
			if(words.length==3) {
				String s = String.format("|%36s|%36s|%36s|	\n", words[0], words[1], words[2]);	
				System.out.print(s);
				}
			if(words.length==2) {
				String s = String.format("|%36s|%36s|	\n", words[0], words[1]);	
				System.out.print(s);
				}
			if(words.length==1) {
				String s = String.format("|%36s|	\n", words[0]);	
				System.out.print(s);
				}
			if(words.length==0) {
				System.out.print("Input File is Empty");
				}
		}
		reader.close();
				
	}
}
