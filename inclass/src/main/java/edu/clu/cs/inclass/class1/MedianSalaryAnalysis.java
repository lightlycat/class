package edu.clu.cs.inclass.class1;

import java.io.*;

public class MedianSalaryAnalysis {

	/*
	Input data: median salary per zip code across US
	Location of data in the VM: /home/cloudera/dataset/DEC_00_SF3_P077_with_ann.csv
	Format: csv file {id, zip, geography, median income}
	Process the data to print following:
	  1. Poorest/Richest zip code and lowest/highest median income
      2. Percentage of poor, middle-class and rich neighborhoods

      Poor neighborhood is defined as zip code with income less than $35K
	  Middle-class neighborhood is defined as zip code with income more than $35K but less than $100K
      Rich neighborhood is defined as zip code with income more than 100K

	Notes:
	  Ignore zero salaries
	  Ignore where salary cannot be interpreted as a number. E.g. Ignore salary �200,000+� and �2,500-�

	 */

	public static void main(String[] args) throws Exception {
		
		BufferedReader br = new BufferedReader(new FileReader(args[0])); 
		String line;
		int lowestIncome = Integer.MAX_VALUE;
		int highestIncome = Integer.MIN_VALUE;
		String poorestZip = "";
		String richestZip = "";
		int numOfPoorNeighbourhoods = 0;
		int numOfMiddleClassNeighbourhoods = 0;
		int numOfRichNeighbourhoods = 0;

		while ((line = br.readLine()) != null) { 
			String[] fields = line.split(",");
			if(fields.length<4) continue;
			//zip = fields[1]
			//BUGBUG:consider last column as salary
			//salary = fields[fields.length-1]
			try {
				int salary = Integer.parseInt(fields[fields.length-1]);
				if(salary<lowestIncome) {
					lowestIncome = salary;
					poorestZip = fields[1];
				}
				if(salary>highestIncome) {
					highestIncome = salary;
					richestZip = fields[1];
				}
				if(salary<35000) numOfPoorNeighbourhoods++;
				else if(salary>=35000 && salary <100000) numOfMiddleClassNeighbourhoods++;
				else numOfRichNeighbourhoods++;

			} finally {
				//supress error
			}
		} 
		System.out.println("Poorest Zip is "+poorestZip+" with income of "+lowestIncome);
		System.out.println("Richest Zip is "+richestZip+" with income of "+highestIncome);
		int totalNeighbourhoods = numOfPoorNeighbourhoods+numOfMiddleClassNeighbourhoods+numOfRichNeighbourhoods;
		System.out.println("Percentage of poor neighbourhoods are "+
				numOfPoorNeighbourhoods*100f/totalNeighbourhoods);
		System.out.println("Percentage of middle-class neighbourhoods are "+
				numOfMiddleClassNeighbourhoods*100f/totalNeighbourhoods);
		System.out.println("Percentage of rich neighbourhoods are "+
				numOfRichNeighbourhoods*100f/totalNeighbourhoods);

		br.close();
	}

}
