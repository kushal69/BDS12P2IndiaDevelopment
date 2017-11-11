package com;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class FilterUDF extends EvalFunc<String> {
	public String exec(Tuple input) throws IOException {
		try {
			String formatedOutput = "";
			String record = input.toString();
			String [] columns = record.split(",");
			String formatedValue = columns[0].substring(1, columns[0].length());
			double bplObj = Double.parseDouble(columns[1]);
			double bplPerf = Double.parseDouble(formatedValue);
			double result = ((bplPerf * 100) / bplObj);
			if ( result >= 80) {
				formatedOutput = columns[2].substring(0, (columns[2].length())-1)
						+","+bplPerf+","+bplObj+","+result;
				return formatedOutput;
			} else {
				return null;
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}
}
