REGISTER piggybank.jar;
REGISTER S12P2Filter.jar;
DEFINE filterRecords com.FilterUDF;
DEFINE XPath org.apache.pig.piggybank.evaluation.xml.XPath();

-- Command to load XML data

xmlData =  LOAD 'hdfs://localhost:9000/user/flume_import/StatewiseDistrictwisePhysicalProgress.xml' using org.apache.pig.piggybank.storage.XMLLoader('row') as (x:chararray);

-- Command to get the required columns using Xpath

xmlData_columns = FOREACH xmlData GENERATE XPath(x, 'row/Project_Performance-IHHL_BPL'), XPath(x, 'row/Project_Objectives_IHHL_BPL'), XPath(x, 'row/District_Name');

-- Command to remove the rows where performed number is greater than objective number

xmldata_filter = FOREACH xmlData_columns GENERATE filterRecords(*) as (x:chararray);

-- Command to null records which filtered in the above operation

filteredData = FILTER xmldata_filter by NOT(x is NULL);

-- Command to save the output into a csv file

STORE filteredData INTO 'hdfs://localhost:9000/user/Project2/Problem2Output/' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','WINDOWS');
