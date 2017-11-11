REGISTER piggybank.jar

DEFINE XPath org.apache.pig.piggybank.evaluation.xml.XPath();

-- Command to load XML data

xmlData =  LOAD 'hdfs://localhost:9000/user/flume_import/StatewiseDistrictwisePhysicalProgress.xml' using org.apache.pig.piggybank.storage.XMLLoader('row') as (x:chararray);

-- Command to get the required columns using Xpath

xmlData_columns = FOREACH xmlData GENERATE XPath(x, 'row/District_Name'),(int) XPath(x, 'row/Project_Objectives_IHHL_BPL'),(int) XPath(x, 'row/Project_Performance-IHHL_BPL');

-- Command to remove the rows where performed number is greater than objective number

xmldata_filter = FILTER xmlData_columns by $2 >= $1 ;

-- Command to save the output into a csv file

STORE xmldata_filter INTO 'hdfs://localhost:9000/user/Project2/Problem1Output1/' USING org.apache.pig.piggybank.storage.CSVExcelStorage(',','NO_MULTILINE','WINDOWS');
