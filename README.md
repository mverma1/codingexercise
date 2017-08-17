# codingexercise
Write a command line tool to issue a query on Treasure Data and query a database and table to retrieve the values of a specified set of columns in a specified date/time range.
I created a maven project so all the dependencies will be automatically downloaded on the client
Steps to Run the Program Locally:
<br />Step 1 : Please replace your current td.conf file with the file available on the project

<br />Step 2 : Download the project from GitHub and Import the project in eclipse workplace

<br />Step 3 : Now Open the MyCodingTest.java file and on the top you will see 2 file location, you can change the location accordingly

<br />Step 4 : Now right click on MyCodingTest.class and go to runas-> run configurations

<br />Step 5 : Now on the Arguments Tab user enter inout query
<br />         Sample Query:
<br />        a) query -f csv -e hive -c 'address,crimedescr' -m 1146964391 -M 1196964391 100 my_database crime_report
<br />        b) query my_database crime_report
<br />        c) query -e presto my_database crime_report
<br />        d) query -f tabular 100 my_database crime_report

<br />Step 6 : Please see the downloaded result on the file location entered



