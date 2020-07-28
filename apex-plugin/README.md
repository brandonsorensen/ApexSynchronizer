# Apex Learning Plugin

This  plugin defines four custom SQL queries (known as _PowerQueries_ in the
PowerSchool ecosystem) that collectively return all information necessary for
the `ApexSynchronizer` module. It consists of two principle components:
[plugin.xml](plugin.xml) and the [queries_root](queries_root) directory.

## plugin.xml

This file defines the plugin's metadata and outlines the data sources that will
be called in by the queries. There must be an XML element in this file for any
column that is referenced in a query. Otherwise, that query will fail with a
permissions error.

## queries_root

This directory contains the files in which the PowerQueries are defined. Given
the limited scope of this project, there is only one file. Each XML file should
roughly adhere to the following sketch.

~~~~xml 
<queries>
  <query name="query name"
         coreTable="OPTIONAL_SINGLE_TABLE"
         flattened="whether to return a flat JSON object or one indexed on tables">
	<description>Plugin description</description>
    <args> Optional arguments to pass to the PowerQuery </args>
    <columns>
      <column column="Table1.Column1">ALIAS_COL1</column>
      <column column="Table2.Column3">ALIAS_COL3</column>
    </columns>
    <sql> SELECT Table1.Column, Table2.Column3
      FROM FANCY_SQL_QUERY
      <!--DO NOT CLOSE WITH A SEMICOLON!-->
    </sql>
  </query>
</queries>
~~~~

Four such queries are defined in the [named_queries](queries_root/com.apex.learning.data_export.named_queries.xml) XML file.

- `com.apex.learning.school.students` returns all information necessary to
  create an `ApexStudent` object for each student in the PowerSchool database.
- `com.apex.learning.school.classrooms` returns all information necessary to
  create an `ApexClassroom` object for each _section_ in the PowerSchool
  database.
- `com.apex.learning.school.teachers` returns all information necessary to
  create an `ApexStaffMember` object 
- `com.apex.learning.school.enrollment` returns the student ID and section ID 
  pair for each section in which an _active_ student is enrolled. If, for 
  example, student John Dewey with ID 1234 were enrolled in two classes for 
  the current semester, two JSON objects might be returns of the format:
  `{"EDUID": 1234, "SECTION_ID": 10}` and `{"EDUID": 1234, "SECTION_ID": 11}`.
  This is useful when syncing enrollment information.

Note: After rummaging around the internals of the PowerSchool database I had on
hand as well as some ~~tedious~~ experimentation, it seems that PowerQueries
are based on MySQL. However, some MySQL keywords and functions do not
validate and others throw runtime errors. Stick to a basic and SQL-universal
syntax and feature set and if possible!
