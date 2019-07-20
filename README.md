
Background
----------
While working  with ORC data, it becomes difficult to maintain schema while
model is still evolving. At the same time creating test data becomes
cumbersome. Issue becomes more severe when model is evolving.
Development team has to go through multiple iteration of phases where model
change triggers change in testing data and whole lot of time has to be invested.

Solution
--------
**OrcUtil** tries to solve such problems.
1. Using annotation we can specify metadata in model class,
for ORC schema and **OrcUtil** will generate ORC schema on the fly.
No need to manually update schema everytime model changes.
For more info, refer test cases in
[OrcSchemaGeneratorSpec](https://github.com/sudat29/orcutil/blob/master/src/test/groovy/sud/indepth/orcutil/OrcSchemaGeneratorSpec.groovy).
2. Unit test data can be easily generated using
[OrcCreator#createOrcStruct](https://github.com/sudat29/orcutil/blob/master/src/main/java/sud/indepth/orcutil/OrcCreator.java#L61).
For more info, refer test cases in
[OrcCreatorSpec](https://github.com/sudat29/orcutil/blob/master/src/test/groovy/sud/indepth/orcutil/OrcCreatorSpec.groovy).

Supported Types
---------------
Currently below [ORC types](https://github.com/apache/hive/blob/master/serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/ObjectInspector.java#L50) are supported:
* <code>PRIMITIVE</code> (<code>INT</code>, <code>DOUBLE</code>, <code>LONG</code> and <code>STRING</code>)
* <code>STRUCT</code>
* <code>LIST</code>
* <code>MAP</code>