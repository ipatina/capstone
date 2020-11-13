## Capstone project

### Task 1
`DataframeAttributeProjection` - implementation of Task 1 projection by Spark SQL functions.

`AggregatorAttributeProjection` - implementation of Task 1 projection by a custom Aggregator.


### Task 2
`SqlMarketingStatistics` - implementation of Task 2 aggregations using plain SQL.

`DataframeMarketingStatistics` - implementation of Task 2 aggregations using Spark SQL functions.


### Build jar
`mvn clean package` or without running tests `mvn clean package -DskipTests=true`


### Run
The application uses spark version 2.4.7. 
Run spark-submit with option "spark.capstone.action.type=build_projection" to get results for Task 1, example:
`spark-submit --master local --conf "spark.capstone.config.path=/full/path/to/config.yaml" --conf "spark.capstone.action.type=build_projection" path/to/capstone-1.0-SNAPSHOT.jar`
Run spark-submit with option "spark.capstone.action.type=build_statistics" to get results for Task 2, example:
`spark-submit --master local --conf "spark.capstone.config.path=/full/path/to/config.yaml" --conf "spark.capstone.action.type=build_projection" path/to/capstone-1.0-SNAPSHOT.jar`

Required configurations:
`spark.capstone.config.path` - path config.yaml, see the example in src/main/resources/config.yaml
`spark.capstone.action.type` - specify whether build Dataframe for Task 1 or Task 2. For Task 1 put value `build_projection`, for Task 2 - `build_statistics`.

Optional configurations:
`spark.capstone.action.implementation` - specify `aggregator`to use `AggregatorAttributeProjection` for Task 1, by default `DataframeAttributeProjection` will be used. Specify `sql` to use `SqlMarketingStatistics` for Task 2, by default `DataframeMarketingStatistics` will be used.


### Run tests
Run maven command: `mvn clean test` to execute tests.
