# RandomWalk-TimeSeriesGenerator
Random Walk Time Series Generator with Spark

This is a generator, where a random number is drawn from a Gaussian distribution N(0,1), then at each time point a new number is drawn from this distribution and added to the value of the last number.

![Alt text] (https://github.com/lev-a/RandomWalk-tsGenerator/blob/master/randomWalkTS.png)

## Building 
The code is presented as a Maven-built project. An executable jar with all dependencies can be built with the following command:

    mvn clean package

## Usage 
To generate Time Series presented as **Object files** or **CSV files**  use the command with the `Options` required:
<pre>

<path_to_spark_bin>/spark-submit --class fr.inria.zenith.DataGenerator  <path_to_jar>/tsGen-scala-1.0-SNAPSHOT.jar [options]

 
  Options:

  --numFiles       [Int]        Total number of files to generate
  --tsNum          [Int]        Total numbers of lines (Time Series)
  --tsSize         [Int]        Size of Time Series
  --outputPath     [Text]       Path to output file
  --csv            [Boolean]    Output as Text files with delimiter ","  


