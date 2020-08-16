# Project-Phase2

### Hot zone analysis

We perform a range join operation on a rectangle datasets and a point dataset. For each rectangle, the number of points located within the rectangle will be obtained. The hotter rectangle means that it include more points. So this task is to calculate the hotness of all the rectangles. 

### Hot cell analysis

This task will focus on applying spatial statistics to spatio-temporal big data in order to identify statistically significant spatial hot spots using Apache Spark. The topic of this task is from ACM SIGSPATIAL GISCUP 2016.

The Problem Definition page is here: [http://sigspatial2016.sigspatial.org/giscup2016/problem](http://sigspatial2016.sigspatial.org/giscup2016/problem) 

The Submit Format page is here: [http://sigspatial2016.sigspatial.org/giscup2016/submit](http://sigspatial2016.sigspatial.org/giscup2016/submit)

Note that: We clip the source data to an envelope (latitude 40.5N – 40.9N, longitude 73.7W – 74.25W) encompassing the New York City in order to remove some of the noisy error data.

#### Special requirement (different from GIS CUP)
As stated in the Problem Definition page, in this task, we implement a Spark program to calculate the Getis-Ord statistic of NYC Taxi Trip datasets. We call it "**Hot cell analysis**"

To reduce the computation power need，we made the following changes:

1. The input will be a monthly taxi trip dataset from 2009 - 2012. For example, "yellow\_tripdata\_2009-01\_point.csv", "yellow\_tripdata\_2010-02\_point.csv".
2. Each cell unit size is 0.01 * 0.01 in terms of latitude and longitude degrees.
3. We use 1 day as the Time Step size. The first day of a month is step 1. Every month has 31 days.
4. We only consider Pick-up Location.
5. We don't use Jaccard similarity to check your answer.

## Coding template specification

### Input parameters

1. Output path (Mandatory)
2. Task name: "hotzoneanalysis" or "hotcellanalysis"
3. Task parameters: (1) Hot zone (2 parameters): nyc taxi data path, zone path(2) Hot cell (1 parameter): nyc taxi data path

Example
```
test/output hotzoneanalysis src/resources/point-hotzone.csv src/resources/zone-hotzone.csv hotcellanalysis src/resources/yellow_trip_sample_100000.csv
```

Note: 

1. The number/order of tasks do not matter.
2. But, the first 7 of our final test cases on Vocareum will be hot zone analysis, the last 8 will be hot cell analysis.



### Input data format
The main function/entrace is "cse512.Entrance" scala file.

1. Point data: the input point dataset is the pickup point of New York Taxi trip datasets. The data format of this phase is the original format of NYC taxi trip which is different from Phase 2. But the coding template already parsed it for you. Find the data from our asu google drive shared folder: [https://drive.google.com/open?id=1bN-U4nknvN5p7jiVHO-wduM7oXR5CBji](https://drive.google.com/open?id=1bN-U4nknvN5p7jiVHO-wduM7oXR5CBji)

2. Zone data (only for hot zone analysis): at "src/resources/zone-hotzone" of the template

#### Hot zone analysis
The input point data can be any small subset of NYC taxi dataset.

#### Hot cell analysis
The input point data is a monthly NYC taxi trip dataset (2009-2012) like "yellow\_tripdata\_2009-01\_point.csv"

### Output data format

#### Hot zone analysis
All zones with their count, sorted by "rectangle" string in an ascending order. 

```
"-73.795658,40.743334,-73.753772,40.779114",1
"-73.797297,40.738291,-73.775740,40.770411",1
"-73.832707,40.620010,-73.746541,40.665414",20
```


#### Hot cell analysis
The coordinates of top 50 hotest cells sorted by their G score in a descending order.

```
-7399,4075,15
-7399,4075,29
-7399,4075,22
```
### Example answers
An example input and answer are put in "testcase" folder of the coding template
