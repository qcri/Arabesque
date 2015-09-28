title: Download Arabesque
project: Arabesque
---

# Github repo

The source code can be download from [Github](http://github/qcri/arabesque.io)

# Precompiled Java package

The jar contains Arabesque with a number of example applications, and the 
modified Giraph.

To submit an Arabesque job, you need the following script (run_arabesque.sh)
```bash
#! /usr/bin/env sh

hdfs dfs -rm -r Output

ARABESQUE_JAR_DIR="`pwd`"
ARABESQUE_JAR=`find $ARABESQUE_JAR_DIR -maxdepth 1 -name "arabesque-1.0-jar-with-dependencies.jar" | head -1`

if [ -z "$ARABESQUE_JAR" ] ; then
  echo "No Arabesque jar found in $ARABESQUE_JAR_DIR. Did you compile it?"
  exit 66
fi

HADOOP_CP=".:${ARABESQUE_JAR}"

HADOOP_CLASSPATH=$HADOOP_CP hadoop jar $ARABESQUE_JAR io.arabesque.ArabesqueRunner -y $@
```

Then you need to define a cluster.yaml and an application.yaml file and run as following.
```bash
./run_arabesque.sh cluster.yaml cliques.yaml
```

Details about how to run and program in Arabesque can be found in the [user guide](user_guide.html) section. 