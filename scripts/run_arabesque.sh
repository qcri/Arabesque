#! /usr/bin/env sh

#hdfs dfs -rm -r Output

ARABESQUE_JAR_DIR="`pwd`"
ARABESQUE_JAR=`find $ARABESQUE_JAR_DIR -maxdepth 1 -name "arabesque-*-jar-with-dependencies.jar" | head -1`

if [ -z "$ARABESQUE_JAR" ] ; then
  echo "No Arabesque jar found in $ARABESQUE_JAR_DIR. Did you compile it?"
  exit 66
fi

HADOOP_CP=".:${ARABESQUE_JAR}"

HADOOP_CLASSPATH=$HADOOP_CP hadoop jar $ARABESQUE_JAR io.arabesque.ArabesqueRunner -y $@
