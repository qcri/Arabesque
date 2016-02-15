#! /usr/bin/env sh

ARABESQUE_JAR_DIR="`pwd`"
ARABESQUE_JAR=`find $ARABESQUE_JAR_DIR -maxdepth 1 -name "arabesque-*-jar-with-dependencies.jar" | head -1`

if [ -z "$ARABESQUE_JAR" ] ; then
  echo "No Arabesque jar found in $ARABESQUE_JAR_DIR. Did you compile it?"
  exit 66
fi

spark-submit --class io.arabesque.ArabesqueRunner $ARABESQUE_JAR -y $@
