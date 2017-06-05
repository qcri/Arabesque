#! /usr/bin/env sh

# garantees that the environment knows Spark
if [ -z "$SPARK_HOME" ]; then
   echo "Please, make sure SPARK_HOME is properly set"
   exit
fi

echo "Spark installation: SPARK_HOME=$SPARK_HOME"

# configuration files in spark must be read from $SPARK_HOME/conf
# we aggregate the configs passed by the user in one sigle temporary file
tempfile=$(mktemp $SPARK_HOME/conf/arabesque-yaml.XXXXXX)
for config_file in "$@"; do
   cat $config_file >> $tempfile
done

echo ""
echo "The aggregated config passed by the user:"
echo "========================================="
cat $tempfile
echo ""

# extract the driver memory
driverMemory=1g
maxResultSize=1g

while IFS='' read -r line || [ -n "$line" ]; do
	if echo "$line" | grep 'driver_memory'; then
		driverMemory=${line#*: }
	fi
	if echo "$line" | grep 'max_result_size'; then
		maxResultSize=${line#*: }
	fi
done < "$tempfile"

# arabesque executable
ARABESQUE_JAR_DIR="`pwd`"
ARABESQUE_JAR=`find $ARABESQUE_JAR_DIR -maxdepth 1 -name "arabesque-*-jar-with-dependencies.jar" | head -1`

if [ -z "$ARABESQUE_JAR" ] ; then
  echo "No Arabesque jar found in $ARABESQUE_JAR_DIR. Did you compile it?"
  exit 66
fi

# submit the application to spark cluster
$SPARK_HOME/bin/spark-submit --driver-memory $driverMemory --conf spark.driver.maxResultSize=$maxResultSize --verbose --class io.arabesque.ArabesqueRunner $ARABESQUE_JAR -y $(basename $tempfile)

# remove the tempfile
rm $tempfile
