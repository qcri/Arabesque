#! /usr/bin/env sh

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
echo "DIR=$DIR"
# add another execution engines here
SPARK_EXEC_ENGINE="spark"; SPARK_CMD="$DIR/run_qfrag_spark.sh"
GIRAPH_EXEC_ENGINE="giraph"; GIRAPH_CMD="$DIR/run_qfrag_giraph.sh"

execution_engine=$(echo -n `cat $@ | grep execution_engine | cut -d":" -f2`)

case $execution_engine in
   $SPARK_EXEC_ENGINE)
      echo "Running $SPARK_EXEC_ENGINE execution engine"
      echo "$SPARK_CMD $@"
      exec $SPARK_CMD $@
      ;;

   $GIRAPH_EXEC_ENGINE)
      echo "Running $GIRAPH_EXEC_ENGINE execution engine"
      echo "$GIRAPH_CMD $@"
      $GIRAPH_CMD $@
      ;;

   *)
      echo "Please inform execution_engine in the YAML files"
      ;;
esac
