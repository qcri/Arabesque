package io.arabesque.aggregation;

import org.apache.giraph.utils.ReflectionUtils;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Alex on 19-Sep-15.
 */
public class AggregationStorageWrapper<K extends Writable, V extends Writable> implements Writable {
    private AggregationStorage<K, V> aggregationStorage;

    public AggregationStorageWrapper() {
        aggregationStorage = null;
    }

    public AggregationStorageWrapper(AggregationStorage<K, V> aggregationStorage) {
        this.aggregationStorage = aggregationStorage;
    }

    public AggregationStorage<K, V> getAggregationStorage() {
        return aggregationStorage;
    }

    public void aggregate(AggregationStorageWrapper<K, V> otherWrapper) {
        if (otherWrapper.getAggregationStorage() == null) {
            return;
        }

        AggregationStorage<K, V> otherAggregationStorage = otherWrapper.getAggregationStorage();

        if (aggregationStorage == null) {
            aggregationStorage = otherWrapper.getAggregationStorage();
        } else {
            aggregationStorage.aggregate(otherAggregationStorage);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        if (aggregationStorage != null) {
            dataOutput.writeBoolean(true);
            WritableUtils.writeClass(aggregationStorage.getClass(), dataOutput);
            aggregationStorage.write(dataOutput);
        } else {
            dataOutput.writeBoolean(false);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        aggregationStorage = null;

        if (dataInput.readBoolean()) {
            Class<? extends AggregationStorage> aggregationStorageClass = WritableUtils.readClass(dataInput);
            aggregationStorage = ReflectionUtils.newInstance(aggregationStorageClass);
            aggregationStorage.readFields(dataInput);
        }
    }

    public void endedAggregation() {
        if (aggregationStorage != null) {
            aggregationStorage.endedAggregation();
        }
    }

    @Override
    public String toString() {
        return "AggregationStorageWrapper{" +
                "aggregationStorage=" + aggregationStorage +
                '}';
    }
}
