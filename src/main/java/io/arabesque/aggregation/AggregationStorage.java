package io.arabesque.aggregation;

import io.arabesque.aggregation.reductions.ReductionFunction;
import io.arabesque.conf.Configuration;
import org.apache.giraph.utils.UnsafeByteArrayOutputStream;
import org.apache.giraph.utils.UnsafeReusableByteArrayInput;
import org.apache.giraph.utils.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.*;

public class AggregationStorage<K extends Writable, V extends Writable> implements Writable {
    private String name;
    protected Map<K, V> keyValueMap;
    protected Class<K> keyClass;
    protected Class<V> valueClass;
    protected ReductionFunction<V> reductionFunction;
    protected EndAggregationFunction<K, V> endAggregationFunction;

    private UnsafeByteArrayOutputStream reusedOut;
    private UnsafeReusableByteArrayInput reusedIn;

    public AggregationStorage() {
    }

    public AggregationStorage(String name) {
        init(name);
    }

    protected void init(String name) {
        if (keyValueMap == null) {
            keyValueMap = new HashMap<>();
        }

        reset();

        this.name = name;

        AggregationStorageMetadata<K, V> metadata = Configuration.get().getAggregationMetadata(name);

        if (metadata == null) {
            return;
        }

        keyClass = metadata.getKeyClass();
        valueClass = metadata.getValueClass();
        reductionFunction = metadata.getReductionFunction();
        endAggregationFunction = metadata.getEndAggregationFunction();
    }

    public void reset() {
        if (keyValueMap != null) {
            keyValueMap.clear();
        }
    }

    public String getName() {
        return name;
    }

    public int getNumberMappings() {
        return keyValueMap.size();
    }

    public Set<K> getKeys() {
        return Collections.unmodifiableSet(keyValueMap.keySet());
    }

    public Map<K, V> getMapping() {
        return Collections.unmodifiableMap(keyValueMap);
    }

    public K getKey(K key) {
        if (keyValueMap.containsKey(key)) {
            return key;
        } else {
            return null;
        }
    }

    public V getValue(K key) {
        return keyValueMap.get(key);
    }

    public void removeKey(K key) {
        keyValueMap.remove(key);
    }

    public void removeKeys(Set<K> keys) {
        for (K key : keys) {
            removeKey(key);
        }
    }

    // Not thread-safe
    // Watch out if reusing either Key or Value. Copies ARE NOT MADE!!!
    public void aggregate(K key, V value) {
        V myValue = keyValueMap.get(key);

        if (myValue == null) {
            keyValueMap.put(key, value);
        } else {
            keyValueMap.put(key, reductionFunction.reduce(myValue, value));
        }
    }

    public void aggregateWithReusables(K key, V value) {
        V myValue = keyValueMap.get(key);

        if (myValue == null) {
            keyValueMap.put(copyWritable(key), copyWritable(value));
        } else {
            reductionFunction.reduce(myValue, value);
        }
    }

    private <W extends Writable> W copyWritable(W writable) {
        if (reusedOut == null) {
            reusedOut = new UnsafeByteArrayOutputStream();
        }

        if (reusedIn == null) {
            reusedIn = new UnsafeReusableByteArrayInput();
        }

        return WritableUtils.createCopy(reusedOut, reusedIn, writable, null);
    }

    // Thread-safe
    public void finalLocalAggregate(AggregationStorage<K, V> otherStorage) {
        synchronized (this) {
            aggregate(otherStorage);
        }
    }

    // Not thread-safe
    public void aggregate(AggregationStorage<K, V> otherStorage) {
        if (!getName().equals(otherStorage.getName())) {
            throw new RuntimeException("Aggregating storages with different names");
        }

        for (Map.Entry<K, V> otherStorageEntry : otherStorage.keyValueMap.entrySet()) {
            K otherKey = otherStorageEntry.getKey();
            V otherValue = otherStorageEntry.getValue();

            aggregate(otherKey, otherValue);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(name);

        dataOutput.writeInt(keyValueMap.size());

        for (Map.Entry<K, V> entry : keyValueMap.entrySet()) {
            entry.getKey().write(dataOutput);
            entry.getValue().write(dataOutput);
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        reset();

        name = dataInput.readUTF();

        init(name);

        try {
            Constructor<K> keyClassConstructor = keyClass.getConstructor();
            Constructor<V> valueClassConstructor = valueClass.getConstructor();

            int numEntries = dataInput.readInt();

            for (int i = 0; i < numEntries; ++i) {
                K key = keyClassConstructor.newInstance();

                key.readFields(dataInput);

                V value = valueClassConstructor.newInstance();

                value.readFields(dataInput);

                keyValueMap.put(key, value);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading aggregation storage", e);
        }

    }

    public void endedAggregation() {
        if (endAggregationFunction != null) {
            endAggregationFunction.endAggregation(this);
        }
    }

    public void transferKeyFrom(K key, AggregationStorage<K, V> otherAggregationStorage) {
        aggregate(key, otherAggregationStorage.getValue(key));
        otherAggregationStorage.removeKey(key);
    }

    @Override
    public String toString() {
        return "AggregationStorage{" +
                "name='" + name + '\'' +
                ", keyValueMap=" + keyValueMap +
                '}';
    }

    public String toOutputString() {
        StringBuilder strBuilder = new StringBuilder();

        ArrayList<K> keys = new ArrayList<>(keyValueMap.keySet());

        if (WritableComparable.class.isAssignableFrom(keyClass)) {
            ArrayList<? extends WritableComparable> orderedKeys = (ArrayList<? extends WritableComparable>) keys;
            Collections.sort(orderedKeys);
        }

        for (K key : keys) {
            strBuilder.append(key.toString());
            strBuilder.append(": ");
            strBuilder.append(keyValueMap.get(key));
            strBuilder.append('\n');
        }

        return strBuilder.toString();
    }

    public boolean containsKey(K key) {
        return keyValueMap.containsKey(key);
    }
}
