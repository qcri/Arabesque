package io.arabesque.aggregation;

import io.arabesque.pattern.Pattern;
import com.koloboke.collect.map.hash.HashObjByteMap;
import com.koloboke.collect.map.hash.HashObjByteMaps;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;

import java.io.*;
import java.lang.reflect.Constructor;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class PatternAggregationStorage<K extends Pattern, V extends Writable> extends AggregationStorage<K, V> {
    private static final Logger LOG = Logger.getLogger(PatternAggregationStorage.class);

    private final HashObjByteMap<K> reservations;
    private final ConcurrentHashMap<K, K> quick2CanonicalMap;
    private final ConcurrentHashMap<K, ConcurrentHashMap<K,Boolean>> canonical2quickMap; // Fake map, should be set,
                                                                                         // just to be safe for concurrency.

    public PatternAggregationStorage() {
        this(null);
    }

    public PatternAggregationStorage(String name) {
        super(name);
        reservations = HashObjByteMaps.getDefaultFactory().withDefaultValue((byte) 0).newMutableMap();
        quick2CanonicalMap = new ConcurrentHashMap<>();
        canonical2quickMap = new ConcurrentHashMap<>();
    }

    @Override
    public void reset() {
        super.reset();

        if (reservations != null) {
            reservations.clear();
        }

        if (quick2CanonicalMap != null) {
            quick2CanonicalMap.clear();
            canonical2quickMap.clear();
        }
    }

    @Override
    public K getKey(K key) {
        K superKey = super.getKey(key);

        if (superKey == null) {
            superKey = quick2CanonicalMap.get(key);
        }

        return superKey;
    }

    @Override
    public V getValue(K key) {
        V value = super.getValue(key);

        // If we didn't find a value, key might be a non-canonical pattern. If we have
        // quick2CanonicalMappings, we can attempt to translate the request to the canonical
        // pattern
        if (value == null && !quick2CanonicalMap.isEmpty()) {
            K canonical = quick2CanonicalMap.get(key);

            if (canonical != null) {
                value = super.getValue(canonical);
            }
        }

        return value;
    }

    //TODO: USE THE INDEX TO REMOVE AND CLEAN.
//    @Override
//    public void removeKey(K key) {
//        super.removeKey(key);
//
//        // If quick2Canonical is not empty then we need to clean it up.
//        // Key may represent a quick or canonical pattern.
//        // We need to clean all keys and values matching it.
//        if (!quick2CanonicalMap.isEmpty()) {
//            Iterator<Map.Entry<K, K>> quick2CanonicalIterator = quick2CanonicalMap.entrySet().iterator();
//
//            while (quick2CanonicalIterator.hasNext()) {
//                Map.Entry<K, K> entry = quick2CanonicalIterator.next();
//
//                if (entry.getKey().equals(key) || entry.getValue().equals(key)) {
//                    quick2CanonicalIterator.remove();
//                }
//            }
//        }
//    }

    @Override
    public void removeKeys(Set<K> keys) {
        // If quick2Canonical is not empty then we need to clean it up.
        // Key may represent a quick or canonical pattern.
        // We need to clean all keys and values matching it.

        for (K key : keys) {
            super.removeKey(key);
            quick2CanonicalMap.remove(key);
            ConcurrentHashMap<K, Boolean> quickSet = canonical2quickMap.get(key);
            if (quickSet!=null){
                for (K key2:quickSet.keySet()){
                    quick2CanonicalMap.remove(key2);
                }
            }
        }


//        if (!quick2CanonicalMap.isEmpty()) {
//            Iterator<Map.Entry<K, K>> quick2CanonicalIterator = quick2CanonicalMap.entrySet().iterator();
//
//            while (quick2CanonicalIterator.hasNext()) {
//                Map.Entry<K, K> entry = quick2CanonicalIterator.next();
//
//                K quickPattern = entry.getKey();
//                K canonicalPattern = entry.getValue();
//
//                if (keys.contains(quickPattern) || keys.contains(canonicalPattern)) {
//                    quick2CanonicalIterator.remove();
//                }
//            }
//        }
    }

    @Override
    // Threadsafe
    public void finalLocalAggregate(AggregationStorage<K, V> otherStorage) {
        while (!otherStorage.keyValueMap.isEmpty()) {
            Iterator<Map.Entry<K, V>> entryIterator = otherStorage.keyValueMap.entrySet().iterator();

            while (entryIterator.hasNext()) {
                Map.Entry<K, V> entry = entryIterator.next();

                if (canonicalAggregate(entry.getKey(), entry.getValue())) {
                    entryIterator.remove();
                }
            }

            // Thread sleep for simplicity, Because otherwise we need to implement a signal method to know when
            // the processing of the other thread was finished. Is called once per thread, if another thread is
            // computing the canonical pattern.
            if (!otherStorage.keyValueMap.isEmpty()) {
                try {
                    System.out.println("Sleeping ...");
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void aggregate(AggregationStorage<K, V> otherStorage) {
        if (!(otherStorage instanceof PatternAggregationStorage)) {
            throw new RuntimeException("This should never happen");
        }

        super.aggregate(otherStorage);

        PatternAggregationStorage<K, V> otherPatternStorage = (PatternAggregationStorage<K, V>) otherStorage;

        for (Map.Entry<K, K> otherQuick2CanonicalMapEntry : otherPatternStorage.quick2CanonicalMap.entrySet()) {
            K quickPattern = otherQuick2CanonicalMapEntry.getKey();
            K canonicalPattern = otherQuick2CanonicalMapEntry.getValue();

            quick2CanonicalMap.putIfAbsent(quickPattern, canonicalPattern);

            // Should i do the reverse also???
            ConcurrentHashMap<K, Boolean> t = canonical2quickMap.get(canonicalPattern);
            if (t == null){
                ConcurrentHashMap<K,Boolean> one = new ConcurrentHashMap<>();
                one.put(quickPattern,true);
                canonical2quickMap.put(canonicalPattern,one);
            }
            else {
                t.put(quickPattern,true);
            }
        }
    }


    //TODO: WHY I DON'T WRITE THE CANONICAL2QUICK?
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);

        dataOutput.writeInt(quick2CanonicalMap.size());

        for (Map.Entry<K, K> quick2CanonicalEntry : quick2CanonicalMap.entrySet()) {
            quick2CanonicalEntry.getKey().write(dataOutput);
            quick2CanonicalEntry.getValue().write(dataOutput);
        }

        dataOutput.writeInt(canonical2quickMap.size());

        for (Map.Entry<K, ConcurrentHashMap<K, Boolean>> canonical2QuickEntry : canonical2quickMap.entrySet()) {
            canonical2QuickEntry.getKey().write(dataOutput);
            dataOutput.writeInt(canonical2QuickEntry.getValue().size());
            for (K one: canonical2QuickEntry.getValue().keySet()){
                one.write(dataOutput);
            }
        }
    }

    @Override
    public void writeExternal(ObjectOutput objOutput) throws IOException {
        super.writeExternal (objOutput);

        objOutput.writeInt(quick2CanonicalMap.size());

        for (Map.Entry<K, K> quick2CanonicalEntry : quick2CanonicalMap.entrySet()) {
            quick2CanonicalEntry.getKey().write(objOutput);
            quick2CanonicalEntry.getValue().write(objOutput);
        }

        objOutput.writeInt(canonical2quickMap.size());

        for (Map.Entry<K, ConcurrentHashMap<K, Boolean>> canonical2QuickEntry : canonical2quickMap.entrySet()) {
            canonical2QuickEntry.getKey().write(objOutput);
            objOutput.writeInt(canonical2QuickEntry.getValue().size());
            for (K one: canonical2QuickEntry.getValue().keySet()){
                one.write(objOutput);
            }
        }
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);

        try {
            Constructor<K> keyClassConstructor = keyClass.getConstructor();

            int sizeQuick2CanonicalMap = dataInput.readInt();

            for (int i = 0; i < sizeQuick2CanonicalMap; ++i) {
                K quick = keyClassConstructor.newInstance();
                quick.readFields(dataInput);

                K canonical = keyClassConstructor.newInstance();
                canonical.readFields(dataInput);

                quick2CanonicalMap.put(quick, canonical);
            }

            // Next read the canonical2quick.
            int sizeB = dataInput.readInt();

            for (int i = 0; i < sizeB; ++i) {
                K canonical = keyClassConstructor.newInstance();
                canonical.readFields(dataInput);

                int sizeBB = dataInput.readInt();
                ConcurrentHashMap<K,Boolean> tmp = new ConcurrentHashMap<>(sizeBB);

                for (int j = 0; j < sizeBB;++j) {
                    K quick = keyClassConstructor.newInstance();
                    quick.readFields(dataInput);
                    tmp.put(quick,true);
                }
                canonical2quickMap.put(canonical,tmp);
            }

        } catch (Exception e) {
            throw new RuntimeException("Error reading quick2canonical mapping", e);
        }
    }

    @Override
    public void readExternal(ObjectInput objInput) throws IOException, ClassNotFoundException {
        super.readExternal (objInput);
        try {
            Constructor<K> keyClassConstructor = keyClass.getConstructor();

            int sizeQuick2CanonicalMap = objInput.readInt();

            for (int i = 0; i < sizeQuick2CanonicalMap; ++i) {
                K quick = keyClassConstructor.newInstance();
                quick.readFields(objInput);

                K canonical = keyClassConstructor.newInstance();
                canonical.readFields(objInput);

                quick2CanonicalMap.put(quick, canonical);
            }

            // Next read the canonical2quick.
            int sizeB = objInput.readInt();

            for (int i = 0; i < sizeB; ++i) {
                K canonical = keyClassConstructor.newInstance();
                canonical.readFields(objInput);

                int sizeBB = objInput.readInt();
                ConcurrentHashMap<K,Boolean> tmp = new ConcurrentHashMap<>(sizeBB);

                for (int j = 0; j < sizeBB;++j) {
                    K quick = keyClassConstructor.newInstance();
                    quick.readFields(objInput);
                    tmp.put(quick,true);
                }
                canonical2quickMap.put(canonical,tmp);
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading quick2canonical mapping", e);
        }
    }

    // Thread-safe
    private boolean canonicalAggregate(K quickPattern, V value) {
        K canonicalPattern = getCanonicalPattern(quickPattern);

        if (canonicalPattern == null) {
            return false;
        }

        if (value instanceof PatternAggregationAwareValue) {
            PatternAggregationAwareValue patternAggregationAwareValue = (PatternAggregationAwareValue) value;

            patternAggregationAwareValue.handleConversionFromQuickToCanonical(quickPattern, canonicalPattern);
        }

        synchronized (this) {
            aggregate(canonicalPattern, value);
        }

        return true;
    }

    // Thread-safe
    private K getCanonicalPattern(K quickPattern) {
        K canonicalPattern = quick2CanonicalMap.get(quickPattern);

        if (canonicalPattern == null) {
            byte currentReservation;

            synchronized (reservations) {
                currentReservation = reservations.getByte(quickPattern);

                if (currentReservation != 0) {
                    return null;
                } else {
                    reservations.put(quickPattern, (byte) 1);
                }

                //LOG.info("Quick 2 canonical map: ");
                //LOG.info(quick2CanonicalMap);

                //LOG.info("Reservations: ");
                //LOG.info(reservations);
            }

            //LOG.info("Calculate canonical pattern of quick pattern: " + quickPattern);
            canonicalPattern = (K) quickPattern.copy();
            canonicalPattern.turnCanonical();
            //LOG.info("Canonical pattern: " + canonicalPattern);

            quick2CanonicalMap.put(quickPattern, canonicalPattern);

            add_to_canonical2Quick(canonicalPattern,quickPattern);
        }

        return canonicalPattern;
    }

    private void add_to_canonical2Quick(K canonicalPattern, K quickPattern) {
        // Next, populate the inverse...
        ConcurrentHashMap<K, Boolean> canonicalHM = canonical2quickMap.get(canonicalPattern);
        if (canonicalHM==null){
            canonical2quickMap.putIfAbsent(canonicalPattern, new ConcurrentHashMap<K, Boolean>());
            canonicalHM = canonical2quickMap.get(canonicalPattern);
        }
        canonicalHM.put(quickPattern,Boolean.TRUE);
    }

    @Override
    /**
     * Create the maps based on the information of the otherAggregationStorage. Basically, trying to split-up the
     * aggregation by canonical patterns.
     * TODO: Is this really necessary? Probably we don't have to do either way.
     * TODO: SHOULD THE KEYS BE COPIED?
     */
    public void transferKeyFrom(K key, AggregationStorage<K, V> otherAggregationStorage) {
        if (otherAggregationStorage instanceof PatternAggregationStorage) {
            PatternAggregationStorage<K, V> otherPatternAggStorage = (PatternAggregationStorage<K, V>) otherAggregationStorage;

            ConcurrentHashMap<K, Boolean> quickPatterns = otherPatternAggStorage.canonical2quickMap.get(key);

            if (quickPatterns==null){
                throw new RuntimeException("Empty quicks?");
            }
//            K canonical_copy = (K) key.copy();
            canonical2quickMap.put(key, new ConcurrentHashMap<K,Boolean>(quickPatterns.size()));

            for (K entry: quickPatterns.keySet()){
//                final K copied = (K) entry.copy();
                quick2CanonicalMap.put(entry, key);
                canonical2quickMap.get(key).put(entry,Boolean.TRUE);
            }
        }

        super.transferKeyFrom(key, otherAggregationStorage);
    }

//    @Override
//    public void transferKeyFrom_original(K key, AggregationStorage<K, V> otherAggregationStorage) {
//        if (otherAggregationStorage instanceof PatternAggregationStorage) {
//            PatternAggregationStorage<K, V> otherPatternAggStorage = (PatternAggregationStorage<K, V>) otherAggregationStorage;
//
//            for (Map.Entry<K, K> quick2CanonicalEntry : otherPatternAggStorage.quick2CanonicalMap.entrySet()) {
//                K quickPattern = quick2CanonicalEntry.getKey();
//                K canonicalPattern = quick2CanonicalEntry.getValue();
//
//                if (canonicalPattern.equals(key)) {
//                    quick2CanonicalMap.put(quickPattern, canonicalPattern);
//                }
//            }
//        }
//
//        super.transferKeyFrom(key, otherAggregationStorage);
//    }

    public K getCanonicalPatternValueOnly (K quick){
        return quick2CanonicalMap.get(quick);
    }

    @Override
    public String toString() {
        return "PatternAggregationStorage{" +
                "quick2CanonicalMap=" + quick2CanonicalMap +
                "} " + super.toString();
    }

    @Override
    public boolean containsKey(K key) {
        // Try finding in normal mapping
        boolean result = super.containsKey(key);

        if (result) {
            return true;
        }

        // If we didn't find in normal mapping, key might be quick and
        // normal mapping might only have canonicals. Lets do the translation
        if (!quick2CanonicalMap.isEmpty()) {
            K canonical = quick2CanonicalMap.get(key);

            if (canonical != null) {
                result = super.containsKey(canonical);

                return result;
            }
        }

        return false;
    }
}
