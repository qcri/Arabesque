package io.arabesque.testutils

class ListUtils {
    static <O> List<O> getRandomSample(List<O> collection, int sampleSize) {
        Collections.shuffle(collection)
        return collection.take(sampleSize)
    }
}
