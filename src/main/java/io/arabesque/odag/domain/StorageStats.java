package io.arabesque.odag.domain;

public class StorageStats implements StorageStatsInt {
    public int minDomainSize = Integer.MAX_VALUE;
    public int maxDomainSize = 0;
    public long sumDomainSize = 0;
    public long numDomains = 0;
    public int minPointersSize = Integer.MAX_VALUE;
    public int maxPointersSize = 0;
    public long sumPointersSize = 0;
    public long sumWastedPointers = 0;

    public void aggregate(StorageStatsInt other) {
        if (!(other instanceof StorageStats)) {
            return;
        }

        StorageStats otherDomainStorageStats = (StorageStats) other;

        minDomainSize = Math.min(minDomainSize, otherDomainStorageStats.minDomainSize);
        maxDomainSize = Math.max(maxDomainSize, otherDomainStorageStats.maxDomainSize);
        sumDomainSize += otherDomainStorageStats.sumDomainSize;
        numDomains += otherDomainStorageStats.numDomains;
        minPointersSize = Math.min(minPointersSize, otherDomainStorageStats.minPointersSize);
        maxPointersSize = Math.max(maxPointersSize, otherDomainStorageStats.maxPointersSize);
        sumPointersSize += otherDomainStorageStats.sumPointersSize;
        sumWastedPointers += otherDomainStorageStats.sumWastedPointers;
    }

    public String getSizeEstimations() {
        long sizeDomainEntriesMap = numDomains * 30;
        long sizeDomainEntries = sumDomainSize * (8 + 30);
        long sizePointers = sumPointersSize * 4;
        long sizeWastedPointers = sumWastedPointers * 4;

        long total = sizeDomainEntriesMap + sizeDomainEntries + sizePointers + sizeWastedPointers;
        long totalIfTight = sizeDomainEntriesMap + sizeDomainEntries + sizePointers;

        return "Sizes{total=" + total +
                ", totalIfTight=" + totalIfTight +
                ", sizeDomainEntriesMap=" + sizeDomainEntriesMap +
                ", sizeDomainEntries=" + sizeDomainEntries +
                ", sizePointers=" + sizePointers +
                ", sizeWastedPointers=" + sizeWastedPointers +
                "}";
    }

    @Override
    public String toString() {
        return "Stats{" +
                "maxDomainSize=" + maxDomainSize +
                ", minDomainSize=" + minDomainSize +
                ", sumDomainSize=" + sumDomainSize +
                ", avgDomainSize=" + (((double) sumDomainSize) / numDomains) +
                ", numDomains=" + numDomains +
                ", minPointersSize=" + minPointersSize +
                ", maxPointersSize=" + maxPointersSize +
                ", avgPointersSize=" + (((double) sumPointersSize) / sumDomainSize) +
                ", sumPointersSize=" + sumPointersSize +
                ", sumWastedPointers=" + sumWastedPointers +
                '}';
    }
}