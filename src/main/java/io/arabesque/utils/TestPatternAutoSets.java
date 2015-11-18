package io.arabesque.utils;

import io.arabesque.conf.Configuration;
import io.arabesque.conf.TestConfiguration;
import io.arabesque.graph.MainGraph;
import io.arabesque.pattern.JBlissPattern;
import io.arabesque.pattern.Pattern;
import io.arabesque.pattern.VICPattern;
import io.arabesque.pattern.VertexPositionEquivalences;
import io.arabesque.utils.collection.IntArrayList;
import net.openhft.koloboke.collect.IntCollection;
import net.openhft.koloboke.collect.IntCursor;
import net.openhft.koloboke.collect.map.IntIntMap;
import net.openhft.koloboke.collect.set.hash.HashIntSet;
import net.openhft.koloboke.collect.set.hash.HashIntSets;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;

import java.util.ArrayList;

public class TestPatternAutoSets {
    public static void main(String[] args) {
        if (args.length < 1) {
            throw new IllegalArgumentException("No graph given");
        }

        if (args.length < 2) {
            throw new IllegalArgumentException("No embedding given");
        }

        org.apache.hadoop.conf.Configuration giraphConfiguration = new org.apache.hadoop.conf.Configuration();
        giraphConfiguration.set(Configuration.CONF_MAINGRAPH_PATH, args[0]);
        Configuration.setIfUnset(new TestConfiguration(new ImmutableClassesGiraphConfiguration(giraphConfiguration)));
        Configuration.get().initialize();

        String embeddingStr = args[1];

        String[] edgesStr = embeddingStr.split(" ");

        HashIntSet vertexIds = HashIntSets.newMutableSet();
        IntArrayList edgeIds = new IntArrayList(edgesStr.length);
        ArrayList<IntIntPair> edges = new ArrayList<>(edgesStr.length);

        MainGraph mainGraph = Configuration.get().getMainGraph();

        for (String edgeStr : edgesStr) {
            String[] edgeComponentsStr = edgeStr.split("-");

            int srcId = Integer.parseInt(edgeComponentsStr[0]);
            int dstId = Integer.parseInt(edgeComponentsStr[1]);

            edges.add(new IntIntPair(srcId, dstId));

            vertexIds.add(srcId);
            vertexIds.add(dstId);

            System.out.println("Found edge " + srcId + ", " + dstId);

            IntCollection edgeIdsToAdd = mainGraph.getEdgeIds(srcId, dstId);
            IntCursor edgeIdsToAddCursor = edgeIdsToAdd.cursor();

            while (edgeIdsToAddCursor.moveNext()) {
                edgeIds.add(edgeIdsToAddCursor.elem());
            }
        }

        JBlissPattern jblissPattern = new JBlissPattern();
        VICPattern vicPattern = new VICPattern();

        for (int i = 0; i < edgeIds.getSize(); ++i) {
            int edgeId = edgeIds.getUnchecked(i);
            jblissPattern.addEdge(edgeId);
            vicPattern.addEdge(edgeId);
        }

        printPattern("vic", vicPattern);
        printCanonicalLabelling("vic", vicPattern.getCanonicalLabeling());
        printVertexPositionEquivalences("vic", vicPattern.getVertexPositionEquivalences());
        vicPattern.turnCanonical();
        printPattern("vic-min", vicPattern);
        printCanonicalLabelling("vic-min", vicPattern.getCanonicalLabeling());
        printVertexPositionEquivalences("vic-min", vicPattern.getVertexPositionEquivalences());

        printPattern("jbliss", jblissPattern);
        printCanonicalLabelling("jbliss", jblissPattern.getCanonicalLabeling());
        printVertexPositionEquivalences("jbliss", jblissPattern.getVertexPositionEquivalences());
        jblissPattern.turnCanonical();
        printPattern("jbliss-min", jblissPattern);
        printCanonicalLabelling("jbliss-min", jblissPattern.getCanonicalLabeling());
        printVertexPositionEquivalences("jbliss-min", jblissPattern.getVertexPositionEquivalences());
    }

    public static void printPattern(String title, Pattern pattern) {
        System.out.println("Pattern of " + title);
        System.out.println(pattern.toString());
    }

    public static void printCanonicalLabelling(String title, IntIntMap canonicalLabelling) {
        System.out.println("Canonical labelling of " + title);
        System.out.println(canonicalLabelling.toString());
    }

    public static void printVertexPositionEquivalences(String title, VertexPositionEquivalences vertexPositionEquivalences) {
        System.out.println("Autovertex set of " + title);
        System.out.println(vertexPositionEquivalences.toString());
    }
}
