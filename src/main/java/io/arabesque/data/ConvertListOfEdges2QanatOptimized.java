package io.arabesque.data;

import net.openhft.koloboke.collect.IntCursor;
import net.openhft.koloboke.collect.map.IntIntCursor;
import net.openhft.koloboke.collect.map.IntIntMap;
import net.openhft.koloboke.collect.map.IntObjMap;
import net.openhft.koloboke.collect.map.hash.HashIntIntMaps;
import net.openhft.koloboke.collect.map.hash.HashIntObjMaps;
import net.openhft.koloboke.collect.set.IntSet;
import net.openhft.koloboke.collect.set.hash.HashIntSets;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class ConvertListOfEdges2QanatOptimized {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("ConvertListOfEdges2Qanat <input file> <output file> [<num labels>]");
            return;
        }

        File inputPath = new File(args[0]);
        File outputPath = new File(args[1]);
        int numLabels = 1;

        if (args.length >= 3) {
            numLabels = Integer.parseInt(args[2]);
        }

        if (!inputPath.exists()) {
            System.err.println("Input path " + inputPath.getAbsolutePath() + " does not exist");
        }

        List<String> inputFilePaths;

        if (inputPath.isFile()) {
            inputFilePaths = Collections.singletonList("");
        } else {
            inputFilePaths = Arrays.asList(inputPath.list());
        }

        long numEdges = 0;

        IntObjMap<IntSet> adjList = HashIntObjMaps.newMutableMap();
        IntIntMap vertexTranslationMap = HashIntIntMaps.newMutableMap();

        int numFilesProcessed = 0;

        for (String inputFilePath : inputFilePaths) {
            System.out.println("Processing " + inputFilePath + " (" + (++numFilesProcessed) + "/" + inputFilePaths.size() + ")");
            try (BufferedReader reader = Files.newBufferedReader(Paths.get(inputPath.getAbsolutePath(), inputFilePath), Charset.defaultCharset())) {
                String line;

                while ((line = reader.readLine()) != null) {
                    ++numEdges;
                    //System.out.println(line);
                    String parts[] = line.split(" |\t");
                    int v1 = Integer.valueOf(parts[0]);
                    int v2 = Integer.valueOf(parts[1]);

                    vertexTranslationMap.putIfAbsent(v1, vertexTranslationMap.size());
                    vertexTranslationMap.putIfAbsent(v2, vertexTranslationMap.size());

                    IntSet v1Neigh = adjList.get(v1);
                    IntSet v2Neigh = adjList.get(v2);

                    if (v1Neigh == null) {
                        v1Neigh = HashIntSets.newMutableSet();
                        adjList.put(v1, v1Neigh);
                    }

                    if (v2Neigh == null) {
                        v2Neigh = HashIntSets.newMutableSet();
                        adjList.put(v2, v2Neigh);
                    }

                    v1Neigh.add(v2);
                    v2Neigh.add(v1);
                }

            } catch (IOException x) {
                System.err.format("IOException: %s%n", x);
            }
        }

        int numVertices = vertexTranslationMap.size();

        try {
            printGraph(outputPath, numVertices, numEdges, adjList, vertexTranslationMap, numLabels);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Num vertices: " + numVertices);
        System.out.println("Num edges: " + numEdges);
    }

    private static void printGraph(File outputFile, int numVertices, long numEdges, IntObjMap<IntSet> adjList, IntIntMap vertexTranslationMap, int numLabels)
            throws IOException {
        Random randomGenerator = new Random();

        try (BufferedWriter writer = Files.newBufferedWriter(outputFile.toPath(), Charset.defaultCharset())) {
            IntIntCursor vertexMapCursor = vertexTranslationMap.cursor();

            writer.write("# ");
            writer.write(Integer.toString(numVertices));
            writer.write(' ');
            writer.write(Long.toString(numEdges));
            writer.newLine();

            while (vertexMapCursor.moveNext()) {
                int vertexId = vertexMapCursor.key();
                int translatedVertexId = vertexMapCursor.value();
                int label = randomGenerator.nextInt(numLabels);

                IntSet neighbours = adjList.get(vertexId);

                writer.write(Integer.toString(translatedVertexId));
                writer.write(' ');
                writer.write(Integer.toString(label));

                if (neighbours != null) {
                    IntCursor neighboursCursor = neighbours.cursor();

                    while (neighboursCursor.moveNext()) {
                        int neighbourId = neighboursCursor.elem();
                        int translatedNeighbourId = vertexTranslationMap.get(neighbourId);

                        if (translatedVertexId > translatedNeighbourId) {
                            continue;
                        }

                        writer.write(' ');
                        writer.write(Integer.toString(translatedNeighbourId));
                    }
                }

                writer.newLine();
            }
        }
    }
}
