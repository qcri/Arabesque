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

public class ConvertGph2Qanat {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("ConvertGph2Qanat <input file> <output file> [<num labels>]");
            return;
        }

        File inputPath = new File(args[0]);
        File outputPath = new File(args[1]);
        int numLabels = -1;

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
        IntIntMap vertexLabels = HashIntIntMaps.newMutableMap();

        Random randomGenerator = new Random();

        int numFilesProcessed = 0;

        for (String inputFilePath : inputFilePaths) {
            System.out.println("Processing " + inputFilePath + " (" + (++numFilesProcessed) + "/" + inputFilePaths.size() + ")");
            try (BufferedReader reader = Files.newBufferedReader(Paths.get(inputPath.getAbsolutePath(), inputFilePath), Charset.defaultCharset())) {
                String line;

                while ((line = reader.readLine()) != null) {
                    String parts[] = line.split(" |\t");

                    switch (parts[0]) {
                        case "v":
                            int vId = Integer.valueOf(parts[1]);

                            vertexTranslationMap.putIfAbsent(vId, vertexTranslationMap.size());

                            Integer label = null;

                            if (numLabels <= 0 && parts.length >= 3) {
                                label = Integer.valueOf(parts[2]);
                            }

                            if (label == null) {
                                if (numLabels <= 0) {
                                    throw new RuntimeException("No labels and no num labels specified");
                                }
                                label = randomGenerator.nextInt(numLabels);
                            }

                            vertexLabels.put(vId, (int) label);

                            break;
                        case "e":
                            ++numEdges;
                            int v1 = Integer.valueOf(parts[1]);
                            int v2 = Integer.valueOf(parts[2]);

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

                            break;
                    }
                }

            } catch (IOException x) {
                System.err.format("IOException: %s%n", x);
            }
        }

        int numVertices = vertexTranslationMap.size();

        try {
            printGraph(outputPath, numVertices, numEdges, adjList, vertexTranslationMap, vertexLabels);
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Num vertices: " + numVertices);
        System.out.println("Num edges: " + numEdges);
    }

    private static void printGraph(File outputFile, int numVertices, long numEdges, IntObjMap<IntSet> adjList, IntIntMap vertexTranslationMap, IntIntMap vertexLabels)
            throws IOException {
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
                int label = vertexLabels.get(vertexId);

                IntSet neighbours = adjList.get(vertexId);

                writer.write(Integer.toString(translatedVertexId));
                writer.write(' ');
                writer.write(Integer.toString(label));

                if (neighbours != null) {
                    IntCursor neighboursCursor = neighbours.cursor();

                    while (neighboursCursor.moveNext()) {
                        int neighbourId = neighboursCursor.elem();
                        int translatedNeighbourId = vertexTranslationMap.get(neighbourId);
                        writer.write(' ');
                        writer.write(Integer.toString(translatedNeighbourId));
                    }
                }

                writer.newLine();
            }
        }
    }
}
