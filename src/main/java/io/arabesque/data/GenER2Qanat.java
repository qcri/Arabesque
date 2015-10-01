package io.arabesque.data;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.*;

public class GenER2Qanat {

    static final Charset charset = Charset.defaultCharset();//Charset.forName("US-ASCII");
    static final String output = "/tmp/random_10000_10_1.txt";
    static final int numNodes = 10000;
    static final int numLabels = 10;
    static final double edgeProb = 0.01;
    protected static Random random = new Random();

    public static void main(String[] args) {
        // TODO Auto-generated method stub


        Random randomGenerator = new Random();
        Map<Integer, Map.Entry<Integer, List<Integer>>> mapNode = new HashMap<Integer, Map.Entry<Integer, List<Integer>>>();
        for (int i = 1; i <= numNodes; i++) {
            for (int j = (i + 1); j <= numNodes; j++) {
                if (randomInRange(0, 1) <= edgeProb) {
                    Map.Entry<Integer, List<Integer>> nodedata1 = mapNode.get(i);
                    Map.Entry<Integer, List<Integer>> nodedata2 = mapNode.get(j);
                    if (nodedata1 == null) {
                        int label = randomGenerator.nextInt(numLabels) + 1;
                        List<Integer> edges = new ArrayList<Integer>();
                        nodedata1 = new AbstractMap.SimpleEntry<Integer, List<Integer>>(label, edges);
                        mapNode.put(i, nodedata1);
                    }
                    if (nodedata2 == null) {
                        int label = randomGenerator.nextInt(numLabels) + 1;
                        List<Integer> edges = new ArrayList<Integer>();
                        nodedata2 = new AbstractMap.SimpleEntry<Integer, List<Integer>>(label, edges);
                        mapNode.put(j, nodedata2);
                    }
                    nodedata1.getValue().add(j);
                    nodedata2.getValue().add(i);

                }
            }
        }
        File fpout = new File(output);
        printGraph(mapNode, fpout);
    }


    public static double randomInRange(double min, double max) {
        double range = max - min;
        double scaled = random.nextDouble() * range;
        double shifted = scaled + min;
        return shifted; // == (rand.nextDouble() * (max-min)) + min;
    }

    static void printGraph(Map<Integer, Map.Entry<Integer, List<Integer>>> mapNode, File fp) {
        try (BufferedWriter writer = Files.newBufferedWriter(fp.toPath(), charset)) {
            for (Map.Entry<Integer, Map.Entry<Integer, List<Integer>>> node : mapNode.entrySet()) {
                Map.Entry<Integer, List<Integer>> vdata = node.getValue();
                writer.write(node.getKey() + " " + vdata.getKey());
                for (Integer i : vdata.getValue()) {
                    writer.write(" " + i);
                }
                writer.newLine();
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
