package io.arabesque.data;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map.Entry;

public class SortNodesGraphByDegree {

    static final Charset charset = Charset.defaultCharset();
    static final String input = "/home/carlos/qanat/data/human-qanat.txt";
    static final String output = "/home/carlos/qanat/data/human-qanat-sortedByDegree.txt";

    public static class ValueComparator implements Comparator<Integer> {
        Map<Integer, Entry<Integer, Set<Integer>>> map;

        public ValueComparator(Map<Integer, Entry<Integer, Set<Integer>>> base) {
            this.map = base;
        }

        public int compare(Integer a, Integer b) {
            if (map.get(a).getValue().size() >= map.get(b).getValue().size()) {
                return 1;
            } else {
                return -1;
            } // returning 0 would merge keys
        }
    }

    public static TreeMap<Integer, Entry<Integer, Set<Integer>>> SortByValue(Map<Integer, Entry<Integer, Set<Integer>>> highestDegree) {
        ValueComparator vc = new ValueComparator(highestDegree);
        TreeMap<Integer, Entry<Integer, Set<Integer>>> sortedMap = new TreeMap<Integer, Entry<Integer, Set<Integer>>>(vc);
        sortedMap.putAll(highestDegree);
        return sortedMap;
    }

    public static void main(String[] args) {
        File fp = new File(input);
        Map<Integer, Entry<Integer, Set<Integer>>> mapNode = new HashMap<Integer, Map.Entry<Integer, Set<Integer>>>();
        Map<Integer, Integer> mapId = new HashMap<Integer, Integer>();

        try (BufferedReader reader = Files.newBufferedReader(fp.toPath(), charset)) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                String parts[] = line.split(" |\t");
                Integer vertex = Integer.parseInt(parts[0]);
                Integer label = Integer.parseInt(parts[1]);

                Set<Integer> list = new HashSet<Integer>();
                SimpleEntry<Integer, Set<Integer>> vertexValue = new AbstractMap.SimpleEntry<Integer, Set<Integer>>(label, list);

                for (int i = 2; i < parts.length; i++) {
                    list.add(Integer.parseInt(parts[i]));
                }
                mapNode.put(vertex, vertexValue);
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        TreeMap<Integer, Entry<Integer, Set<Integer>>> sortedMap = SortByValue(mapNode);
        int newLabel = 0;
        for (Entry<Integer, Entry<Integer, Set<Integer>>> entry : sortedMap.entrySet()) {
            mapId.put(entry.getKey(), newLabel++);
        }

        File fpout = new File(output);
        printGraph(sortedMap, fpout, mapId);
    }


    static void printGraph(Map<Integer, Map.Entry<Integer, Set<Integer>>> mapNode, File fp, Map<Integer, Integer> mapId) {
        try (BufferedWriter writer = Files.newBufferedWriter(fp.toPath(), charset)) {
            for (Map.Entry<Integer, Map.Entry<Integer, Set<Integer>>> node : mapNode.entrySet()) {
                Map.Entry<Integer, Set<Integer>> vdata = node.getValue();
                writer.write(mapId.get(node.getKey()) + " " + vdata.getKey());
                for (Integer i : vdata.getValue()) {
                    writer.write(" " + mapId.get(i));
                }
                writer.newLine();
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}