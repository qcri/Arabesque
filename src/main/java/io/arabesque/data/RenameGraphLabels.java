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

public class RenameGraphLabels {

    static final Charset charset = Charset.defaultCharset();
    static final String input = "/home/carlos/citeseer-grami.txt";
    static final String output = "/home/carlos/rn_citeseer-grami.txt";

    public static class ValueComparator implements Comparator<Integer> {
        Map<Integer, Integer> map;

        public ValueComparator(Map<Integer, Integer> base) {
            this.map = base;
        }

        public int compare(Integer a, Integer b) {
            if (map.get(a) >= map.get(b)) {
                return 1;
            } else {
                return -1;
            } // returning 0 would merge keys
        }
    }

    public static TreeMap<Integer, Integer> SortByValue(Map<Integer, Integer> labelHighestDegree) {
        ValueComparator vc = new ValueComparator(labelHighestDegree);
        TreeMap<Integer, Integer> sortedMap = new TreeMap<Integer, Integer>(vc);
        sortedMap.putAll(labelHighestDegree);
        return sortedMap;
    }

    public static void main(String[] args) {
        File fp = new File(input);
        Map<Integer, Entry<Integer, Set<Integer>>> mapNode = new HashMap<Integer, Map.Entry<Integer, Set<Integer>>>();

        Map<Integer, Integer> countLabelFreq = new HashMap<Integer, Integer>();
        Map<Integer, Integer> labelHighestDegree = new HashMap<Integer, Integer>();
        Map<Integer, Integer> mapLabel = new HashMap<Integer, Integer>();

        try (BufferedReader reader = Files.newBufferedReader(fp.toPath(), charset)) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                String parts[] = line.split(" |\t");
                Integer vertex = Integer.parseInt(parts[0]);
                Integer label = Integer.parseInt(parts[1]);

                Integer count = countLabelFreq.get(label);
                if (count == null) {
                    count = 0;
                }
                countLabelFreq.put(label, ++count);

                Set<Integer> list = new HashSet<Integer>();
                SimpleEntry<Integer, Set<Integer>> vertexValue = new AbstractMap.SimpleEntry<Integer, Set<Integer>>(label, list);

                for (int i = 2; i < parts.length; i++) {
                    list.add(Integer.parseInt(parts[i]));
                }
                mapNode.put(vertex, vertexValue);

                Integer degree = labelHighestDegree.get(label);
                if (degree == null || degree.compareTo(list.size()) < 0) {
                    labelHighestDegree.put(label, list.size());
                }

            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        TreeMap<Integer, Integer> sortedMap = SortByValue(labelHighestDegree);
        int newLabel = 0;
        for (Entry<Integer, Integer> entry : sortedMap.entrySet()) {
            mapLabel.put(entry.getKey(), ++newLabel);
        }

        File fpout = new File(output);
        printGraph(mapNode, fpout, mapLabel);
    }


    static void printGraph(Map<Integer, Map.Entry<Integer, Set<Integer>>> mapNode, File fp, Map<Integer, Integer> mapLabel) {
        try (BufferedWriter writer = Files.newBufferedWriter(fp.toPath(), charset)) {
            for (Map.Entry<Integer, Map.Entry<Integer, Set<Integer>>> node : mapNode.entrySet()) {
                Map.Entry<Integer, Set<Integer>> vdata = node.getValue();
                writer.write(node.getKey() + " " + mapLabel.get(vdata.getKey()));
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