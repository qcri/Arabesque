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

public class ConvertQanat2ListOfEdges {

    static final Charset charset = Charset.defaultCharset();//Charset.forName("US-ASCII");
    static final String input = "/home/carlos/qanat/data/citeseer-qanat-sortedByDegree-sameLabel.txt";
    static final String output = "/home/carlos/qanat/data/citeseer.edges";


    public static void main(String[] args) {
        // TODO Auto-generated method stub
        File fp = new File(input);
        Map<Integer, Entry<Integer, Set<Integer>>> mapNode = new HashMap<Integer, Map.Entry<Integer, Set<Integer>>>();

        try (BufferedReader reader = Files.newBufferedReader(fp.toPath(), charset)) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                //System.out.println(line);
                String parts[] = line.split(" |\t");
                Integer vertex = Integer.parseInt(parts[0]);
                Integer label = Integer.parseInt(parts[1]);

                Set<Integer> list = new HashSet<Integer>();
                SimpleEntry<Integer, Set<Integer>> vertexValue = new AbstractMap.SimpleEntry<Integer, Set<Integer>>(label, list);

                for (int i = 2; i < parts.length; i++) {
                    //System.out.println(parts[i]);
                    list.add(Integer.parseInt(parts[i]));
                }
                mapNode.put(vertex, vertexValue);
            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        File fpout = new File(output);
        printGraph(mapNode, fpout);
    }


    static void printGraph(Map<Integer, Map.Entry<Integer, Set<Integer>>> mapNode, File fp) {
        Map<Integer, Integer> mapId = new HashMap<Integer, Integer>();

        try (BufferedWriter writer = Files.newBufferedWriter(fp.toPath(), charset)) {
            for (Map.Entry<Integer, Map.Entry<Integer, Set<Integer>>> node : mapNode.entrySet()) {
                Map.Entry<Integer, Set<Integer>> vdata = node.getValue();
                if (vdata.getValue().isEmpty()) continue;

                Integer first = mapId.get(node.getKey());
                if (first == null) {
                    first = mapId.size();
                    mapId.put(node.getKey(), first);
                }
                for (Integer i : vdata.getValue()) {
                    Integer second = mapId.get(i);
                    if (second == null) {
                        second = mapId.size();
                        mapId.put(i, second);
                    }
                    if (first.compareTo(second) < 0) {
                        writer.write(first + " " + second);
                        writer.newLine();
                    }
                }
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void remapNodes(Map<Integer, Map.Entry<Integer, Set<Integer>>> mapNode, Map<Integer, Integer> mapId, Integer id) {

        if (mapId.size() == mapNode.size()) {

        }

    }
}


