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


public class RemoveNonFrequentEdges {


    static final Charset charset = Charset.defaultCharset();
    static final String inputDefault = "/home/carlos/random_10000_10_1.txt";
    static final String outputDefault = "/home/carlos/filtered-random_10000_10_1-1010.txt";
    static final Integer minsuppDefault = 1010;


    public static void main(String[] args) {
        String input = inputDefault;
        String output = outputDefault;
        Integer minsupp = minsuppDefault;

        if (args.length == 4) {
            input = args[1];
            output = args[2];
            minsupp = Integer.valueOf(args[3]);
        }

        File fp = new File(input);

        Map<Integer, Map.Entry<Integer, List<Integer>>> mapNode = new HashMap<Integer, Map.Entry<Integer, List<Integer>>>();
        Map<String, Map.Entry<Set<Integer>, Set<Integer>>> mapEdge = new HashMap<String, Map.Entry<Set<Integer>, Set<Integer>>>();

        try (BufferedReader reader = Files.newBufferedReader(fp.toPath(), charset)) {
            String line = null;
            while ((line = reader.readLine()) != null) {
                String parts[] = line.split(" |\t");
                Integer vertex = Integer.parseInt(parts[0]);
                Integer label = Integer.parseInt(parts[1]);

                List<Integer> list = new ArrayList<Integer>();
                SimpleEntry<Integer, List<Integer>> vertexValue = new AbstractMap.SimpleEntry<Integer, List<Integer>>(label, list);

                for (int i = 2; i < parts.length; i++) {
                    list.add(Integer.parseInt(parts[i]));
                }
                mapNode.put(vertex, vertexValue);
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        //calculating the frequency
        int numOfEdges = 0;
        for (Map.Entry<Integer, Map.Entry<Integer, List<Integer>>> node : mapNode.entrySet()) {
            Map.Entry<Integer, List<Integer>> vdata = node.getValue();
            numOfEdges += vdata.getValue().size();
            for (Integer i : vdata.getValue()) {
                Map.Entry<Integer, List<Integer>> vdataNeigh = mapNode.get(i);
                if (vdata.getKey().compareTo(vdataNeigh.getKey()) <= 0) {
                    StringBuilder sb = new StringBuilder();
                    sb.append(vdata.getKey() + "-" + vdataNeigh.getKey());
                    String labelEdge = sb.toString();


                    Map.Entry<Set<Integer>, Set<Integer>> edgeSupp = mapEdge.get(labelEdge);
                    if (edgeSupp == null) {
                        Set<Integer> set1 = new HashSet<Integer>();
                        Set<Integer> set2 = new HashSet<Integer>();
                        edgeSupp = new AbstractMap.SimpleEntry<Set<Integer>, Set<Integer>>(set1, set2);

                    }
                    edgeSupp.getKey().add(node.getKey());
                    edgeSupp.getValue().add(i);
                    mapEdge.put(labelEdge, edgeSupp);
                    if (node.getKey().compareTo(146) == 0 && vdataNeigh.getKey().compareTo(27) == 0)
                        System.out.println(labelEdge);

                }
            }
        }


        //removing the infrequent ones
        int numOfFiltered = 0;
        for (Map.Entry<Integer, Map.Entry<Integer, List<Integer>>> node : mapNode.entrySet()) {
            Map.Entry<Integer, List<Integer>> vdata = node.getValue();

            Iterator<Integer> it = vdata.getValue().iterator();
            while (it.hasNext()) {
                Integer neigh = it.next();
                if (neigh.compareTo(node.getKey()) == 0) {
                    System.err.println("Loop detected!");
                    System.exit(1);
                } else {
                    Map.Entry<Integer, List<Integer>> vdataNeigh = mapNode.get(neigh);
                    StringBuilder sb = new StringBuilder();
                    if (vdata.getKey().compareTo(vdataNeigh.getKey()) <= 0) {
                        sb.append(vdata.getKey() + "-" + vdataNeigh.getKey());
                    } else {
                        sb.append(vdataNeigh.getKey() + "-" + vdata.getKey());
                    }
                    String labelEdge = sb.toString();
                    Map.Entry<Set<Integer>, Set<Integer>> edgeSupp = mapEdge.get(labelEdge);

                    if (edgeSupp == null) {
                        System.err.println("Edge not found! " + labelEdge + " Node " + node.getKey()
                                + " Neigh " + neigh);
                        System.exit(1);
                    }
                    //removing edges
                    if (edgeSupp.getKey().size() < minsupp || edgeSupp.getValue().size() < minsupp) {
                        it.remove();
                        numOfFiltered++;
                    }
                }
            }
        }


        int numOfPatternFiltered = 0;
        for (Entry<String, Entry<Set<Integer>, Set<Integer>>> edge : mapEdge.entrySet()) {
            if (edge.getValue().getKey().size() < minsupp || edge.getValue().getValue().size() < minsupp) {
                numOfPatternFiltered++;
            }
        }

        System.out.println("Total Edges: " + numOfEdges / 2.0 + " Filtered Edges: " + numOfFiltered / 2.0 +
                " Total Patterns: " + mapEdge.size() + " Filtered Patterns: " + numOfPatternFiltered);
        File fpout = new File(output);
        printMapNode(mapNode, fpout);
        File fpoutGrami = new File(output + "grami");
        printMapNodeGramiFormat(mapNode, fpoutGrami);
        File fpoutDegree = new File(output + "degree");
        printDegreeHistogram(mapNode, fpoutDegree);
    }

    static void printMapNode(Map<Integer, Map.Entry<Integer, List<Integer>>> mapNode, File fp) {
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

    static void printMapNodeGramiFormat(Map<Integer, Map.Entry<Integer, List<Integer>>> mapNode, File fp) {
        try (BufferedWriter writer = Files.newBufferedWriter(fp.toPath(), charset)) {
            for (Map.Entry<Integer, Map.Entry<Integer, List<Integer>>> node : mapNode.entrySet()) {
                Map.Entry<Integer, List<Integer>> vdata = node.getValue();
                writer.write("v " + node.getKey() + " " + vdata.getKey());
                writer.newLine();
            }
            for (Map.Entry<Integer, Map.Entry<Integer, List<Integer>>> node : mapNode.entrySet()) {
                Map.Entry<Integer, List<Integer>> vdata = node.getValue();

                for (Integer i : vdata.getValue()) {
                    if (node.getKey().compareTo(i) < 0) {
                        writer.write("e " + node.getKey() + " " + i + " 1");
                        writer.newLine();
                    }
                }
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void printDegreeHistogram(Map<Integer, Map.Entry<Integer, List<Integer>>> mapNode, File fp) {
        try (BufferedWriter writer = Files.newBufferedWriter(fp.toPath(), charset)) {
            for (Map.Entry<Integer, Map.Entry<Integer, List<Integer>>> node : mapNode.entrySet()) {
                Map.Entry<Integer, List<Integer>> vdata = node.getValue();
                writer.write(node.getKey() + " " + vdata.getValue().size());
                writer.newLine();
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}