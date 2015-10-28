package io.arabesque.optimization;

import java.util.ArrayList;

public class OptimizationSet {
    private ArrayList<Optimization> optimizations;

    public OptimizationSet() {
        optimizations = new ArrayList<>();
    }

    public void add(Optimization optimization) {
        optimizations.add(optimization);
    }

    public void add(int index, Optimization optimization) {
        optimizations.add(index, optimization);
    }

    public void addAfter(Optimization newOptimization, Optimization predecessor) {
        int index = optimizations.indexOf(predecessor);

        add(index + 1, newOptimization);
    }

    public void addBefore(Optimization newOptimization, Optimization successor) {
        int index = optimizations.indexOf(successor);

        add(index, newOptimization);
    }

    public void applyStartup() {
        for (Optimization optimization : optimizations) {
            optimization.applyStartup();
        }
    }

    public void applyAfterGraphLoad() {
        for (Optimization optimization : optimizations) {
            optimization.applyAfterGraphLoad();
        }
    }

    @Override
    public String toString() {
        return "OptimizationSet{" +
                "optimizations=" + optimizations +
                '}';
    }
}
