package io.arabesque.optimization;

import io.arabesque.conf.Configuration;
import org.apache.giraph.utils.ReflectionUtils;

public class ConfigBasedOptimizationSetDescriptor implements OptimizationSetDescriptor {
    public static final String CONF_OPTIMIZATION_CLASSES = "arabesque.optimizations";
    public static final Class[] CONF_OPTMIZATION_CLASSES_DEFAULT = {};

    @Override
    public OptimizationSet describe() {
        OptimizationSet optimizationSet = new OptimizationSet();

        Configuration configuration = Configuration.get();
        Class[] optimizationClasses = configuration.getClasses(CONF_OPTIMIZATION_CLASSES);

        for (Class optimizationClass : optimizationClasses) {
            if (!Optimization.class.isAssignableFrom(optimizationClass)) {
                throw new RuntimeException("Class " + optimizationClass + " does not implement hte Optimization interface");
            }

            Optimization optimization = (Optimization) ReflectionUtils.newInstance(optimizationClass);
            optimizationSet.add(optimization);
        }

        return optimizationSet;
    }
}
