package io.arabesque.pattern;

import fi.tkk.ics.jbliss.Graph;
import fi.tkk.ics.jbliss.Reporter;
import net.openhft.koloboke.collect.map.IntIntCursor;
import net.openhft.koloboke.collect.map.IntIntMap;
import net.openhft.koloboke.collect.map.hash.HashIntIntMap;
import org.apache.log4j.Logger;

/**
 * Created by afonseca on 3/15/2015.
 */
public class JBlissPattern extends BasicPattern {
    private static final Logger LOG = Logger.getLogger(JBlissPattern.class);

    private Graph<Integer> jblissGraph;

    public JBlissPattern() {
        super();
    }

    public JBlissPattern(JBlissPattern other) {
        super(other);
    }

    @Override
    protected void init() {
        super.init();
        jblissGraph = new Graph<>(this);
    }

    @Override
    public Pattern copy() {
        return new JBlissPattern(this);
    }

    protected class VertexPositionEquivalencesReporter implements Reporter {
        VertexPositionEquivalences equivalences;

        public VertexPositionEquivalencesReporter(VertexPositionEquivalences equivalences) {
            this.equivalences = equivalences;
        }

        @Override
        public void report(HashIntIntMap generator, Object user_param) {
            IntIntCursor generatorCursor = generator.cursor();

            while (generatorCursor.moveNext()) {
                int oldPos = generatorCursor.key();
                int newPos = generatorCursor.value();

                equivalences.addEquivalence(oldPos, newPos);
            }
        }
    }

    @Override
    protected void fillVertexPositionEquivalences(VertexPositionEquivalences vertexPositionEquivalences) {
        VertexPositionEquivalencesReporter reporter = new VertexPositionEquivalencesReporter(vertexPositionEquivalences);
        jblissGraph.findAutomorphisms(reporter, null);
        vertexPositionEquivalences.propagateEquivalences();
    }

    @Override
    protected void fillCanonicalLabelling(IntIntMap canonicalLabelling) {
        jblissGraph.fillCanonicalLabeling(canonicalLabelling);
    }
}
