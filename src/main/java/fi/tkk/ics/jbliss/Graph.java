/* 
 * @(#)Graph.java
 *
 * Copyright 2007-2010 by Tommi Junttila.
 * Released under the GNU General Public License version 3.
 */

package fi.tkk.ics.jbliss;

import cz.adamh.utils.NativeUtils;
import io.arabesque.conf.Configuration;
import io.arabesque.graph.MainGraph;
import io.arabesque.graph.Vertex;
import io.arabesque.pattern.JBlissPattern;
import io.arabesque.pattern.PatternEdge;
import io.arabesque.pattern.PatternEdgeArrayList;
import io.arabesque.utils.collection.IntArrayList;
import net.openhft.koloboke.collect.IntCursor;
import net.openhft.koloboke.collect.ObjCursor;
import net.openhft.koloboke.collect.map.IntIntMap;
import net.openhft.koloboke.collect.map.hash.HashIntIntMap;
import net.openhft.koloboke.collect.map.hash.HashIntIntMaps;
import org.apache.commons.lang3.SystemUtils;
import sun.misc.Unsafe;

import java.io.IOException;
import java.lang.reflect.Field;

/**
 * An undirected graph.
 * Vertices can be colored (with integers) and self-loops are allowed
 * but multiple edges between vertices are ignored.
 *
 * @author Tommi Junttila
 */
public class Graph<V extends Comparable> {
	protected JBlissPattern pattern;
    protected Reporter       _reporter;
    protected Object         _reporter_param;

	protected void _report(int[] aut)
	{
		if(_reporter == null)
			return;

		int numVertices = pattern.getNumberOfVertices();

		HashIntIntMap real_aut = HashIntIntMaps.newMutableMap(numVertices);

		for (int i = 0; i < numVertices; ++i) {
			real_aut.put(i, aut[i]);
		}

		_reporter.report(real_aut, _reporter_param);
	}

    /* The internal JNI interface to true bliss */
    public static native long create();
    public static native void destroy(long true_bliss);
    public static native int _add_vertex(long true_bliss, int color);
    public static native void _add_edge(long true_bliss, int v1, int v2);
    protected native void _find_automorphisms(long true_bliss, Reporter r);
    public static native int[] _canonical_labeling(long true_bliss, Reporter r);


    /**
     * Create a new undirected graph with no vertices or edges.
     */
    public Graph(JBlissPattern pattern)
    {
		this.pattern = pattern;
    }

	private long createBliss() {
		MainGraph mainGraph = Configuration.get().getMainGraph();
		IntArrayList vertices = pattern.getVertices();
		int numVertices = vertices.size();
		PatternEdgeArrayList edges = pattern.getEdges();

		long bliss = create();
		assert bliss != 0;

		IntCursor vertexCursor = vertices.cursor();

		while (vertexCursor.moveNext()) {
			Vertex vertex = mainGraph.getVertex(vertexCursor.elem());
			_add_vertex(bliss, vertex.getVertexLabel());
		}

		ObjCursor<PatternEdge> edgeCursor = edges.cursor();

		while (edgeCursor.moveNext()) {
			PatternEdge edge = edgeCursor.elem();

			if (edge.getSrcPos() >= numVertices || edge.getDestPos() >= numVertices) {
				throw new RuntimeException("Wrong (possibly old?) pattern edge found. Src (" + edge.getSrcPos() + "), Dst (" + edge.getDestPos() + ") or both are higher than numVertices (" + numVertices + ")");
			}
			_add_edge(bliss, edge.getSrcPos(), edge.getDestPos());
		}

		return bliss;
	}

	public void findAutomorphisms(Reporter reporter, Object reporter_param) {
		long bliss = createBliss();

		_reporter = reporter;
		_reporter_param = reporter_param;
		_find_automorphisms(bliss, _reporter);
		destroy(bliss);
		_reporter = null;
		_reporter_param = null;
	}

	public void fillCanonicalLabeling(IntIntMap canonicalLabelling) {
		fillCanonicalLabeling(null, null, canonicalLabelling);
	}

	public void fillCanonicalLabeling(Reporter reporter, Object reporter_param, IntIntMap canonicalLabellling) {
		int numVertices = pattern.getNumberOfVertices();
		long bliss = createBliss();

		_reporter = reporter;
		_reporter_param = reporter_param;
		int[] cf = _canonical_labeling(bliss, _reporter);
		destroy(bliss);

		canonicalLabellling.clear();

		for (int i = 0; i < numVertices; ++i) {
			canonicalLabellling.put(i, cf[i]);
		}

		_reporter = null;
		_reporter_param = null;
	}

    static {
		int systemBits;

        try {
			Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
            unsafeField.setAccessible(true);
            Unsafe unsafe = (Unsafe) unsafeField.get(null);

			// Get system bits
			systemBits = unsafe.addressSize() * 8;

			if (SystemUtils.IS_OS_MAC || SystemUtils.IS_OS_MAC_OSX) {
				if (systemBits == 64) {
					NativeUtils.loadLibraryFromJar("/libjbliss-mac.so");
				} else {
					throw new UnsupportedOperationException("Library not compiled for MAC " + systemBits + " bits");
				}
			}
			else if (SystemUtils.IS_OS_LINUX) {
				if (systemBits == 64) {
					NativeUtils.loadLibraryFromJar("/libjbliss-linux.so");
				} else {
					throw new UnsupportedOperationException("Library not compiled for Linux " + systemBits + " bits");
				}
			}
			else if (SystemUtils.IS_OS_WINDOWS) {
				if (systemBits == 64) {
					NativeUtils.loadLibraryFromJar("/libjbliss-win.dll");
				} else {
					throw new UnsupportedOperationException("Library not compiled for Windows " + systemBits + " bits");
				}
			}
			else {
				throw new UnsupportedOperationException("Library not compiled for " + SystemUtils.OS_NAME);
			}
        } catch (IOException | IllegalAccessException | NoSuchFieldException e) {
			throw new RuntimeException("Unable to load correct jbliss library for system: " + SystemUtils.OS_NAME + " " + SystemUtils.OS_ARCH + " " + SystemUtils.OS_NAME, e);
        }
	}
}
