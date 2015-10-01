/* 
 * @(#)Utils.java
 *
 * Copyright 2008 by Tommi Junttila.
 * Released under the GNU General Public License version 2.
 */

package fi.tkk.ics.jbliss;

import java.util.Map;

/**
 * A container class for some utility functions.
 *
 * @author Tommi Junttila
 */

abstract public class Utils
{
    /**
     * Print a labeling (given as a Map).
     *
     * @param stream    the stream into which the permutation is printed
     * @param labeling  the labeling to be printed
     */
    public static void print_labeling(java.io.PrintStream stream,
				      Map labeling)
    {
	String sep = "";
	stream.print("{");
        for(Map.Entry e: ((Map<Object,Object>)labeling).entrySet()) {
	    stream.print(sep+e.getKey()+" -> "+e.getValue());
	    sep = ",";
	}
	stream.print("}");
    }

    /*
     * Print a permutation in the cycle format.
     * The argument perm must be a bijection on {0,...,perm.length-1}.
     *
     * @param stream   the stream into which the permutation is printed
     * @param perm     the permutation to be printed
     */
    /*
    public static void print_permutation(java.io.PrintStream stream,
					 int[] perm)
    {
	for(int i = 0; i < perm.length; i++) {
	    boolean is_first = true;
	    for(int j = i; perm[j] != i; j = perm[j]) {
		if(perm[j] < i) {
		    is_first = false;
		    break;
		}
	    }
	    if(perm[i] != i && is_first) {
		String sep = "";
		stream.print("(");
		int j = i;
		do {
		    stream.print(sep+j);
		    sep = ",";
		    j = perm[j];
		} while(j != i);
		stream.print(")");
	    }
	}
    }
    */
}
