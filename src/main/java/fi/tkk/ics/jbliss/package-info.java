																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																																															/**
/ * The jbliss package provides a Java interface to the
 * <a href="http://www.tcs.hut.fi/Software/bliss/">bliss</a> tool.
 * The interface is implemented by using the
 * <a href="http://java.sun.com/docs/books/jni/">Java Native Interface (JNI)</a>.
 * 
 * <h2>Compiling</h2>
 *
 * In order to compile jbliss in a Linux/Unix environment,
 * you should modify (at least) the path variables in <code>Makefile</code>
 * and then execute the <code>make</code> command.
 *
 *
 * <h2>Usage</h2>
 *
 * First, make sure that the Java virtual machine can find the created
 * jbliss library; e.g. in Linux/Unix set the environment variable
 * LD_LIBRARY_PATH to include the directory containing
 * the library file <code>libjbliss.so</code>.
 * For further information, see the instructions in the
 * <a href="http://java.sun.com/docs/books/jni/">JNI documentation</a>.
 *
 * Perhaps the best place to get an idea of how jbliss can be used in
 * a Java program is to see the source code
 * of the class {@link jbliss.JBliss}.
 */
package fi.tkk.ics.jbliss;	

																																				