package io.arabesque.embedding;

import io.arabesque.misc.WritableObject;
import io.arabesque.pattern.Pattern;
import net.openhft.koloboke.collect.set.hash.HashIntSet;

/**
 * Created by Alex on 21-Sep-15.
 */
public interface Embedding extends WritableObject {
    int[] getWords();

    int[] getVertices();

    int getNumVertices();

    int[] getEdges();

    int getNumEdges();

    int getNumWords();

    Pattern getPattern();

    int getNumVerticesAddedWithExpansion();

    int getNumEdgesAddedWithExpansion();

    void addWord(int word);

    void removeLastWord();

    HashIntSet getExtensibleWordIds();

    boolean isCanonicalEmbeddingWithWord(int wordId);

    String toOutputString();
}
