package io.arabesque.embedding;

import io.arabesque.misc.WritableObject;
import io.arabesque.pattern.Pattern;
import io.arabesque.utils.collection.IntArrayList;
import net.openhft.koloboke.collect.IntCollection;

import java.io.Externalizable;

public interface Embedding extends WritableObject, Externalizable {
    IntArrayList getWords();

    IntArrayList getVertices();

    int getNumVertices();

    IntArrayList getEdges();

    int getNumEdges();

    int getNumWords();

    Pattern getPattern();

    int getNumVerticesAddedWithExpansion();

    int getNumEdgesAddedWithExpansion();

    void addWord(int word);

    void removeLastWord();

    IntCollection getExtensibleWordIds();

    boolean isCanonicalEmbeddingWithWord(int wordId);

    String toOutputString();

}
