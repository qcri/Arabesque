package io.arabesque.embedding;

import io.arabesque.conf.Configuration;
import io.arabesque.graph.MainGraph;
import io.arabesque.pattern.Pattern;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

public abstract class BasicEmbedding implements Embedding {

    int words[];

    protected MainGraph g;
    protected int numWords;

    private Pattern pattern;
    private boolean dirtyPattern;

    // Incremental to return extensibleIds...
    int[] previousVertices;
    int previousWordsPos = -1;

    int total = 0;
    int incremental = 0;

    static final int INC_ARRAY_SIZE = 15;

    public BasicEmbedding(int size) {
        g = Configuration.get().getMainGraph();
        words = new int[size];
        numWords = 0;
        dirtyPattern = true;
    }

    public BasicEmbedding() {
        this(INC_ARRAY_SIZE);
        previousVertices = new int[INC_ARRAY_SIZE];
    }

    public BasicEmbedding(BasicEmbedding other) {
        words = Arrays.copyOf(other.words, other.numWords);
        numWords = other.numWords;
        g = Configuration.get().getMainGraph();
        pattern = other.pattern.copy();
        previousVertices = new int[INC_ARRAY_SIZE];
    }

    public void reset() {
        this.numWords = 0;
        dirtyPattern = true;
    }

    @Override
    public int[] getWords() {
        return words;
    }

    @Override
    public int getNumWords() {
        return numWords;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(numWords);
        int i = 0;
        while (i < numWords) {
            out.writeInt(words[i]);
            i++;
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        reset();
        numWords = in.readInt();

        if (words.length < numWords) {
            words = Arrays.copyOf(words, numWords);
        }

        int i = 0;
        while (i < numWords) {
            words[i] = in.readInt();
            i++;
        }
    }

    @Override
    public void addWord(int word) {
        numWords++;
        if (words.length < numWords) {
            words = Arrays.copyOf(words, words.length + INC_ARRAY_SIZE);
        }
        words[numWords - 1] = word;
        dirtyPattern = true;
    }

    @Override
    public void removeLastWord() {
        numWords = Math.max(0, numWords - 1);
        dirtyPattern = true;
    }


    /**
     * We check the number of differences between the old and the new embedding to decide
     * whether we do it incrementally or from scratch. We return the position of the common.
     *
     * @return
     */
    int canDoIncremental(final int[] keys,
            final int numKeys) {
        if (previousWordsPos < 0) {
            return 0;
        }
        int pos = 0;
        while (pos < numKeys) {
            if (keys[pos] != previousVertices[pos]) {
                return pos;
            }
            ++pos;
        }
        return pos;
    }

    @Override
    public String toString() {
        return "Embedding{" +
                "words=" + Arrays.toString(words) +
                ", numWords=" + numWords +
                "} " + super.toString();
    }

    @Override
    public Pattern getPattern() {
        if (pattern == null) {
            pattern = Configuration.get().createPattern();
            dirtyPattern = true;
        }

        if (dirtyPattern) {
            pattern.setEmbedding(this);
            dirtyPattern = false;
        }

        return pattern;
    }

}
