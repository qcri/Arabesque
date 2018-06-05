package io.arabesque.search.trees;

/*import net.openhft.koloboke.collect.set.IntSet;
import net.openhft.koloboke.collect.set.hash.HashIntSets;
import net.openhft.koloboke.function.IntConsumer;*/
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import com.koloboke.function.IntConsumer;

import java.io.BufferedOutputStream;
import java.io.IOException;

/**
 * We don't care whether it is Vertex or Edge, since expansions are handled
 * in a different way.
 * Created by siganos on 4/18/16.
 */
public class SearchEmbedding {
    private int words[];
    private IntSet active;
    private int size = 0;
    private IntConsumer addToThisActive;

    public SearchEmbedding() {
        words = new int[32]; // Large enough to ignore boundaries.
                             // 1 per thread, serializing only the occupied slots.
        active = HashIntSets.newMutableSet(32);
        addToThisActive = new IntConsumer() {
            @Override
            public void accept(int word) {
                active.add(word);
            }
        };
    }

    public boolean contains(int word){
        return active.contains(word);
    }

    public void reset(){
        size = 0;
        active.clear();
    }

    public int getAtPos(int i){
        return words[i];
    }

    public int getLast() {
        return words[size-1];
    }

    public int getNumWords() {
        return size;
    }

    public void addWord(int word) {
        words[size] = word;
        size++;
        active.add(word);
    }

    public void removeLastWord() {
        size--;
        active.removeInt(words[size]);
    }

    @Override
    public String toString(){
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < size; ++i) {
            sb.append(words[i]).append(' ');
        }
        return sb.toString();
    }

    public int getSize() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public void setFrom(SearchEmbedding embedding){
        System.arraycopy(embedding.words, 0, this.words, 0,
                embedding.size);
        this.size = embedding.size;
        // TODO we can probably make something faster here
        this.active.clear();
//        for(int word : embedding.active) {
//            this.active.add(word);
//        }

        embedding.active.forEach(addToThisActive);
    }

    public void write_to_stream(BufferedOutputStream out) throws IOException {
        for (int i = 0; i < size; ++i) {
            final int k = words[i];
            out.write((k >>> 24) & 0xFF);
            out.write((k >>> 16) & 0xFF);
            out.write((k >>>  8) & 0xFF);
            out.write((k >>>  0) & 0xFF);
//            outputStream.writeInt(words[i]);
        }
    }

    public String toOutputString() {
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < size; ++i) {
            sb.append(words[i]).append(' ');
        }
        sb.append('\n');
        return sb.toString();
    }
}