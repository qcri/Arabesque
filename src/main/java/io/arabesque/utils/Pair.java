package io.arabesque.utils;

public abstract class Pair<A, B> {
    private A first;
    private B second;

    public Pair() {
        first = null;
        second = null;
    }

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public int hashCode() {
        int hashFirst = first != null ? first.hashCode() : 0;
        int hashSecond = second != null ? second.hashCode() : 0;

        return (hashFirst + hashSecond) * hashSecond + hashFirst;
    }

    public boolean equals(Object other) {
        if (other instanceof Pair) {
            Pair otherWritablePair = (Pair) other;
            return ((this.first == otherWritablePair.first ||
                    (this.first != null && otherWritablePair.first != null && this.first.equals(otherWritablePair.first))) &&
                    (this.second == otherWritablePair.second || (this.second != null && otherWritablePair.second != null &&
                            this.second.equals(otherWritablePair.second))));
        }

        return false;
    }

    public String toString() {
        return "(" + first + ", " + second + ")";
    }

    public A getFirst() {
        return first;
    }

    public void setFirst(A first) {
        this.first = first;
    }

    public B getSecond() {
        return second;
    }

    public void setSecond(B second) {
        this.second = second;
    }
}
