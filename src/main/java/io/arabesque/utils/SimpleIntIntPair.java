package io.arabesque.utils;

public class SimpleIntIntPair {
    private int first;
    private int second;

    public SimpleIntIntPair() {
        first = 0;
        second = 0;
    }

    public SimpleIntIntPair(int first, int second) {
        this.first = first;
        this.second = second;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + first;
        result = prime * result + second;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SimpleIntIntPair other = (SimpleIntIntPair) obj;
        if (first != other.first)
            return false;
        if (second != other.second)
            return false;
        return true;
    }

    public String toString() {
        return "(" + first + ", " + second + ")";
    }

    public int getFirst() {
        return first;
    }

    public void setFirst(int first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }


}


