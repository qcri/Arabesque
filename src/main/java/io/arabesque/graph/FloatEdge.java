package io.arabesque.graph;

/**
 * Created by siganos on 2/16/16.
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FloatEdge extends Edge {
  private float edgeLabel;

  public FloatEdge() {
    edgeLabel = 0.0f;
  }

  public FloatEdge(int sourceVertexId, int destinationVertexId, float edgeLabel) {
    super(sourceVertexId, destinationVertexId);

    this.edgeLabel = edgeLabel;
  }

  public FloatEdge(int edgeId, int sourceVertexId, int destinationVertexId, float edgeLabel) {
    super(edgeId, sourceVertexId, destinationVertexId);

    this.edgeLabel = edgeLabel;
  }

  public float getEdgeLabel() {
    return edgeLabel;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    super.write(dataOutput);

    dataOutput.writeFloat(edgeLabel);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    super.readFields(dataInput);

    edgeLabel = dataInput.readInt();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;

    FloatEdge that = (FloatEdge) o;

    return edgeLabel == that.edgeLabel;

  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + (edgeLabel != +0.0f ? Float.floatToIntBits(edgeLabel) : 0);
    return result;
  }

  @Override
  public String toString() {
    return "FloatEdge{" +
        "edgeLabel=" + edgeLabel +
        "} " + super.toString();
  }
}
