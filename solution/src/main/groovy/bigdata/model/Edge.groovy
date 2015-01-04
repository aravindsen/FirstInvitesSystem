package bigdata.model

import java.io.DataOutput
import java.io.IOException

import org.apache.hadoop.io.WritableComparable


class Edge implements WritableComparable<Edge> {

    long leftVertex
    long rightVertex


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(leftVertex)
        out.writeLong(rightVertex)
    }

    @Override
    public void readFields(DataInput inp) throws IOException {
        inp.readLong(leftVertex)
        inp.readLong(rightVertex)
    }

    @Override
    public int compareTo(Edge that) {
        if ((this.leftVertex == that.leftVertex && this.rightVertex == that.rightVertex) || 
            (this.leftVertex == that.rightVertex && this.rightVertex == that.leftVertex)) {
            return 0
        }
        
        return 0
    }    
}
