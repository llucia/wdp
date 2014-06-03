package org.mysamples.partition;

import org.apache.giraph.conf.GiraphConstants;
import org.apache.giraph.conf.ImmutableClassesGiraphConfiguration;
import org.apache.giraph.partition.SimplePartitionerFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.mysamples.io.types.LongEncodedGoodsWritable;

/**
 * Created by user on 6/3/14.
 */
public class SimpleLongEncodedVertexRangePartitionerFactory
        <V extends Writable, E extends Writable>
        extends SimplePartitionerFactory<LongEncodedGoodsWritable, V, E> {

    /** Vertex key space size. */
    private long keySpaceSize;

    @Override
    protected int getPartition(LongEncodedGoodsWritable id, int partitionCount) {
        return getPartitionInRange(id.getLongId(), keySpaceSize, partitionCount);
    }

    @Override
    protected int getWorker(int partition, int partitionCount, int workerCount) {
        return getPartitionInRange(partition, partitionCount, workerCount);
    }

    @Override
    public void setConf(ImmutableClassesGiraphConfiguration conf) {
        super.setConf(conf);
        keySpaceSize =
                conf.getLong(GiraphConstants.PARTITION_VERTEX_KEY_SPACE_SIZE, -1);
        if (keySpaceSize == -1) {
            throw new IllegalStateException("Need to specify " +
                    GiraphConstants.PARTITION_VERTEX_KEY_SPACE_SIZE +
                    " when using SimpleLongRangePartitionerFactory");
        }
    }
}
