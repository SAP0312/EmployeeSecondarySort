package com.driver;

import com.mapper.SecondarySortMapper;
import com.partitioner.SecondarySortPartitioner;
import com.reducer.SecondarySortReducer;
import com.utils.CompositeKey;
import com.utils.SecondarySortCompKeyComparator;
import com.utils.SecondarySortGroupComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondarySortDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("Two parameters are required for SecondarySortBasicDriver");
            return -1;
        }

        Job job = new Job(getConf());
        job.setJobName("Secondary Sort Example");

        job.setJarByClass(SecondarySortDriver.class);
        job.setMapperClass(SecondarySortMapper.class);
        job.setReducerClass(SecondarySortReducer.class);
        job.setOutputKeyClass(CompositeKey.class);
        job.setOutputValueClass(NullWritable.class);
        job.setPartitionerClass(SecondarySortPartitioner.class);
        job.setSortComparatorClass(SecondarySortCompKeyComparator.class);
        job.setGroupingComparatorClass(SecondarySortGroupComparator.class);

        job.setNumReduceTasks(8);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(),
                new SecondarySortDriver(), args);

    }
}
