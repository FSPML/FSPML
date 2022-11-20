package org.apache.hama.myhama.io;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

public class TextRecordReader extends RecordReader<Text, Text> {

    private static final Log LOG = LogFactory.getLog(TextRecordReader.class);
    private CompressionCodecFactory compressionCodecs = null;
    private long start;
    private long pos;
    private long end;
    private LineReader in;
    private int maxLineLength;
    private Text key = null;
    private Text value = null;
    private String separator = "\t";

    @Override
    public void initialize(InputSplit genericSplit, Configuration conf)
            throws IOException, InterruptedException {
        FileSplit split = ( FileSplit ) genericSplit;
        this.maxLineLength = conf.getInt(
                "mapred.linerecordreader.maxlength", Integer.MAX_VALUE);
        this.separator = conf
                .get("key.value.separator.in.input.line", "\t");
        start = split.getStart();
        end = start + split.getLength();
        final Path file = split.getPath();
        compressionCodecs = new CompressionCodecFactory(conf);
        final CompressionCodec codec = compressionCodecs.getCodec(file);

        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream fileIn = fs.open(split.getPath());
        boolean skipFirstLine = false;
        if (codec != null) {
            in = new LineReader(codec.createInputStream(fileIn), conf);
            end = Long.MAX_VALUE;
        } else {
            if (start != 0) {
                skipFirstLine = true;
                --start;
                fileIn.seek(start);
            }
            in = new LineReader(fileIn, conf);
        }
        if (skipFirstLine) {
            start += in.readLine(new Text(), 0, ( int ) Math.min(
                    ( long ) Integer.MAX_VALUE, end - start));
        }
        this.pos = start;

    }

    @Override
    public synchronized void close() throws IOException {
        if (in != null) {
            in.close();
        }
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos - start) / ( float ) (end - start));
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        Text line = new Text();
        if (key == null) {
            key = new Text();
        }
        if (value == null) {
            value = new Text();
        }
        int newSize = 0;
        while (pos < end) {
            newSize = in.readLine(line, maxLineLength, Math.max(
                    ( int ) Math.min(Integer.MAX_VALUE, end - pos),
                    maxLineLength));
            if (null != line) {
                String[] kv = line.toString().split(this.separator);
                if (kv.length == 2) {
                    key.set(kv[0]);
                    value.set(kv[1]);
                } else {
                    //LOG.info("Skipped line has no separator");
                    key.set(kv[0]);
                    value.set("");
                }
            }
            if (newSize == 0) {
                break;
            }
            pos += newSize;
            if (newSize < maxLineLength) {
                break;
            }

            LOG.info("Skipped line of size " + newSize + " at pos "
                    + (pos - newSize));
        }
        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
    }
}
