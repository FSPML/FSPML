package org.apache.hama.myhama.io;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.hadoop.io.Text;
import org.apache.hama.Constants;
import org.apache.hama.bsp.BSPJob;

public class TextRecordWriter extends RecordWriter<Text, Text> {
    private static final String utf8 = "UTF-8";
    private static final byte[] newline;
    static {
        try {
            newline = "\n".getBytes(utf8);
        } catch (UnsupportedEncodingException uee) {
            throw new IllegalArgumentException("can't find " + utf8
                    + " encoding");
        }
    }

    protected DataOutputStream out;
    private final byte[] keyValueSeparator;

    public TextRecordWriter(DataOutputStream out, String keyValueSeparator) {
        this.out = out;
        try {
            this.keyValueSeparator = keyValueSeparator.getBytes(utf8);
        } catch (UnsupportedEncodingException uee) {
            throw new IllegalArgumentException("can't find " + utf8
                    + " encoding");
        }
    }

    public TextRecordWriter(DataOutputStream out) {
        this(out, Constants.KV_SPLIT_FLAG);
    }

    /**
     * Write the object to the byte stream, handling Text as a special case.
     * 
     * @param o the object to print
     * @throws IOException if the write throws, we pass it on
     */
    private void writeObject(Object o) throws IOException {
        if (o instanceof Text) {
            Text to = ( Text ) o;
            out.write(to.getBytes(), 0, to.getLength());
        } else {
            out.write(o.toString().getBytes(utf8));
        }
    }

    @Override
    public void write(Text key, Text value) throws IOException {
        boolean nullKey = (key == null);
        boolean nullValue = (value == null);
        if (nullKey && nullValue) {
            return;
        }
        if (!nullKey) {
            writeObject(key);
        }
        if (!(nullKey || nullValue)) {
            out.write(keyValueSeparator);
        }
        if (!nullValue) {
            writeObject(value);
        }
        out.write(newline);
    }

    @Override
    public void close(BSPJob job) throws IOException {
        out.close();
    }
}