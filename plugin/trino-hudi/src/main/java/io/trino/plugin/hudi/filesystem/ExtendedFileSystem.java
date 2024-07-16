package io.trino.plugin.hudi.filesystem;

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
public abstract class ExtendedFileSystem
        extends FileSystem
{
    public FSDataInputStream openFile(Path path, HiveFileContext hiveFileContext)
            throws Exception
    {
        return open(path);
    }
}
