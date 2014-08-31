package de.eiswind.jackrabbit.persistence.orient;

import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.fs.FileSystem;
import org.apache.jackrabbit.core.fs.FileSystemException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;

/**
 * Created by Thomas on 31.08.2014.
 */
public class BinaryFileSystemHelper {

    private static Logger log = LoggerFactory.getLogger(BinaryFileSystemHelper.class);
    private final FileSystem fileSystem;

    public BinaryFileSystemHelper(FileSystem fileSystem){
        this.fileSystem = fileSystem;
        try {
            if(!fileSystem.exists("docs")){
                fileSystem.createFolder("docs");
            }
        } catch (FileSystemException e) {
           log.error("Create docs",e);
        }
    }

    public String write(InputStream in){
        String id = UUID.randomUUID().toString();
        String folder = id.substring(0,2);
        try {
            if(!fileSystem.exists("docs/"+folder)){
                fileSystem.createFolder("docs/"+folder);
            }
            IOUtils.copy(in,fileSystem.getOutputStream("docs/"+folder+"/"+id));
            return id;
        } catch (IOException|FileSystemException e) {
            log.error("write blob "+id);
            throw new RuntimeException("write blob",e);
        }
    }

    public InputStream read(String id){
        String path = "docs/"+id.substring(0,2)+"/"+id;
        try {
            if(!fileSystem.exists(path)){
                throw new RuntimeException("Blob not found "+path);
            }
            return fileSystem.getInputStream(path);
        } catch (FileSystemException e) {
            log.error("read "+path+" failed");
            throw new RuntimeException("read ",e);
        }
    }

    public void delete(String id){
        String path = "docs/"+id.substring(0,2)+"/"+id;

        try {
            if (!fileSystem.exists(path)) {
                throw new RuntimeException("Blob not found " + path);
            }
            fileSystem.deleteFile(path);
        } catch (FileSystemException e) {
            log.error("delete "+path);
            throw new RuntimeException("delete",e);
        }
    }
}
