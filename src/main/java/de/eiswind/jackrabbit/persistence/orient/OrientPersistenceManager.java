/*
 * (c) 2013 by Thomas  Kratz
 */
package de.eiswind.jackrabbit.persistence.orient;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.query.OQuery;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.apache.jackrabbit.core.fs.FileSystem;
import org.apache.jackrabbit.core.fs.FileSystemException;
import org.apache.jackrabbit.core.id.NodeId;
import org.apache.jackrabbit.core.id.PropertyId;
import org.apache.jackrabbit.core.persistence.PMContext;
import org.apache.jackrabbit.core.persistence.bundle.AbstractBundlePersistenceManager;
import org.apache.jackrabbit.core.persistence.util.*;
import org.apache.jackrabbit.core.state.ChangeLog;
import org.apache.jackrabbit.core.state.ItemStateException;
import org.apache.jackrabbit.core.state.NoSuchItemStateException;
import org.apache.jackrabbit.core.state.NodeReferences;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.PropertyType;
import java.sql.SQLException;
import java.util.*;
import java.util.function.Function;

/**
 * (C) 2013 by Thomas Kratz
 *  this is experimental code. if you reuse it, you do so at your own risk
 *  if you modify and/or or republish it, you must publish the original authors name with your source code.
 */

/**
 * This is a generic persistence manager that stores the {@link NodePropBundle}s
 * in an orient db datastore.
 * <p>
 */
public class OrientPersistenceManager extends AbstractBundlePersistenceManager {

    /**
     * the default logger
     */
    private static Logger log = LoggerFactory.getLogger(OrientPersistenceManager.class);

    /**
     * flag indicating if this manager was initialized
     */
    protected boolean initialized;

    /**
     * prefix for orient classnames
     */
    protected String objectPrefix;

    private String url;
    private String user = "admin";
    private String pass = "admin";
    private boolean createDB = true;


    private ODatabaseDocumentPool pool;

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getSchemaObjectPrefix() {
        return objectPrefix;
    }

    public void setSchemaObjectPrefix(String objectPrefix) {
        this.objectPrefix = objectPrefix;

    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPass() {
        return pass;
    }

    public void setPass(String pass) {
        this.pass = pass;
    }

    /**
     * file system where BLOB data is stored
     */
    protected OrientPersistenceManager.CloseableBLOBStore blobStore;


    private int blobFSBlockSize;

    /**
     * the minimum size of a property until it gets written to the blob store
     */
    private int minBlobSize = 0x1000;

    /**
     * the filesystem where the items are stored
     */
    private FileSystem itemFs;


    /**
     * the bundle binding
     */
    protected BundleBinding binding;


    /**
     * flag for error handling
     */
    protected ErrorHandling errorHandling = new ErrorHandling();

    /**
     * the name of this persistence manager
     */
    private String name = super.toString();


    private String bundleClassName;
    private String refsClassName;


    public String getObjectPrefix() {
        return objectPrefix;
    }

    public void setObjectPrefix(String objectPrefix) {
        this.objectPrefix = objectPrefix;

    }

    private Map<NodeId, BundleMapper> documentMap = new HashMap<NodeId, BundleMapper>();

    /**
     * Sets the error handling behaviour of this manager. See {@link ErrorHandling}
     * for details about the flags.
     *
     * @param errorHandling
     */
    public void setErrorHandling(String errorHandling) {
        this.errorHandling = new ErrorHandling(errorHandling);
    }

    /**
     * Returns the error handling configuration of this manager
     *
     * @return the error handling configuration of this manager
     */
    public String getErrorHandling() {
        return errorHandling.toString();
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected BLOBStore getBlobStore() {
        return blobStore;
    }

    public boolean useLocalFsBlobStore() {
        return blobFSBlockSize == 0;
    }

    /**
     * {@inheritDoc}
     */
    public void init(PMContext context) throws Exception {
        if (initialized) {
            throw new IllegalStateException("already initialized");
        }
        OGlobalConfiguration.STORAGE_KEEP_OPEN.setValue(false);
        ODatabaseDocumentTx db;

        db = new ODatabaseDocumentTx(url);

        if (!db.exists()) {
            db.create();

        }

        pool = new ODatabaseDocumentPool(url, "admin", "admin");
        pool.setup(1, 50);
        super.init(context);

        // load namespaces
        binding = new BundleBinding(errorHandling, blobStore, getNsIndex(), getNameIndex(), context.getDataStore());
        binding.setMinBlobSize(minBlobSize);


        this.name = context.getHomeDir().getName();


        runWithDatabse(database -> {

            OSchema schema = database.getMetadata().getSchema();
            bundleClassName = getSchemaObjectPrefix() + name + "Bundle";
            refsClassName = getSchemaObjectPrefix() + name + "Refs";
            OClass bundleClass = schema.getClass(bundleClassName);
            OClass vertexClass = schema.getClass("V");
            if (bundleClass == null) {
                bundleClass = schema.createClass(bundleClassName, vertexClass);
                OProperty id = bundleClass.createProperty("uuid", OType.STRING);
                id.createIndex(OClass.INDEX_TYPE.UNIQUE);
                schema.save();
            }

            OClass refsClass = schema.getClass(refsClassName);
            if (refsClass == null) {
                refsClass = schema.createClass(refsClassName, vertexClass);
                OProperty id = refsClass.createProperty("targetuuid", OType.STRING);
                id.createIndex(OClass.INDEX_TYPE.UNIQUE);
                schema.save();
            }
            return null;
        });


        initialized = true;
    }

    public synchronized void store(ChangeLog changeLog)
            throws ItemStateException {
        documentMap.clear();
        runWithDatabse(db -> {

            try {

                super.store(changeLog);
            } catch (ItemStateException e) {
                log.error("jackrabbit", e);
            }
            return null;
        });

        documentMap.clear();
    }


    protected Object runWithDatabse(Function<ODatabaseRecord, Object> function) {

        ODatabaseDocumentTx closeDB = null;
        ODatabaseRecord database = null;
        if (!ODatabaseRecordThreadLocal.INSTANCE.isDefined()) {


            closeDB = pool.acquire(url, user, pass);
            database = closeDB;


        } else {
            database = ODatabaseRecordThreadLocal.INSTANCE.get();
            if (database.isClosed()) {
                database.open(user, pass);
            }
        }

        Object result = null;
        try {
            database.getTransaction().begin();
            result = function.apply(database);
            database.getTransaction().commit();
        } catch (OException x) {
            database.getTransaction().rollback();
            log.error("DB error", x);
            throw x;
        } finally {
            if (closeDB != null) {

                pool.release(closeDB);

            }
        }

        return result;

    }

    /**
     * {@inheritDoc}
     */
    public synchronized void close() throws Exception {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        }

        try {

            pool.close();

            super.close();
        } finally {
            initialized = false;
        }
    }

    /**
     * {@inheritDoc}
     */
    protected NodePropBundle loadBundle(NodeId id) throws ItemStateException {
        try {
            return (NodePropBundle) runWithDatabse(database -> {
                String uuid = id.toString();

                ODocument doc = loadBundleDoc(uuid);
                if (doc == null) {
                    return null;
                }

                BundleMapper mapper = new BundleMapper(doc, database, bundleClassName);
                NodePropBundle bundle = mapper.read();
                return bundle;
            });
        } catch (Exception e) {
            String msg = "failed to read bundle: " + id + ": " + e;
            log.error(msg);
            throw new ItemStateException(msg, e);
        }
    }

    /**
     * Creates the file path for the given node id that is
     * suitable for storing node states in a filesystem.
     *
     * @param buf buffer to append to or <code>null</code>
     * @param id  the id of the node
     * @return the buffer with the appended data.
     */
    protected StringBuffer buildNodeFilePath(StringBuffer buf, NodeId id) {
        if (buf == null) {
            buf = new StringBuffer();
        }
        buildNodeFolderPath(buf, id);
        buf.append('.');
        buf.append(NODEFILENAME);
        return buf;
    }

    /**
     * Creates the file path for the given references id that is
     * suitable for storing reference states in a filesystem.
     *
     * @param buf buffer to append to or <code>null</code>
     * @param id  the id of the node
     * @return the buffer with the appended data.
     */
    protected StringBuffer buildNodeReferencesFilePath(
            StringBuffer buf, NodeId id) {
        if (buf == null) {
            buf = new StringBuffer();
        }
        buildNodeFolderPath(buf, id);
        buf.append('.');
        buf.append(NODEREFSFILENAME);
        return buf;
    }

    /**
     * {@inheritDoc}
     */
    protected synchronized void storeBundle(NodePropBundle bundle) throws ItemStateException {
        runWithDatabse(database -> {

            try {
                ODocument vertex = null;
                if (bundle.isNew()) {

                    vertex = new ODocument(bundleClassName);
                } else {
                    vertex = loadBundleDoc(bundle.getId().toString());
                }
                if (vertex == null) {
                    throw new IllegalStateException("FATAL: Tried to update non existing bundle" + bundle.getId().toString());
                }
                BundleMapper mapper = new BundleMapper(vertex, database, bundleClassName);
                mapper.writePhase1(bundle);
                vertex.save();
                NodeId id = bundle.getId();
                // store this for phase2

                documentMap.put(id, mapper);

            } catch (Exception e) {
                String msg = "failed to write bundle: " + bundle.getId();
                OrientPersistenceManager.log.error(msg, e);

            }
            return null;
        });

    }

    /**
     * {@inheritDoc}
     */
    protected synchronized void destroyBundle(NodePropBundle bundle) throws ItemStateException {

        runWithDatabse(database -> {
            try {
                String uuid = bundle.getId().toString();
                ODocument result = loadBundleDoc(uuid);
                if (result == null) {
                    throw new NullPointerException(uuid + " is missing");
                }
                cascadeDeleteToBlobs(result);

                result.delete();
            } catch (Exception e) {
                String msg = "failed to delete bundle: " + bundle.getId();
                OrientPersistenceManager.log.error(msg, e);

            }
            return null;
        });
    }

    private void cascadeDeleteToBlobs(ODocument result) {
        List<ODocument> propertyDocs = result.field("properties", OType.EMBEDDEDLIST);
        if (propertyDocs != null) {
            for (ODocument pDoc : propertyDocs) {
                List<ODocument> valDocs = pDoc.field("properties", OType.EMBEDDEDLIST);
                if (valDocs != null) {
                    for (ODocument vDoc : valDocs) {
                        int type = vDoc.field("type", OType.INTEGER);
                        if (PropertyType.BINARY == type) {
                            Boolean embedded = vDoc.field("embedded", OType.BOOLEAN);
                            if (!embedded) {
                                for (OIdentifiable id : (List<OIdentifiable>) vDoc.field("value")) {
                                    ORecordBytes chunk = id.getRecord();
                                    chunk.delete();
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private ODocument loadBundleDoc(String uuid) {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        }
        return (ODocument) runWithDatabse(database -> {
            OIndex<OIdentifiable> index = (OIndex<OIdentifiable>) database.getMetadata().getIndexManager().getIndex(bundleClassName + ".uuid");
            OIdentifiable id = index.get(uuid);
            ODocument doc = null;
            if (id != null) {
                doc = id.getRecord();
            }

            return doc;
        });


    }

    private ODocument loadRefsDoc(String targetuuid) {

        return (ODocument) runWithDatabse(database -> {
            OQuery<ODocument> query = new OSQLSynchQuery<ODocument>("select from " + refsClassName + " WHERE targetuuid = '" + targetuuid + "'");
            List<ODocument> result = database.query(query);
            if (result.size() == 0) {
                return null;
            }
            // result must be unique since we have the index
            return result.get(0);
        });
    }

    /**
     * {@inheritDoc}
     */
    public synchronized NodeReferences loadReferencesTo(NodeId targetId)
            throws NoSuchItemStateException, ItemStateException {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        }
        return (NodeReferences) runWithDatabse(database -> {
            ODocument refsDoc = loadRefsDoc(targetId.toString());
            if (refsDoc == null) {
                throw new NullPointerException(targetId.toString());
            }
            NodeReferences refs = new NodeReferences(targetId);
            List<ODocument> refDocs = refsDoc.field("refs", OType.EMBEDDEDLIST);
            for (ODocument rDoc : refDocs) {
                String value = rDoc.field("ref", OType.STRING);
                refs.addReference(PropertyId.valueOf(value));
            }
            return refs;
        });
    }

    /**
     * {@inheritDoc}
     */
    public synchronized void store(NodeReferences refs)
            throws ItemStateException {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        }
        runWithDatabse(database -> {
            ODocument refsDoc = loadRefsDoc(refs.getTargetId().toString());
            if (refsDoc == null) {
                refsDoc = new ODocument(refsClassName);
                refsDoc.field("targetuuid", refs.getTargetId().toString(), OType.STRING);

            }
            List<ODocument> refDocs = new ArrayList<ODocument>();
            for (PropertyId propId : refs.getReferences()) {
                ODocument refDoc = database.newInstance();
                refDoc.field("ref", propId.toString(), OType.STRING);
                refDocs.add(refDoc);

            }
            refsDoc.field("refs", refDocs, OType.EMBEDDEDLIST);
            refsDoc.save();
            return null;
        });
    }

    /**
     * {@inheritDoc}
     */
    public synchronized void destroy(NodeReferences refs) throws ItemStateException {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        }
        runWithDatabse(database -> {
            ODocument doc = loadRefsDoc(refs.getTargetId().toString());
            if (doc != null) {
                doc.delete();
            }
            return null;
        });
    }

    /**
     * {@inheritDoc}
     */
    public synchronized boolean existsReferencesTo(NodeId targetId) throws ItemStateException {
        if (!initialized) {
            throw new IllegalStateException("not initialized");
        }
        return (Boolean) runWithDatabse(database -> {
            ODocument doc = loadRefsDoc(targetId.toString());
            return doc != null;
        });
    }

    /**
     * logs an sql exception
     *
     * @param message
     * @param se
     */
    protected void logException(String message, SQLException se) {
        if (message != null) {
            OrientPersistenceManager.log.error(message);
        }
        OrientPersistenceManager.log.error("       Reason: " + se.getMessage());
        OrientPersistenceManager.log.error(
                "   State/Code: " + se.getSQLState() + "/" + se.getErrorCode());
        OrientPersistenceManager.log.debug("   dump:", se);
    }

    /**
     * @inheritDoc
     */
    public String toString() {
        return name;
    }

    public void setCreateDB(boolean createDB) {
        this.createDB = createDB;
    }

    /**
     * Helper interface for closeable stores
     */
    protected static interface CloseableBLOBStore extends BLOBStore {
        void close();
    }

    /**
     * own implementation of the filesystem blob store that uses a different
     * blob-id scheme.
     */
    private class FSBlobStore extends FileSystemBLOBStore implements OrientPersistenceManager.CloseableBLOBStore {

        private FileSystem fs;

        public FSBlobStore(FileSystem fs) {
            super(fs);
            this.fs = fs;
        }

        public String createId(PropertyId id, int index) {
            return buildBlobFilePath(null, id, index).toString();
        }

        public void close() {
            try {
                fs.close();
                fs = null;
            } catch (Exception e) {
                // ignore
            }
        }

    }

    /**
     * {@inheritDoc}
     */
    public List<NodeId> getAllNodeIds(NodeId bigger, int maxCount)
            throws ItemStateException {
        ArrayList<NodeId> list = new ArrayList<NodeId>();
        try {
            getListRecursive(list, "", bigger == null ? null : bigger, maxCount);
            return list;
        } catch (FileSystemException e) {
            String msg = "failed to read node list: " + bigger + ": " + e;
            log.error(msg);
            throw new ItemStateException(msg, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    protected NodeId getIdFromFileName(String fileName) {
        StringBuffer buff = new StringBuffer(35);
        if (!fileName.endsWith("." + NODEFILENAME)) {
            return null;
        }
        for (int i = 0; i < fileName.length(); i++) {
            char c = fileName.charAt(i);
            if (c == '.') {
                break;
            }
            if (c != '/') {
                buff.append(c);
                int len = buff.length();
                if (len == 8 || len == 13 || len == 18 || len == 23) {
                    buff.append('-');
                }
            }
        }
        return new NodeId(buff.toString());
    }

    private void getListRecursive(
            ArrayList<NodeId> list, String path, NodeId bigger, int maxCount)
            throws FileSystemException {
        if (maxCount > 0 && list.size() >= maxCount) {
            return;
        }
        String[] files = itemFs.listFiles(path);
        Arrays.sort(files);
        for (int i = 0; i < files.length; i++) {
            String f = files[i];
            NodeId n = getIdFromFileName(path + FileSystem.SEPARATOR + f);
            if (n == null) {
                continue;
            }
            if (bigger != null && bigger.toString().compareTo(n.toString()) >= 0) {
                continue;
            }
            list.add(n);
            if (maxCount > 0 && list.size() >= maxCount) {
                return;
            }
        }
        String[] dirs = itemFs.listFolders(path);
        Arrays.sort(dirs);
        for (int i = 0; i < dirs.length; i++) {
            getListRecursive(list, path + FileSystem.SEPARATOR + dirs[i],
                    bigger, maxCount);
        }
    }

}
