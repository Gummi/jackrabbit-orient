package de.eiswind.jackrabbit.persistence.orient;

import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import org.apache.commons.io.IOUtils;
import org.apache.jackrabbit.core.id.NodeId;
import org.apache.jackrabbit.core.id.PropertyId;
import org.apache.jackrabbit.core.persistence.util.NodePropBundle;
import org.apache.jackrabbit.core.value.InternalValue;
import org.apache.jackrabbit.spi.Name;
import org.apache.jackrabbit.spi.commons.name.NameFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

/**
 * (C) 2013 by Thomas Kratz
 *  this is experimental code. if you reuse it, you do at your own risk
 *  if you modify and/or or republish it, you must publish the original authors name with your source code.
 */

/**
 * Takes care of mapping the NodePropBundles to OrientDB Documents and back
 */
public class BundleMapper {

    public static final String VALUE = "value";
    private static Logger log = LoggerFactory.getLogger(BundleMapper.class);

    static final NodeId NULL_PARENT_ID =
            new NodeId("bb4e9d10-d857-11df-937b-0800200c9a66");

    private static final long minBlobSize = 1024;

    private ODocument doc;
    private ODatabaseRecord database;
    private NodePropBundle bundle;
    private String bundleClassName;

    public BundleMapper(ODocument doc, ODatabaseRecord database, String bundleClassName) {
        this.bundleClassName = bundleClassName;

        this.doc = doc;
        this.database = database;
    }

    public void writePhase1(NodePropBundle _bundle) throws IOException {
        this.bundle = _bundle;
        doc.field("primaryType", writeName(bundle.getNodeTypeName()), OType.EMBEDDED);
        NodeId parentId = bundle.getParentId();
        if (parentId == null) {
            parentId = NULL_PARENT_ID;
        }
        doc.field("parentuuid", parentId.toString());
        doc.field("uuid", bundle.getId().toString());
//        System.out.println("Wrote " + bundle.getId());
        doc.field("modCount", bundle.getModCount());

        List<ODocument> mixinDocs = new ArrayList<ODocument>();
        for (Name name : bundle.getMixinTypeNames()) {
            mixinDocs.add(writeName(name));
        }
        doc.field("mixinTypes", mixinDocs, OType.EMBEDDEDLIST);
        List<ODocument> propertyDocs = new ArrayList<ODocument>();
        for (NodePropBundle.PropertyEntry entry : bundle.getPropertyEntries()) {
            propertyDocs.add(writeProperty(entry));
        }
        doc.field("properties", propertyDocs, OType.EMBEDDEDLIST);

        List<ODocument> sharedDoc = new ArrayList<ODocument>();
        for (NodeId shared : bundle.getSharedSet()) {
            ODocument shareddoc = new ODocument();
            shareddoc.field("uuid", shared.toString());
            sharedDoc.add(shareddoc);
        }
        doc.field("sharedSet", sharedDoc, OType.EMBEDDEDLIST);

        List<ODocument> childDocs = new ArrayList<ODocument>();
        for (NodePropBundle.ChildNodeEntry child : bundle.getChildNodeEntries()) {
            ODocument childDoc = database.newInstance();
            ODocument name = writeName(child.getName());
            childDoc.field("name", name, OType.EMBEDDED);
            childDoc.field("uuid", child.getId().toString(), OType.STRING);
            childDocs.add(childDoc);
        }
        doc.field("children", childDocs, OType.EMBEDDEDLIST);
    }

    private ODocument writeName(Name name) {
        ODocument nDoc = database.newInstance();
        nDoc.field("local", name.getLocalName());
        nDoc.field("uri", name.getNamespaceURI());
        return nDoc;
    }

    private Name readName(ODocument nDoc) {
        String local = nDoc.field("local", OType.STRING);
        String uri = nDoc.field("uri", OType.STRING);
        return NameFactoryImpl.getInstance().create(uri, local);

    }

    public NodePropBundle read() {
        String uuid = doc.field("uuid", OType.STRING);
        bundle = new NodePropBundle(NodeId.valueOf(uuid));

        String parentUUID = doc.field("parentuuid", OType.STRING);
        NodeId parentId = NodeId.valueOf(parentUUID);
        if (NULL_PARENT_ID.equals(parentId)) {
           bundle.setParentId(null);
        } else {

            bundle.setParentId(parentId);
        }
        ODocument primaryTypeDoc = doc.field("primaryType", OType.EMBEDDED);

        Name name = readName(primaryTypeDoc);
        bundle.setNodeTypeName(name);

        List<ODocument> mixinDocs = doc.field("mixinTypes", OType.EMBEDDEDLIST);
        Set<Name> mixins = new HashSet<Name>();
        for (ODocument mDoc : mixinDocs) {
            mixins.add(readName(mDoc));
        }
        bundle.setMixinTypeNames(mixins);

        List<ODocument> propertyDocs = doc.field("properties", OType.EMBEDDEDLIST);
        if (propertyDocs != null) {
            for (ODocument pDoc : propertyDocs) {
                bundle.addProperty(readProperty(pDoc, bundle));
            }
        }
        // read child refs
        List<ODocument> childDocs = doc.field("children", OType.EMBEDDEDLIST);
        for (ODocument childDoc : childDocs) {
            ODocument nameDoc = childDoc.field("name", OType.EMBEDDED);
            Name childname = readName(nameDoc);
            String childuuid = childDoc.field("uuid", OType.STRING);
            NodeId id = NodeId.valueOf(childuuid);
            bundle.addChildNodeEntry(childname, id);

        }
        List<ODocument> sharedDocs = doc.field("sharedSet", OType.EMBEDDEDLIST);
        Set<NodeId> sharedSet = new HashSet<NodeId>();
        for (ODocument sharedDoc : sharedDocs) {
            String shUuid = doc.field("uuid", OType.STRING);
            NodeId shId = NodeId.valueOf(shUuid);
            sharedSet.add(shId);
        }

        bundle.setSharedSet(sharedSet);
        // TODO read sharedSet
        return bundle;
    }

    private NodePropBundle.PropertyEntry readProperty(ODocument pDoc, NodePropBundle _bundle) {
        ODocument nameDoc = pDoc.field("name", OType.EMBEDDED);
        NodePropBundle.PropertyEntry entry = new NodePropBundle.PropertyEntry(new PropertyId(_bundle.getId(), readName(nameDoc)));
        Boolean multiValued = pDoc.field("multiValued", OType.BOOLEAN);
        entry.setMultiValued(multiValued);
        List<InternalValue> values = new ArrayList<InternalValue>();
        List<ODocument> valDocs = pDoc.field("properties", OType.EMBEDDEDLIST);
        if (valDocs != null) {
            for (ODocument vDoc : valDocs) {
                int type = vDoc.field("type", OType.INTEGER);
                switch (type) {
                    case PropertyType.BINARY:
                        Boolean embedded = vDoc.field("embedded", OType.BOOLEAN);
                        if (embedded) {
                            values.add(InternalValue.create((byte[]) vDoc.field("value", OType.BINARY)));
                        } else {
                            ORecordBytes bytes = vDoc.field("value", OType.LINK);

                            values.add(InternalValue.create(bytes.toStream()));
                        }
                        break;
                    case PropertyType.DOUBLE:

                        values.add(InternalValue.create((Double) vDoc.field(VALUE, OType.DOUBLE)));
                        break;

                    case PropertyType.DECIMAL:
                        values.add(InternalValue.create((BigDecimal) vDoc.field(VALUE, OType.DECIMAL)));
                        break;
                    case PropertyType.LONG:
                        values.add(InternalValue.create((Long) vDoc.field(VALUE, OType.LONG)));
                        break;
                    case PropertyType.BOOLEAN:
                        values.add(InternalValue.create((Boolean) vDoc.field(VALUE, OType.BOOLEAN)));
                        break;

                    case PropertyType.NAME:
                        ODocument nDoc = vDoc.field(VALUE, OType.EMBEDDED);
                        values.add(InternalValue.create(readName(nDoc)));
                        break;

                    case PropertyType.WEAKREFERENCE:
                    case PropertyType.REFERENCE:
                        //must be handled later by referenceresolver
                        break;

                    case PropertyType.DATE:
                        values.add(InternalValue.create((Calendar) vDoc.field(VALUE, OType.DATETIME)));
                        break;
                    default:
                        values.add(InternalValue.create((String) vDoc.field(VALUE, OType.STRING)));
                }
            }
        }
        entry.setValues(values.toArray(new InternalValue[]{}));
        return entry;
    }

    private ODocument writeProperty(NodePropBundle.PropertyEntry state) throws IOException {
        ODocument propDoc = database.newInstance();
        propDoc.field("name", writeName(state.getName()), OType.EMBEDDED);
        propDoc.field("multiValued", state.isMultiValued());

        InternalValue[] values = state.getValues();
        List<ODocument> valDocs = new ArrayList<ODocument>();
        for (int i = 0; i < values.length; i++) {
            ODocument valDoc = database.newInstance();
            InternalValue val = values[i];
            valDoc.field("type", state.getType());
            try {
                switch (state.getType()) {
                    case PropertyType.BINARY:

                        long size = val.getLength();

                        valDoc.field("embedded", size < minBlobSize);
                        if (size < minBlobSize) {
                            ByteArrayOutputStream out = new ByteArrayOutputStream((int) size);
                            IOUtils.copy(val.getStream(), out);
                            valDoc.field(VALUE, out.toByteArray(), OType.BINARY);
                        } else {
                            ORecordBytes blob = new ORecordBytes();
                            blob.fromInputStream(val.getStream());
                            blob.save();
                            valDoc.field(VALUE, blob);


                        }
                        break;

                    case PropertyType.DOUBLE:

                        valDoc.field(VALUE, val.getDouble());
                        break;

                    case PropertyType.DECIMAL:
                        valDoc.field(VALUE, val.getDecimal());
                        break;
                    case PropertyType.LONG:
                        valDoc.field(VALUE, val.getLong());
                        break;
                    case PropertyType.BOOLEAN:
                        valDoc.field(VALUE, val.getBoolean());
                        break;

                    case PropertyType.NAME:
                        valDoc.field(VALUE, writeName(val.getName()), OType.EMBEDDED);
                        break;

                    case PropertyType.WEAKREFERENCE:
                    case PropertyType.REFERENCE:
                        //must be handled later by referenceresolver
                        break;

                    case PropertyType.DATE:
                        valDoc.field(VALUE, val.getDate());
                        break;
                    default:
                        valDoc.field(VALUE, val.toString());
                }
            } catch (RepositoryException x) {
                String msg = "Error while storing value. id="
                        + state.getId() + " idx=" + i + " value=" + val;
                log.error(msg, x);
                throw new IOException(msg, x);
            }
            valDocs.add(valDoc);
        }

        propDoc.field("values", valDocs, OType.EMBEDDEDLIST);
        return propDoc;
    }



    protected synchronized ODocument createChild(NodePropBundle.ChildNodeEntry child) {
        try {
            ODocument doc = null;

            doc = new ODocument(bundleClassName);
            doc.field("primaryType", writeName(child.getName()), OType.EMBEDDED);
            NodeId parentId = bundle.getId();
            if (parentId == null) {
                parentId = NULL_PARENT_ID;
            }
            doc.field("parentuuid", parentId.toString());
            doc.field("uuid", child.getId().toString());

            doc.save();
            return doc;

        } catch (Exception e) {
            String msg = "failed to write bundle: " + bundle.getId();
            log.error(msg, e);
            return null;
        }
    }

    private ODocument loadBundleDoc(String uuid) {
        OIndex<OIdentifiable> index = (OIndex<OIdentifiable>) database.getMetadata().getIndexManager().getIndex(bundleClassName + ".uuid");
        OIdentifiable id = index.get(uuid);
        if (id != null) {
            return id.getRecord();
        } else {
            return null;
        }
    }
}
