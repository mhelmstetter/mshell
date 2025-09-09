package com.mongodb.mshell.interpreter;

import com.mongodb.mshell.translator.MongoCommandTranslator;
import org.mozilla.javascript.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CollectionProxySimple extends ScriptableObject {
    private static final Logger logger = LoggerFactory.getLogger(CollectionProxySimple.class);
    private final MongoCommandTranslator translator;
    private final String collectionName;
    
    public CollectionProxySimple(MongoCommandTranslator translator, String collectionName) {
        this.translator = translator;
        this.collectionName = collectionName;
    }
    
    @Override
    public String getClassName() {
        return "Collection";
    }
    
    @Override
    public Object get(String name, Scriptable start) {
        logger.debug("CollectionProxySimple.get: {}.{}", collectionName, name);
        
        switch (name) {
            case "find":
                return new BaseFunction() {
                    @Override
                    public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
                        Object filter = args.length > 0 ? args[0] : null;
                        Object projection = args.length > 1 ? args[1] : null;
                        return new CursorProxySimple(translator, collectionName, filter, projection);
                    }
                };
                
            case "findOne":
                return new BaseFunction() {
                    @Override
                    public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
                        Object filter = args.length > 0 ? args[0] : null;
                        Object projection = args.length > 1 ? args[1] : null;
                        return translator.findOne(collectionName, filter, projection);
                    }
                };
                
            case "insert":
            case "insertOne":
                return new BaseFunction() {
                    @Override
                    public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
                        if (args.length > 0) {
                            return translator.insertOne(collectionName, args[0]);
                        }
                        return "Document required";
                    }
                };
                
            case "insertMany":
                return new BaseFunction() {
                    @Override
                    public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
                        if (args.length > 0) {
                            return translator.insertMany(collectionName, args[0]);
                        }
                        return "Documents array required";
                    }
                };
                
            case "count":
                return new BaseFunction() {
                    @Override
                    public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
                        if (args.length > 0 && args[0] != null) {
                            // If filter is provided, use countDocuments
                            return translator.countDocuments(collectionName, args[0]);
                        } else {
                            // No filter, use estimatedDocumentCount for better performance
                            return translator.estimatedDocumentCount(collectionName);
                        }
                    }
                };
                
            case "countDocuments":
                return new BaseFunction() {
                    @Override
                    public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
                        Object filter = args.length > 0 ? args[0] : null;
                        return translator.countDocuments(collectionName, filter);
                    }
                };
                
            default:
                return super.get(name, start);
        }
    }
    
    @Override
    public boolean has(String name, Scriptable start) {
        return true;
    }
    
    public CursorProxySimple find(Object filter, Object projection) {
        return new CursorProxySimple(translator, collectionName, filter, projection);
    }
    
    public CursorProxySimple find(Object filter) {
        return find(filter, null);
    }
    
    public CursorProxySimple find() {
        return find(null, null);
    }
    
    public Object findOne(Object filter, Object projection) {
        return translator.findOne(collectionName, filter, projection);
    }
    
    public Object findOne(Object filter) {
        return findOne(filter, null);
    }
    
    public Object findOne() {
        return findOne(null, null);
    }
    
    public String insert(Object document) {
        return translator.insertOne(collectionName, document);
    }
    
    public String insertOne(Object document) {
        return translator.insertOne(collectionName, document);
    }
    
    public String insertMany(Object documents) {
        return translator.insertMany(collectionName, documents);
    }
    
    public String update(Object filter, Object update, boolean upsert) {
        return translator.updateOne(collectionName, filter, update, upsert);
    }
    
    public String update(Object filter, Object update) {
        return update(filter, update, false);
    }
    
    public String updateOne(Object filter, Object update, boolean upsert) {
        return translator.updateOne(collectionName, filter, update, upsert);
    }
    
    public String updateOne(Object filter, Object update) {
        return updateOne(filter, update, false);
    }
    
    public String updateMany(Object filter, Object update, boolean upsert) {
        return translator.updateMany(collectionName, filter, update, upsert);
    }
    
    public String updateMany(Object filter, Object update) {
        return updateMany(filter, update, false);
    }
    
    public String remove(Object filter) {
        return translator.deleteOne(collectionName, filter);
    }
    
    public String deleteOne(Object filter) {
        return translator.deleteOne(collectionName, filter);
    }
    
    public String deleteMany(Object filter) {
        return translator.deleteMany(collectionName, filter);
    }
    
    public long count(Object filter) {
        return translator.countDocuments(collectionName, filter);
    }
    
    public long count() {
        // Use estimatedDocumentCount when no filter is provided for better performance
        return translator.estimatedDocumentCount(collectionName);
    }
    
    public long countDocuments(Object filter) {
        return translator.countDocuments(collectionName, filter);
    }
    
    public long countDocuments() {
        return countDocuments(null);
    }
    
    public Object aggregate(Object pipeline) {
        return translator.aggregate(collectionName, pipeline);
    }
    
    public Object distinct(String field, Object filter) {
        return translator.distinct(collectionName, field, filter);
    }
    
    public Object distinct(String field) {
        return distinct(field, null);
    }
    
    public String drop() {
        return translator.dropCollection(collectionName);
    }
    
    public String createIndex(Object keys, Object options) {
        return translator.createIndex(collectionName, keys, options);
    }
    
    public String createIndex(Object keys) {
        return createIndex(keys, null);
    }
    
    public Object getIndexes() {
        return translator.getIndexes(collectionName);
    }
    
    public Object stats() {
        return translator.getCollectionStats(collectionName);
    }
}