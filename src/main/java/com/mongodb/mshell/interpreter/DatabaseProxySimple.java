package com.mongodb.mshell.interpreter;

import com.mongodb.mshell.translator.MongoCommandTranslator;
import org.mozilla.javascript.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class DatabaseProxySimple extends ScriptableObject {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseProxySimple.class);
    private final MongoCommandTranslator translator;
    private final Map<String, CollectionProxySimple> collections = new HashMap<>();
    
    public DatabaseProxySimple(MongoCommandTranslator translator) {
        this.translator = translator;
    }
    
    @Override
    public String getClassName() {
        return "Database";
    }
    
    @Override
    public Object get(String name, Scriptable start) {
        logger.debug("DatabaseProxySimple.get: {}", name);
        
        // Check for built-in database methods first
        switch (name) {
            case "getName":
                return new BaseFunction() {
                    @Override
                    public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
                        return translator.getCurrentDatabaseName();
                    }
                };
                
            case "getCollectionNames":
                return new BaseFunction() {
                    @Override
                    public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
                        return translator.getCollectionNames();
                    }
                };
                
            case "createCollection":
                return new BaseFunction() {
                    @Override
                    public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
                        if (args.length > 0) {
                            return translator.createCollection(args[0].toString());
                        }
                        return "Collection name required";
                    }
                };
                
            case "dropDatabase":
                return new BaseFunction() {
                    @Override
                    public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
                        return translator.dropDatabase();
                    }
                };
                
            case "stats":
                return new BaseFunction() {
                    @Override
                    public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
                        return translator.getDatabaseStats();
                    }
                };
                
            case "runCommand":
                return new BaseFunction() {
                    @Override
                    public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
                        if (args.length > 0) {
                            return translator.runCommand(args[0]);
                        }
                        return "Command required";
                    }
                };
                
            case "getCollection":
                return new BaseFunction() {
                    @Override
                    public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
                        if (args.length > 0) {
                            String collectionName = args[0].toString();
                            CollectionProxySimple collection = new CollectionProxySimple(translator, collectionName);
                            collection.setParentScope(thisObj.getParentScope());
                            collection.setPrototype(ScriptableObject.getObjectPrototype(thisObj.getParentScope()));
                            return collection;
                        }
                        return "Collection name required";
                    }
                };
                
            default:
                // Return a collection proxy for any other property access
                return collections.computeIfAbsent(name, k -> {
                    CollectionProxySimple collection = new CollectionProxySimple(translator, k);
                    collection.setParentScope(this.getParentScope());
                    collection.setPrototype(ScriptableObject.getObjectPrototype(this.getParentScope()));
                    return collection;
                });
        }
    }
    
    @Override
    public boolean has(String name, Scriptable start) {
        return true; // Allow access to any collection name
    }
    
    @Override
    public String toString() {
        return translator.getCurrentDatabaseName();
    }
}