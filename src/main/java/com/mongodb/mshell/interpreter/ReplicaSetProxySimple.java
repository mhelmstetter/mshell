package com.mongodb.mshell.interpreter;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.mozilla.javascript.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;

public class ReplicaSetProxySimple extends ScriptableObject {
    private static final Logger logger = LoggerFactory.getLogger(ReplicaSetProxySimple.class);
    private final MongoClient mongoClient;

    public ReplicaSetProxySimple(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    @Override
    public String getClassName() {
        return "ReplicaSet";
    }

    @Override
    public Object get(String name, Scriptable start) {
        logger.debug("ReplicaSetProxySimple.get: {}", name);

        switch (name) {
            case "status":
                return new BaseFunction() {
                    @Override
                    public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
                        return getReplicaSetStatus();
                    }
                };

            case "conf":
                return new BaseFunction() {
                    @Override
                    public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
                        return getReplicaSetConfig();
                    }
                };

            case "isMaster":
                return new BaseFunction() {
                    @Override
                    public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
                        return isMaster();
                    }
                };

            case "initiate":
                return new BaseFunction() {
                    @Override
                    public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
                        Document config = null;
                        if (args.length > 0 && args[0] instanceof NativeObject) {
                            config = convertNativeObjectToDocument((NativeObject)args[0]);
                        }
                        return initiateReplicaSet(config);
                    }
                };

            case "stepDown":
                return new BaseFunction() {
                    @Override
                    public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
                        int stepDownSecs = 60;
                        if (args.length > 0) {
                            stepDownSecs = ((Number)args[0]).intValue();
                        }
                        return stepDown(stepDownSecs);
                    }
                };

            default:
                return super.get(name, start);
        }
    }

    private NativeObject getReplicaSetStatus() {
        try {
            MongoDatabase adminDb = mongoClient.getDatabase("admin");
            Document result = adminDb.runCommand(new Document("replSetGetStatus", 1));
            return convertDocumentToNativeObject(result);
        } catch (Exception e) {
            logger.error("Failed to get replica set status", e);
            NativeObject error = new NativeObject();
            error.put("ok", error, 0);
            error.put("errmsg", error, e.getMessage());
            return error;
        }
    }

    private NativeObject getReplicaSetConfig() {
        try {
            MongoDatabase adminDb = mongoClient.getDatabase("admin");
            Document result = adminDb.runCommand(new Document("replSetGetConfig", 1));
            return convertDocumentToNativeObject(result);
        } catch (Exception e) {
            logger.error("Failed to get replica set configuration", e);
            NativeObject error = new NativeObject();
            error.put("ok", error, 0);
            error.put("errmsg", error, e.getMessage());
            return error;
        }
    }

    private NativeObject isMaster() {
        try {
            MongoDatabase adminDb = mongoClient.getDatabase("admin");
            Document result = adminDb.runCommand(new Document("isMaster", 1));
            return convertDocumentToNativeObject(result);
        } catch (Exception e) {
            logger.error("Failed to run isMaster", e);
            NativeObject error = new NativeObject();
            error.put("ok", error, 0);
            error.put("errmsg", error, e.getMessage());
            return error;
        }
    }

    private NativeObject initiateReplicaSet(Document config) {
        try {
            MongoDatabase adminDb = mongoClient.getDatabase("admin");
            Document command = new Document("replSetInitiate", config != null ? config : 1);
            Document result = adminDb.runCommand(command);
            return convertDocumentToNativeObject(result);
        } catch (Exception e) {
            logger.error("Failed to initiate replica set", e);
            NativeObject error = new NativeObject();
            error.put("ok", error, 0);
            error.put("errmsg", error, e.getMessage());
            return error;
        }
    }

    private NativeObject stepDown(int stepDownSecs) {
        try {
            MongoDatabase adminDb = mongoClient.getDatabase("admin");
            Document command = new Document("replSetStepDown", stepDownSecs);
            Document result = adminDb.runCommand(command);
            return convertDocumentToNativeObject(result);
        } catch (Exception e) {
            logger.error("Failed to step down", e);
            NativeObject error = new NativeObject();
            error.put("ok", error, 0);
            error.put("errmsg", error, e.getMessage());
            return error;
        }
    }

    private NativeObject convertDocumentToNativeObject(Document doc) {
        NativeObject obj = new NativeObject();
        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof Document) {
                value = convertDocumentToNativeObject((Document) value);
            } else if (value instanceof java.util.List) {
                value = convertListToNativeArray((java.util.List<?>) value);
            }
            obj.put(entry.getKey(), obj, value);
        }
        return obj;
    }

    private NativeArray convertListToNativeArray(java.util.List<?> list) {
        NativeArray array = new NativeArray(list.size());
        for (int i = 0; i < list.size(); i++) {
            Object value = list.get(i);
            if (value instanceof Document) {
                value = convertDocumentToNativeObject((Document) value);
            } else if (value instanceof java.util.List) {
                value = convertListToNativeArray((java.util.List<?>) value);
            }
            array.put(i, array, value);
        }
        return array;
    }

    private Document convertNativeObjectToDocument(NativeObject obj) {
        Document doc = new Document();
        for (Object id : obj.getIds()) {
            String key = id.toString();
            Object value = obj.get(key, obj);
            if (value instanceof NativeObject) {
                value = convertNativeObjectToDocument((NativeObject) value);
            } else if (value instanceof NativeArray) {
                value = convertNativeArrayToList((NativeArray) value);
            }
            doc.append(key, value);
        }
        return doc;
    }

    private java.util.List<Object> convertNativeArrayToList(NativeArray array) {
        java.util.List<Object> list = new java.util.ArrayList<>();
        for (int i = 0; i < array.getLength(); i++) {
            Object value = array.get(i, array);
            if (value instanceof NativeObject) {
                value = convertNativeObjectToDocument((NativeObject) value);
            } else if (value instanceof NativeArray) {
                value = convertNativeArrayToList((NativeArray) value);
            }
            list.add(value);
        }
        return list;
    }
}