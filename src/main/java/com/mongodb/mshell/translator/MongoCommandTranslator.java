package com.mongodb.mshell.translator;

import com.mongodb.client.*;
import com.mongodb.client.model.*;
import com.mongodb.client.result.*;
import com.mongodb.mshell.cursor.BatchCursor;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.mozilla.javascript.regexp.NativeRegExp;
import org.mozilla.javascript.NativeObject;

import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MongoCommandTranslator {
    private static final Logger logger = LoggerFactory.getLogger(MongoCommandTranslator.class);
    
    private final MongoClient mongoClient;
    private MongoDatabase currentDatabase;
    private final boolean verbose;
    
    public MongoCommandTranslator(MongoClient mongoClient) {
        this(mongoClient, false);
    }
    
    public MongoCommandTranslator(MongoClient mongoClient, boolean verbose) {
        this.mongoClient = mongoClient;
        this.verbose = verbose;
    }
    
    public void setCurrentDatabase(MongoDatabase database) {
        this.currentDatabase = database;
    }
    
    public String getCurrentDatabaseName() {
        return currentDatabase != null ? currentDatabase.getName() : "test";
    }
    
    public List<String> getCollectionNames() {
        if (currentDatabase == null) {
            return Collections.emptyList();
        }
        List<String> names = new ArrayList<>();
        currentDatabase.listCollectionNames().forEach(names::add);
        return names;
    }
    
    public String createCollection(String name) {
        if (currentDatabase == null) {
            return "No database selected";
        }
        currentDatabase.createCollection(name);
        return "Collection created: " + name;
    }
    
    public String dropDatabase() {
        if (currentDatabase == null) {
            return "No database selected";
        }
        String name = currentDatabase.getName();
        currentDatabase.drop();
        return "Database dropped: " + name;
    }
    
    public String dropCollection(String collectionName) {
        if (currentDatabase == null) {
            return "No database selected";
        }
        currentDatabase.getCollection(collectionName).drop();
        return "Collection dropped: " + collectionName;
    }
    
    public Document getDatabaseStats() {
        if (currentDatabase == null) {
            return new Document("error", "No database selected");
        }
        return currentDatabase.runCommand(new Document("dbStats", 1));
    }
    
    public Document getCollectionStats(String collectionName) {
        if (currentDatabase == null) {
            return new Document("error", "No database selected");
        }
        return currentDatabase.runCommand(new Document("collStats", collectionName));
    }
    
    public Object runCommand(Object command) {
        if (currentDatabase == null) {
            return "No database selected";
        }
        Document cmd = toDocument(command);
        return currentDatabase.runCommand(cmd);
    }
    
    public List<Document> find(String collectionName, Object filter, Object projection, 
                               Object sort, Integer limit, Integer skip) {
        if (currentDatabase == null) {
            return Collections.emptyList();
        }
        
        MongoCollection<Document> collection = currentDatabase.getCollection(collectionName);
        FindIterable<Document> iterable = collection.find(toDocument(filter));
        
        if (projection != null) {
            iterable.projection(toDocument(projection));
        }
        if (sort != null) {
            iterable.sort(toDocument(sort));
        }
        if (limit != null) {
            iterable.limit(limit);
        }
        if (skip != null) {
            iterable.skip(skip);
        }
        
        List<Document> results = new ArrayList<>();
        iterable.forEach(results::add);
        return results;
    }
    
    public BatchCursor getBatchCursor(String collectionName, Object filter, Object projection, 
                                      Object sort, Integer limit, Integer skip) {
        if (currentDatabase == null) {
            return null;
        }
        
        MongoCollection<Document> collection = currentDatabase.getCollection(collectionName);
        FindIterable<Document> iterable = collection.find(toDocument(filter));
        
        if (projection != null) {
            iterable.projection(toDocument(projection));
        }
        if (sort != null) {
            iterable.sort(toDocument(sort));
        }
        if (limit != null) {
            iterable.limit(limit);
        }
        if (skip != null) {
            iterable.skip(skip);
        }
        
        return new BatchCursor(iterable);
    }
    
    public Document findOne(String collectionName, Object filter, Object projection) {
        List<Document> results = find(collectionName, filter, projection, null, 1, null);
        return results.isEmpty() ? null : results.get(0);
    }
    
    public String insertOne(String collectionName, Object document) {
        if (currentDatabase == null) {
            return "No database selected";
        }
        
        Document doc = toDocument(document);
        InsertOneResult result = currentDatabase.getCollection(collectionName).insertOne(doc);
        return "Inserted document with _id: " + result.getInsertedId();
    }
    
    public String insertMany(String collectionName, Object documents) {
        if (currentDatabase == null) {
            return "No database selected";
        }
        
        List<Document> docs = toDocumentList(documents);
        InsertManyResult result = currentDatabase.getCollection(collectionName).insertMany(docs);
        return "Inserted " + result.getInsertedIds().size() + " documents";
    }
    
    public String updateOne(String collectionName, Object filter, Object update, boolean upsert) {
        if (currentDatabase == null) {
            return "No database selected";
        }
        
        UpdateOptions options = new UpdateOptions().upsert(upsert);
        Document filterDoc = toDocument(filter);
        Bson updateDoc = toBsonUpdate(update);
        
        UpdateResult result = currentDatabase.getCollection(collectionName)
            .updateOne(filterDoc, updateDoc, options);
            
        return formatUpdateResult(result);
    }
    
    public String updateMany(String collectionName, Object filter, Object update, boolean upsert) {
        if (currentDatabase == null) {
            return "No database selected";
        }
        
        UpdateOptions options = new UpdateOptions().upsert(upsert);
        Document filterDoc = toDocument(filter);
        Bson updateDoc = toBsonUpdate(update);
        
        UpdateResult result = currentDatabase.getCollection(collectionName)
            .updateMany(filterDoc, updateDoc, options);
            
        return formatUpdateResult(result);
    }
    
    public String deleteOne(String collectionName, Object filter) {
        if (currentDatabase == null) {
            return "No database selected";
        }
        
        DeleteResult result = currentDatabase.getCollection(collectionName)
            .deleteOne(toDocument(filter));
            
        return "Deleted " + result.getDeletedCount() + " document(s)";
    }
    
    public String deleteMany(String collectionName, Object filter) {
        if (currentDatabase == null) {
            return "No database selected";
        }
        
        DeleteResult result = currentDatabase.getCollection(collectionName)
            .deleteMany(toDocument(filter));
            
        return "Deleted " + result.getDeletedCount() + " document(s)";
    }
    
    public long countDocuments(String collectionName, Object filter) {
        if (currentDatabase == null) {
            return 0;
        }
        
        Document filterDoc = toDocument(filter);
        if (verbose) {
            System.out.println("VERBOSE: countDocuments query:");
            System.out.println("  Collection: " + currentDatabase.getName() + "." + collectionName);
            try {
                // Use ObjectMapper for better JSON serialization
                com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
                mapper.enable(com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT);
                String filterJson = mapper.writeValueAsString(filterDoc);
                System.out.println("  Filter: " + filterJson);
            } catch (Exception e) {
                // Fallback to Document's toJson()
                try {
                    System.out.println("  Filter: " + filterDoc.toJson());
                } catch (Exception e2) {
                    // Final fallback
                    System.out.println("  Filter: " + filterDoc.toString());
                    logger.debug("Failed to serialize filter for verbose output", e2);
                }
            }
        }
        
        return currentDatabase.getCollection(collectionName)
            .countDocuments(filterDoc);
    }
    
    public List<Document> aggregate(String collectionName, Object pipeline) {
        if (currentDatabase == null) {
            return Collections.emptyList();
        }
        
        List<Bson> pipelineList = toDocumentList(pipeline).stream()
            .map(doc -> (Bson) doc)
            .collect(Collectors.toList());
            
        List<Document> results = new ArrayList<>();
        currentDatabase.getCollection(collectionName)
            .aggregate(pipelineList)
            .forEach(results::add);
            
        return results;
    }
    
    public List<Object> distinct(String collectionName, String field, Object filter) {
        if (currentDatabase == null) {
            return Collections.emptyList();
        }
        
        List<Object> results = new ArrayList<>();
        currentDatabase.getCollection(collectionName)
            .distinct(field, toDocument(filter), Object.class)
            .forEach(results::add);
            
        return results;
    }
    
    public String createIndex(String collectionName, Object keys, Object options) {
        if (currentDatabase == null) {
            return "No database selected";
        }
        
        Document keysDoc = toDocument(keys);
        IndexOptions indexOptions = new IndexOptions();
        
        if (options != null) {
            Document optionsDoc = toDocument(options);
            if (optionsDoc.containsKey("unique")) {
                indexOptions.unique(optionsDoc.getBoolean("unique"));
            }
            if (optionsDoc.containsKey("name")) {
                indexOptions.name(optionsDoc.getString("name"));
            }
            if (optionsDoc.containsKey("sparse")) {
                indexOptions.sparse(optionsDoc.getBoolean("sparse"));
            }
        }
        
        String indexName = currentDatabase.getCollection(collectionName)
            .createIndex(keysDoc, indexOptions);
            
        return "Created index: " + indexName;
    }
    
    public List<Document> getIndexes(String collectionName) {
        if (currentDatabase == null) {
            return Collections.emptyList();
        }
        
        List<Document> indexes = new ArrayList<>();
        currentDatabase.getCollection(collectionName)
            .listIndexes()
            .forEach(indexes::add);
            
        return indexes;
    }
    
    public Object executeShowCommand(String what) {
        switch (what.toLowerCase()) {
            case "dbs":
            case "databases":
                List<String> databases = new ArrayList<>();
                mongoClient.listDatabaseNames().forEach(databases::add);
                return databases;
                
            case "collections":
            case "tables":
                return getCollectionNames();
                
            case "users":
                if (currentDatabase != null) {
                    Document cmd = new Document("usersInfo", 1);
                    Document result = currentDatabase.runCommand(cmd);
                    return result.get("users", Collections.emptyList());
                }
                return "No database selected";
                
            case "profile":
                if (currentDatabase != null) {
                    List<Document> profile = new ArrayList<>();
                    currentDatabase.getCollection("system.profile")
                        .find()
                        .forEach(profile::add);
                    return profile;
                }
                return "No database selected";
                
            default:
                return "Unknown show command: " + what;
        }
    }
    
    private Document toDocument(Object obj) {
        if (obj == null) {
            return new Document();
        }
        if (obj instanceof Document) {
            return (Document) obj;
        }
        if (obj instanceof Map) {
            Document doc = new Document();
            ((Map<?, ?>) obj).forEach((k, v) -> doc.put(String.valueOf(k), convertValue(v)));
            return doc;
        }
        return Document.parse(obj.toString());
    }
    
    private Object convertValue(Object value) {
        if (value == null) {
            return null;
        }
        
        if (verbose) {
            logger.debug("Converting value: {} (type: {})", value, value.getClass().getName());
        }
        
        // Handle JavaScript regex objects (direct NativeRegExp)
        if (value instanceof NativeRegExp) {
            NativeRegExp nativeRegex = (NativeRegExp) value;
            String pattern = nativeRegex.toString();
            
            if (verbose) {
                logger.debug("Found NativeRegExp: {}", pattern);
            }
            
            // Extract pattern and flags from /pattern/flags format
            if (pattern.startsWith("/") && pattern.lastIndexOf("/") > 0) {
                int lastSlash = pattern.lastIndexOf("/");
                String regexPattern = pattern.substring(1, lastSlash);
                String flags = pattern.substring(lastSlash + 1);
                
                // Convert to MongoDB regex format
                Document regexDoc = new Document("$regex", regexPattern);
                if (!flags.isEmpty()) {
                    regexDoc.put("$options", flags);
                }
                
                if (verbose) {
                    logger.debug("Converted to MongoDB regex: {}", regexDoc);
                }
                return regexDoc;
            }
            
            // Fallback - just use the pattern as string
            return new Document("$regex", pattern);
        }
        
        // Handle NativeObject that might contain regex literals
        if (value instanceof NativeObject) {
            NativeObject nativeObj = (NativeObject) value;
            
            if (verbose) {
                logger.debug("Found NativeObject: {}, className: {}", nativeObj, nativeObj.getClassName());
                // Debug: let's see what properties this object has
                Object[] propIds = nativeObj.getIds();
                logger.debug("NativeObject properties: {}", java.util.Arrays.toString(propIds));
            }
            
            // Check if this has a $regex property (JavaScript { "$regex": /:pattern$/ } object)  
            Object regexValue = nativeObj.get("$regex", nativeObj);
            if (regexValue != null && !"undefined".equals(regexValue.toString())) {
                if (verbose) {
                    logger.debug("Found $regex property with value: {} ({})", regexValue, regexValue.getClass().getName());
                }
                
                // The value of $regex should be a JavaScript RegExp object
                String regexPattern = null;
                String regexFlags = "";
                
                // Check if the value is another NativeObject (the actual regex)
                if (regexValue instanceof NativeObject) {
                    NativeObject regexObj = (NativeObject) regexValue;
                    String regexObjString = regexObj.toString();
                    
                    if (verbose) {
                        logger.debug("$regex value is NativeObject: {}, className: {}", regexObjString, regexObj.getClassName());
                    }
                    
                    // Try to get source and flags properties
                    try {
                        Object sourceObj = regexObj.get("source", regexObj);
                        Object flagsObj = regexObj.get("flags", regexObj);
                        if (sourceObj != null) regexPattern = sourceObj.toString();
                        if (flagsObj != null) regexFlags = flagsObj.toString();
                        
                        if (verbose) {
                            logger.debug("Extracted from RegExp properties - source: {}, flags: {}", regexPattern, regexFlags);
                        }
                    } catch (Exception e) {
                        // Fallback: parse from toString if it looks like a regex
                        if (regexObjString.matches("/.*?/[gimsuyx]*")) {
                            int lastSlash = regexObjString.lastIndexOf('/');
                            if (lastSlash > 0) {
                                regexPattern = regexObjString.substring(1, lastSlash);
                                regexFlags = regexObjString.substring(lastSlash + 1);
                                
                                if (verbose) {
                                    logger.debug("Parsed from toString - pattern: {}, flags: {}", regexPattern, regexFlags);
                                }
                            }
                        }
                    }
                } else {
                    // Direct string value
                    String regexStr = regexValue.toString();
                    if (regexStr.matches("/.*?/[gimsuyx]*")) {
                        int lastSlash = regexStr.lastIndexOf('/');
                        if (lastSlash > 0) {
                            regexPattern = regexStr.substring(1, lastSlash);
                            regexFlags = regexStr.substring(lastSlash + 1);
                        }
                    } else {
                        regexPattern = regexStr;
                    }
                }
                
                if (regexPattern != null) {
                    Document regexDoc = new Document("$regex", regexPattern);
                    if (regexFlags != null && !regexFlags.isEmpty()) {
                        regexDoc.put("$options", regexFlags);
                    }
                    
                    if (verbose) {
                        logger.debug("Created MongoDB regex document: {}", regexDoc);
                    }
                    
                    return regexDoc;
                }
            }
            
            // Check if this is a direct RegExp object by examining its class name or toString
            String className = nativeObj.getClassName();
            String objString = nativeObj.toString();
            
            if ("RegExp".equals(className) || objString.matches("/.*?/[gimsuyx]*")) {
                // This is a JavaScript RegExp object
                String source = null;
                String flags = "";
                
                // Try to extract from properties first
                try {
                    Object sourceObj = nativeObj.get("source", nativeObj);
                    Object flagsObj = nativeObj.get("flags", nativeObj);
                    if (sourceObj != null) source = sourceObj.toString();
                    if (flagsObj != null) flags = flagsObj.toString();
                } catch (Exception e) {
                    // Fallback: parse from toString if it looks like a regex
                    if (objString.matches("/.*?/[gimsuyx]*")) {
                        int lastSlash = objString.lastIndexOf('/');
                        if (lastSlash > 0) {
                            source = objString.substring(1, lastSlash);
                            flags = objString.substring(lastSlash + 1);
                        }
                    }
                }
                
                if (verbose) {
                    logger.debug("Found direct RegExp - source: {}, flags: {}", source, flags);
                }
                
                if (source != null) {
                    Document regexDoc = new Document("$regex", source);
                    if (flags != null && !flags.isEmpty()) {
                        regexDoc.put("$options", flags);
                    }
                    
                    if (verbose) {
                        logger.debug("Converted direct RegExp to MongoDB regex: {}", regexDoc);
                    }
                    return regexDoc;
                }
            }
        }
        
        // Handle other Rhino objects that need conversion
        if (value.getClass().getName().startsWith("org.mozilla.javascript.")) {
            if (verbose) {
                logger.debug("Found other Rhino object: {} ({})", value.toString(), value.getClass().getName());
            }
            // For other JavaScript objects, try to convert to string
            return value.toString();
        }
        
        // Handle nested objects
        if (value instanceof Map) {
            Document doc = new Document();
            ((Map<?, ?>) value).forEach((k, v) -> doc.put(String.valueOf(k), convertValue(v)));
            return doc;
        }
        
        if (value instanceof List) {
            return ((List<?>) value).stream()
                .map(this::convertValue)
                .collect(Collectors.toList());
        }
        
        if (value instanceof Object[]) {
            return Arrays.stream((Object[]) value)
                .map(this::convertValue)
                .collect(Collectors.toList());
        }
        
        // Return as-is for primitive types and other objects
        return value;
    }
    
    private List<Document> toDocumentList(Object obj) {
        if (obj == null) {
            return Collections.emptyList();
        }
        if (obj instanceof List) {
            return ((List<?>) obj).stream()
                .map(this::toDocument)
                .collect(Collectors.toList());
        }
        if (obj instanceof Object[]) {
            return Arrays.stream((Object[]) obj)
                .map(this::toDocument)
                .collect(Collectors.toList());
        }
        return Collections.singletonList(toDocument(obj));
    }
    
    private Bson toBsonUpdate(Object update) {
        Document doc = toDocument(update);
        
        boolean hasUpdateOperator = doc.keySet().stream()
            .anyMatch(key -> key.startsWith("$"));
            
        if (!hasUpdateOperator) {
            Document setDoc = new Document("$set", doc);
            return setDoc;
        }
        
        return doc;
    }
    
    private String formatUpdateResult(UpdateResult result) {
        StringBuilder sb = new StringBuilder();
        sb.append("Matched: ").append(result.getMatchedCount());
        sb.append(", Modified: ").append(result.getModifiedCount());
        if (result.getUpsertedId() != null) {
            sb.append(", Upserted: ").append(result.getUpsertedId());
        }
        return sb.toString();
    }
}