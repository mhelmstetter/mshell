package com.mongodb.mshell.interpreter;

import com.mongodb.mshell.translator.MongoCommandTranslator;
import com.mongodb.mshell.cursor.BatchCursor;
import org.mozilla.javascript.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.bson.Document;

import java.util.List;

public class CursorProxySimple extends ScriptableObject {
    private static final Logger logger = LoggerFactory.getLogger(CursorProxySimple.class);
    private final MongoCommandTranslator translator;
    private final String collectionName;
    private Object filter;
    private Object projection;
    private Object sort;
    private Integer limit;
    private Integer skip;
    private BatchCursor batchCursor;
    private boolean executed = false;
    
    public CursorProxySimple(MongoCommandTranslator translator, String collectionName, Object filter, Object projection) {
        this.translator = translator;
        this.collectionName = collectionName;
        this.filter = filter;
        this.projection = projection;
    }
    
    @Override
    public String getClassName() {
        return "Cursor";
    }
    
    private void ensureCursor() {
        if (!executed) {
            batchCursor = translator.getBatchCursor(collectionName, filter, projection, sort, limit, skip);
            executed = true;
        }
    }
    
    public CursorProxySimple sort(Object sortSpec) {
        this.sort = sortSpec;
        return this;
    }
    
    public CursorProxySimple limit(int limitValue) {
        this.limit = limitValue;
        return this;
    }
    
    public CursorProxySimple skip(int skipValue) {
        this.skip = skipValue;
        return this;
    }
    
    public long count() {
        return translator.countDocuments(collectionName, filter);
    }
    
    public List<Document> toArray() {
        return translator.find(collectionName, filter, projection, sort, limit, skip);
    }
    
    public List<Document> nextBatch() {
        ensureCursor();
        return batchCursor.nextBatch();
    }
    
    public boolean hasMore() {
        ensureCursor();
        return batchCursor.hasMore();
    }
    
    @Override
    public String toString() {
        ensureCursor();
        List<Document> batch = batchCursor.nextBatch();
        
        if (batch.isEmpty()) {
            return "no results";
        }
        
        StringBuilder sb = new StringBuilder();
        try {
            com.fasterxml.jackson.databind.ObjectMapper mapper = new com.fasterxml.jackson.databind.ObjectMapper();
            mapper.enable(com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT);
            
            for (Document doc : batch) {
                sb.append(mapper.writeValueAsString(doc)).append("\n");
            }
            
            if (batchCursor.hasMore()) {
                sb.append("Type \"it\" for more\n");
            }
        } catch (Exception e) {
            return batch.toString();
        }
        
        return sb.toString();
    }
    
    @Override
    protected void finalize() throws Throwable {
        if (batchCursor != null) {
            batchCursor.close();
        }
        super.finalize();
    }
}