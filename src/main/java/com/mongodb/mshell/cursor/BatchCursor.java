package com.mongodb.mshell.cursor;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class BatchCursor {
    private static final Logger logger = LoggerFactory.getLogger(BatchCursor.class);
    private static final int DEFAULT_BATCH_SIZE = 20;
    
    private final MongoCursor<Document> cursor;
    private final int batchSize;
    private int docsReturned = 0;
    private boolean hasMore = true;
    
    public BatchCursor(FindIterable<Document> iterable) {
        this(iterable, DEFAULT_BATCH_SIZE);
    }
    
    public BatchCursor(FindIterable<Document> iterable, int batchSize) {
        this.batchSize = batchSize;
        this.cursor = iterable.iterator();
        this.hasMore = cursor.hasNext();
    }
    
    public List<Document> nextBatch() {
        List<Document> batch = new ArrayList<>();
        int count = 0;
        
        while (count < batchSize && cursor.hasNext()) {
            batch.add(cursor.next());
            count++;
            docsReturned++;
        }
        
        hasMore = cursor.hasNext();
        return batch;
    }
    
    public boolean hasMore() {
        return hasMore;
    }
    
    public int getDocsReturned() {
        return docsReturned;
    }
    
    public void close() {
        if (cursor != null) {
            cursor.close();
        }
    }
    
    @Override
    protected void finalize() throws Throwable {
        close();
        super.finalize();
    }
}