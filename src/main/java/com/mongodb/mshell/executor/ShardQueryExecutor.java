package com.mongodb.mshell.executor;

import com.mongodb.shardsync.ShardClient;
import com.mongodb.model.Shard;
import com.mongodb.mshell.interpreter.JSInterpreterSimple;
import com.mongodb.client.MongoClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;

public class ShardQueryExecutor implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(ShardQueryExecutor.class);
    
    private final ShardClient shardClient;
    private final Map<String, JSInterpreterSimple> shardInterpreters;
    private final ExecutorService executorService;
    
    public ShardQueryExecutor(String connectionString) {
        try {
            logger.info("Initializing ShardQueryExecutor with connection: '{}'", connectionString);
            String sourceClusterId = "source";
            
            logger.debug("Creating ShardClient with name='{}' and clusterUri='{}'", sourceClusterId, connectionString);
            
            // FIX: Constructor parameters were in wrong order!
            // Should be: ShardClient(name, clusterUri, shardIdFilter)  
            // We were passing: ShardClient(clusterUri, name, shardIdFilter)
            this.shardClient = new ShardClient(sourceClusterId, connectionString);
            
            logger.debug("Initializing ShardClient...");
            this.shardClient.init();
            
            logger.debug("Populating shard MongoDB clients...");
            this.shardClient.populateShardMongoClients();
            
            this.shardInterpreters = new HashMap<>();
            this.executorService = Executors.newCachedThreadPool();
            
            logger.debug("Initializing shard connections...");
            initializeShardConnections();
            
            logger.info("ShardQueryExecutor initialized successfully with {} shards", 
                       shardInterpreters.size());
        } catch (Exception e) {
            logger.error("Failed to initialize ShardQueryExecutor", e);
            throw new RuntimeException("Failed to initialize ShardQueryExecutor: " + e.getMessage(), e);
        }
    }
    
    private void initializeShardConnections() {
        try {
            Map<String, MongoClient> shardClients = shardClient.getShardMongoClients();
            
            for (Map.Entry<String, MongoClient> entry : shardClients.entrySet()) {
                String shardName = entry.getKey();
                MongoClient mongoClient = entry.getValue();
                
                // Use the existing MongoClient that ShardClient created with proper connection params
                JSInterpreterSimple interpreter = new JSInterpreterSimple(mongoClient);
                shardInterpreters.put(shardName, interpreter);
                
                logger.info("Initialized interpreter for shard: {}", shardName);
            }
        } catch (Exception e) {
            logger.error("Failed to initialize shard connections", e);
            throw new RuntimeException("Failed to initialize shard connections", e);
        }
    }
    
    
    public void executeOnAllShards(String command) {
        if (shardInterpreters.isEmpty()) {
            System.out.println("No shards available");
            return;
        }
        
        List<Future<ShardResult>> futures = new ArrayList<>();
        
        for (Map.Entry<String, JSInterpreterSimple> entry : shardInterpreters.entrySet()) {
            String shardName = entry.getKey();
            JSInterpreterSimple interpreter = entry.getValue();
            
            Future<ShardResult> future = executorService.submit(() -> {
                try {
                    Object result = interpreter.execute(command);
                    return new ShardResult(shardName, result, null);
                } catch (Exception e) {
                    return new ShardResult(shardName, null, e);
                }
            });
            
            futures.add(future);
        }
        
        for (Future<ShardResult> future : futures) {
            try {
                ShardResult result = future.get(30, TimeUnit.SECONDS);
                printShardResult(result);
            } catch (TimeoutException e) {
                System.err.println("Timeout waiting for shard response");
            } catch (Exception e) {
                System.err.println("Error getting shard result: " + e.getMessage());
            }
        }
    }
    
    private void printShardResult(ShardResult result) {
        System.out.println("\n=== Shard: " + result.shardName + " ===");
        
        if (result.error != null) {
            System.err.println("ERROR: " + result.error.getMessage());
        } else if (result.result != null) {
            if (result.result instanceof Collection) {
                Collection<?> collection = (Collection<?>) result.result;
                System.out.println("Results: " + collection.size() + " document(s)");
                for (Object item : collection) {
                    System.out.println(formatResult(item));
                }
            } else {
                System.out.println(formatResult(result.result));
            }
        } else {
            System.out.println("(no output)");
        }
    }
    
    private String formatResult(Object result) {
        if (result == null) {
            return "null";
        }
        
        try {
            if (result instanceof Map || result instanceof List) {
                com.fasterxml.jackson.databind.ObjectMapper mapper = 
                    new com.fasterxml.jackson.databind.ObjectMapper();
                mapper.enable(com.fasterxml.jackson.databind.SerializationFeature.INDENT_OUTPUT);
                return mapper.writeValueAsString(result);
            }
        } catch (Exception e) {
            logger.error("Failed to format result", e);
        }
        
        return result.toString();
    }
    
    @Override
    public void close() {
        try {
            executorService.shutdown();
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
            
            for (JSInterpreterSimple interpreter : shardInterpreters.values()) {
                interpreter.close();
            }
            
            if (shardClient != null) {
                Map<String, MongoClient> clients = shardClient.getShardMongoClients();
                if (clients != null) {
                    for (MongoClient client : clients.values()) {
                        try {
                            client.close();
                        } catch (Exception e) {
                            logger.error("Error closing shard client", e);
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error closing resources", e);
        }
    }
    
    private static class ShardResult {
        final String shardName;
        final Object result;
        final Exception error;
        
        ShardResult(String shardName, Object result, Exception error) {
            this.shardName = shardName;
            this.result = result;
            this.error = error;
        }
    }
}