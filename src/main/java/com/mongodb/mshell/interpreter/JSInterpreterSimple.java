package com.mongodb.mshell.interpreter;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.mshell.translator.MongoCommandTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import java.text.DecimalFormat;

import org.mozilla.javascript.*;

public class JSInterpreterSimple implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(JSInterpreterSimple.class);
    
    private Context jsContext;
    private ScriptableObject jsScope;
    private final MongoClient mongoClient;
    private MongoCommandTranslator translator;
    private ObjectMapper objectMapper;
    private MongoDatabase currentDatabase;
    private CursorProxySimple lastCursor;
    private final boolean ownsMongoClient;
    private final boolean verbose;
    
    public JSInterpreterSimple(String connectionString) {
        this(connectionString, false);
    }
    
    public JSInterpreterSimple(String connectionString, boolean verbose) {
        try {
            this.mongoClient = MongoClients.create(connectionString);
            this.ownsMongoClient = true;
            this.verbose = verbose;
            init();
        } catch (Exception e) {
            logger.error("JavaScript interpreter initialization failed", e);
            throw new RuntimeException("Failed to initialize JavaScript interpreter: " + e.getMessage(), e);
        }
    }
    
    public JSInterpreterSimple(MongoClient mongoClient) {
        this(mongoClient, false);
    }
    
    public JSInterpreterSimple(MongoClient mongoClient, boolean verbose) {
        try {
            this.mongoClient = mongoClient;
            this.ownsMongoClient = false;
            this.verbose = verbose;
            init();
        } catch (Exception e) {
            logger.error("JavaScript interpreter initialization failed", e);
            throw new RuntimeException("Failed to initialize JavaScript interpreter: " + e.getMessage(), e);
        }
    }
    
    private void init() {
        this.translator = new MongoCommandTranslator(mongoClient, verbose);
        this.objectMapper = new ObjectMapper();
        this.objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
        
        this.jsContext = Context.enter();
        this.jsScope = jsContext.initStandardObjects();
        
        initializeMongoShellEnvironment();
    }
    
    private void initializeMongoShellEnvironment() {
        try {
            // Add MongoDB shell functions to scope
            DatabaseProxySimple dbProxy = new DatabaseProxySimple(translator);
            dbProxy.setParentScope(jsScope);
            dbProxy.setPrototype(ScriptableObject.getObjectPrototype(jsScope));
            ScriptableObject.putProperty(jsScope, "db", dbProxy);
            
            ScriptableObject.putProperty(jsScope, "use", new UseFunction());
            ScriptableObject.putProperty(jsScope, "show", new ShowFunction());
            ScriptableObject.putProperty(jsScope, "help", new HelpFunction());
            ScriptableObject.putProperty(jsScope, "it", new ItFunction());

            // Add replica set proxy
            ReplicaSetProxySimple rsProxy = new ReplicaSetProxySimple(mongoClient);
            rsProxy.setParentScope(jsScope);
            rsProxy.setPrototype(ScriptableObject.getObjectPrototype(jsScope));
            ScriptableObject.putProperty(jsScope, "rs", rsProxy);
            
            // Initialize built-in MongoDB shell functions
            jsContext.evaluateString(jsScope, initializeMongoShellFunctions(), "init", 1, null);
            
            String defaultDb = "test";
            currentDatabase = mongoClient.getDatabase(defaultDb);
            translator.setCurrentDatabase(currentDatabase);
        } catch (Exception e) {
            logger.error("MongoDB shell environment initialization failed", e);
            throw new RuntimeException("Failed to initialize MongoDB shell environment: " + e.getMessage(), e);
        }
    }
    
    private String initializeMongoShellFunctions() {
        return "function printjson(obj) {" +
            "    if (typeof obj === 'object') {" +
            "        print(JSON.stringify(obj, null, 2));" +
            "    } else {" +
            "        print(obj);" +
            "    }" +
            "}" +
            "" +
            "function print(msg) {" +
            "    java.lang.System.out.println(msg);" +
            "}" +
            "" +
            "var ObjectId = function(id) {" +
            "    if (!id) {" +
            "        id = java.util.UUID.randomUUID().toString();" +
            "    }" +
            "    return {'$oid': id};" +
            "};" +
            "" +
            "var ISODate = function(dateStr) {" +
            "    if (!dateStr) {" +
            "        dateStr = new Date().toISOString();" +
            "    }" +
            "    return {'$date': dateStr};" +
            "};" +
            "" +
            "var NumberLong = function(value) {" +
            "    return {'$numberLong': String(value)};" +
            "};" +
            "" +
            "var NumberInt = function(value) {" +
            "    return {'$numberInt': String(value)};" +
            "};" +
            "" +
            "var DBRef = function(collection, id, db) {" +
            "    var ref = {'$ref': collection, '$id': id};" +
            "    if (db) ref['$db'] = db;" +
            "    return ref;" +
            "};";
    }
    
    public Object execute(String command) {
        // Create a new Context for each execution to handle threading correctly
        Context currentContext = Context.enter();
        try {
            command = preprocessCommand(command);
            
            if (command.startsWith("use ")) {
                String dbName = command.substring(4).trim();
                currentDatabase = mongoClient.getDatabase(dbName);
                translator.setCurrentDatabase(currentDatabase);
                return "switched to db " + dbName;
            }
            
            if (command.startsWith("show ")) {
                String what = command.substring(5).trim();
                return translator.executeShowCommand(what);
            }
            
            if (command.trim().equals("it")) {
                if (lastCursor != null && lastCursor.hasMore()) {
                    return lastCursor.nextBatch();
                } else {
                    return "no cursor";
                }
            }
            
            // For JavaScript evaluation, we need to create a fresh scope with our objects
            // since we're using a new Context but want to keep the same environment
            ScriptableObject currentScope;
            if (currentContext == jsContext) {
                // Same context, can reuse scope
                currentScope = jsScope;
            } else {
                // Different context, need to recreate the scope with our objects
                currentScope = currentContext.initStandardObjects();
                
                // Re-add our MongoDB shell environment to the new scope
                DatabaseProxySimple dbProxy = new DatabaseProxySimple(translator);
                dbProxy.setParentScope(currentScope);
                dbProxy.setPrototype(ScriptableObject.getObjectPrototype(currentScope));
                ScriptableObject.putProperty(currentScope, "db", dbProxy);
                
                ScriptableObject.putProperty(currentScope, "use", new UseFunction());
                ScriptableObject.putProperty(currentScope, "show", new ShowFunction());
                ScriptableObject.putProperty(currentScope, "help", new HelpFunction());
                ScriptableObject.putProperty(currentScope, "it", new ItFunction());
                
                // Initialize built-in MongoDB shell functions
                currentContext.evaluateString(currentScope, initializeMongoShellFunctions(), "init", 1, null);
            }
            
            Object result = currentContext.evaluateString(currentScope, command, "command", 1, null);
            
            // If result is a cursor, store it as the last cursor
            if (result instanceof CursorProxySimple) {
                lastCursor = (CursorProxySimple) result;
            }
            
            return result;
            
        } catch (Exception e) {
            logger.error("JavaScript execution error", e);
            throw new RuntimeException("JavaScript error: " + e.getMessage(), e);
        } finally {
            Context.exit();
        }
    }
    
    private String preprocessCommand(String command) {
        command = command.trim();
        
        command = command.replaceAll("\\bshow\\s+dbs\\b", "show('dbs')");
        command = command.replaceAll("\\bshow\\s+databases\\b", "show('databases')");
        command = command.replaceAll("\\bshow\\s+collections\\b", "show('collections')");
        command = command.replaceAll("\\bshow\\s+tables\\b", "show('tables')");
        command = command.replaceAll("\\bshow\\s+users\\b", "show('users')");
        
        // Handle dotted collection names by converting db.collection.name.method to db.getCollection('collection.name').method
        // This regex finds patterns like db.rollup.site_daily_stats.count and converts them properly
        command = command.replaceAll("\\bdb\\.([a-zA-Z_][a-zA-Z0-9_]*\\.[a-zA-Z_][a-zA-Z0-9_\\.]*)\\.([a-zA-Z_][a-zA-Z0-9_]*)", "db.getCollection('$1').$2");
        
        return command;
    }
    
    public String formatResult(Object result) {
        try {
            if (result instanceof String) {
                return (String) result;
            }
            if (result instanceof CursorProxySimple) {
                return result.toString();
            }
            // Format numbers with commas
            if (result instanceof Number) {
                DecimalFormat formatter = new DecimalFormat("#,###");
                return formatter.format(result);
            }
            return objectMapper.writeValueAsString(result);
        } catch (Exception e) {
            return result.toString();
        }
    }
    
    @Override
    public void close() {
        try {
            if (jsContext != null) {
                Context.exit();
            }
            if (ownsMongoClient && mongoClient != null) {
                mongoClient.close();
            }
        } catch (Exception e) {
            logger.error("Error closing resources", e);
        }
    }
    
    // Simple function classes for JavaScript bindings
    private class UseFunction extends BaseFunction {
        @Override
        public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
            if (args.length > 0) {
                String dbName = args[0].toString();
                currentDatabase = mongoClient.getDatabase(dbName);
                translator.setCurrentDatabase(currentDatabase);
                return "switched to db " + dbName;
            }
            return "specify database name";
        }
    }
    
    private class ShowFunction extends BaseFunction {
        @Override
        public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
            if (args.length > 0) {
                return translator.executeShowCommand(args[0].toString());
            }
            return "show requires an argument";
        }
    }
    
    private class HelpFunction extends BaseFunction {
        @Override
        public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
            return "MongoDB Shell (mshell) Help\n" +
                "\n" +
                "Database Commands:\n" +
                "  use <db>                 Switch to database\n" +
                "  show dbs                 Show all databases\n" +
                "  show collections         Show collections in current database\n" +
                "  db.<collection>.find()   Find documents\n" +
                "  db.<collection>.insert() Insert document\n" +
                "  db.<collection>.update() Update documents\n" +
                "  db.<collection>.remove() Remove documents\n" +
                "  \n" +
                "Shell Commands:\n" +
                "  help()                   Show this help\n" +
                "  it                       Iterate cursor for more results\n" +
                "  exit/quit                Exit the shell\n" +
                "  \n" +
                "Additional Features:\n" +
                "  - JavaScript expressions supported\n" +
                "  - Multi-line input supported (incomplete statements continue on next line)\n" +
                "  - Execute on all shards with -s flag\n";
        }
    }
    
    private class ItFunction extends BaseFunction {
        @Override
        public Object call(Context cx, Scriptable scope, Scriptable thisObj, Object[] args) {
            if (lastCursor != null && lastCursor.hasMore()) {
                return lastCursor.nextBatch();
            } else {
                return "no cursor";
            }
        }
    }
}