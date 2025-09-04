package com.mongodb.mshell;

import com.mongodb.ConnectionString;
import com.mongodb.mshell.interpreter.JSInterpreterSimple;
import com.mongodb.mshell.executor.ShardQueryExecutor;
import com.mongodb.util.MaskUtil;
import org.apache.commons.cli.*;
import org.jline.reader.*;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class MShell {
    private static final Logger logger = LoggerFactory.getLogger(MShell.class);
    private static final String PROMPT = "mshell> ";
    
    private final JSInterpreterSimple interpreter;
    private final ShardQueryExecutor shardExecutor;
    private final boolean executeOnShards;
    private final boolean verbose;
    private final String connectionString;
    
    public MShell(String connectionString, boolean executeOnShards, boolean verbose) {
        this.connectionString = connectionString;
        this.executeOnShards = executeOnShards;
        this.verbose = verbose;
        
        try {
            this.interpreter = new JSInterpreterSimple(connectionString, verbose);
        } catch (Exception e) {
            logger.error("Failed to initialize JavaScript interpreter", e);
            throw new RuntimeException("Failed to initialize JavaScript interpreter: " + e.getMessage(), e);
        }
        
        this.shardExecutor = executeOnShards ? new ShardQueryExecutor(connectionString, verbose) : null;
    }
    
    public void runInteractive() throws IOException {
        Terminal terminal = TerminalBuilder.builder()
                .system(true)
                .build();
                
        // Set up persistent command history like mongo shell
        Path historyFile = Paths.get(System.getProperty("user.home"), ".mshell_history");
        
        LineReader reader = LineReaderBuilder.builder()
                .terminal(terminal)
                .variable(LineReader.HISTORY_FILE, historyFile)
                .build();
                
        System.out.println("MongoDB Shell (mshell) - Java Edition");
        System.out.println("Type 'help()' for help, 'exit' or 'quit' to exit");
        System.out.println("Connected to: " + MaskUtil.maskConnectionString(new ConnectionString(connectionString)));
        if (executeOnShards) {
            System.out.println("Shard execution mode: ENABLED");
        }
        System.out.println();
        
        String line;
        StringBuilder multilineBuffer = new StringBuilder();
        boolean inMultiline = false;
        
        while (true) {
            try {
                String currentPrompt = inMultiline ? "... " : PROMPT;
                line = reader.readLine(currentPrompt);
                
                if (line == null || line.trim().equals("exit") || line.trim().equals("quit")) {
                    break;
                }
                
                if (line.trim().isEmpty()) {
                    continue;
                }
                
                multilineBuffer.append(line).append("\n");
                
                if (isCompleteStatement(multilineBuffer.toString())) {
                    String command = multilineBuffer.toString().trim();
                    multilineBuffer.setLength(0);
                    inMultiline = false;
                    
                    executeCommand(command);
                } else {
                    inMultiline = true;
                }
                
            } catch (UserInterruptException e) {
                multilineBuffer.setLength(0);
                inMultiline = false;
                System.out.println("^C");
            } catch (EndOfFileException e) {
                break;
            }
        }
        
        System.out.println("\nBye!");
        cleanup();
    }
    
    public void executeFile(String filePath) throws IOException {
        String content = new String(Files.readAllBytes(Paths.get(filePath)));
        executeCommand(content);
    }
    
    public void executeCommand(String command) {
        try {
            if (executeOnShards) {
                System.out.println("Executing on all shards:");
                shardExecutor.executeOnAllShards(command);
            } else {
                Object result = interpreter.execute(command);
                if (result != null) {
                    System.out.println(interpreter.formatResult(result));
                }
            }
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            logger.error("Command execution failed", e);
        }
    }
    
    private boolean isCompleteStatement(String input) {
        input = input.trim();
        if (input.isEmpty()) return true;
        
        int openBraces = 0;
        int openParens = 0;
        int openBrackets = 0;
        boolean inString = false;
        boolean inSingleQuote = false;
        char prevChar = '\0';
        
        for (char c : input.toCharArray()) {
            if (prevChar == '\\') {
                prevChar = c;
                continue;
            }
            
            if (!inSingleQuote && c == '"') {
                inString = !inString;
            } else if (!inString && c == '\'') {
                inSingleQuote = !inSingleQuote;
            } else if (!inString && !inSingleQuote) {
                switch (c) {
                    case '{': openBraces++; break;
                    case '}': openBraces--; break;
                    case '(': openParens++; break;
                    case ')': openParens--; break;
                    case '[': openBrackets++; break;
                    case ']': openBrackets--; break;
                }
            }
            prevChar = c;
        }
        
        return openBraces == 0 && openParens == 0 && openBrackets == 0 && !inString && !inSingleQuote;
    }
    
    private void cleanup() {
        if (interpreter != null) {
            interpreter.close();
        }
        if (shardExecutor != null) {
            shardExecutor.close();
        }
    }
    
    public static void main(String[] args) {
        Options options = new Options();
        
        options.addOption("f", "file", true, "Execute JavaScript file");
        options.addOption("e", "eval", true, "Evaluate JavaScript expression");
        options.addOption("s", "shards", false, "Execute queries on all shards individually");
        options.addOption("v", "verbose", false, "Show verbose output including queries sent to MongoDB");
        options.addOption(null, "help", false, "Show help");
        
        CommandLineParser parser = new DefaultParser();
        
        try {
            CommandLine cmd = parser.parse(options, args);
            
            if (cmd.hasOption("help")) {
                printUsage();
                System.exit(0);
            }
            
            // Get connection string from positional argument or use default
            String connectionString = "mongodb://localhost:27017";
            String[] remainingArgs = cmd.getArgs();
            logger.info("Command line args: {}", java.util.Arrays.toString(args));
            logger.info("Remaining args after parsing: {}", java.util.Arrays.toString(remainingArgs));
            
            if (remainingArgs.length > 0) {
                String uri = remainingArgs[0];
                connectionString = uri.startsWith("mongodb://") || uri.startsWith("mongodb+srv://") 
                    ? uri : "mongodb://" + uri;
                logger.info("Using connection string from argument: '{}'", connectionString);
            } else {
                logger.info("Using default connection string: '{}'", connectionString);
            }
            
            boolean executeOnShards = cmd.hasOption("shards");
            boolean verbose = cmd.hasOption("verbose");
            logger.info("Execute on shards: {}", executeOnShards);
            logger.info("Verbose mode: {}", verbose);
            
            // Only test connection if we're doing interactive mode or shard mode
            // Simple evaluations may not need MongoDB
            if (!cmd.hasOption("eval") || executeOnShards) {
                try {
                    testConnection(connectionString);
                } catch (Exception e) {
                    System.err.println("Failed to connect to MongoDB at " + connectionString);
                    System.err.println("Error: " + e.getMessage());
                    System.err.println("\nMake sure MongoDB is running and accessible.");
                    System.err.println("Usage: mshell [mongodb://]host[:port][/database]");
                    System.exit(1);
                }
            }
            
            MShell shell = new MShell(connectionString, executeOnShards, verbose);
            
            if (cmd.hasOption("file")) {
                shell.executeFile(cmd.getOptionValue("file"));
            } else if (cmd.hasOption("eval")) {
                shell.executeCommand(cmd.getOptionValue("eval"));
            } else {
                shell.runInteractive();
            }
            
        } catch (ParseException e) {
            System.err.println("Error parsing command line: " + e.getMessage());
            printUsage();
            System.exit(1);
        } catch (IllegalStateException e) {
            if (e.getMessage().contains("No language and polyglot implementation")) {
                System.err.println("JavaScript engine initialization failed.");
                System.err.println("This may be due to missing GraalVM JavaScript dependencies.");
                System.err.println("Please ensure GraalVM JavaScript is properly installed.");
            } else {
                System.err.println("Initialization error: " + e.getMessage());
            }
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            if (logger.isDebugEnabled()) {
                logger.error("Fatal error", e);
            }
            System.exit(1);
        }
    }
    
    private static void printUsage() {
        System.out.println("MongoDB Shell (mshell) - Java Edition");
        System.out.println();
        System.out.println("Usage: mshell [options] [mongodb_uri]");
        System.out.println();
        System.out.println("Examples:");
        System.out.println("  mshell                           # Connect to localhost:27017");
        System.out.println("  mshell mongodb://host:27017     # Connect to specific host");
        System.out.println("  mshell host:27017                # Connect to specific host");
        System.out.println("  mshell -e 'db.test.find()'       # Execute command");
        System.out.println("  mshell -f script.js              # Execute script file");
        System.out.println("  mshell -s mongodb://mongos:27017 # Execute on all shards");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  -e, --eval <expression>  Evaluate JavaScript expression");
        System.out.println("  -f, --file <file>        Execute JavaScript file");
        System.out.println("  -s, --shards             Execute queries on all shards individually");
        System.out.println("  -v, --verbose            Show verbose output including queries sent to MongoDB");
        System.out.println("  --help                   Show this help message");
    }
    
    private static void testConnection(String connectionString) {
        com.mongodb.MongoClientSettings.Builder settingsBuilder = com.mongodb.MongoClientSettings.builder()
            .applyConnectionString(new com.mongodb.ConnectionString(connectionString))
            .applyToClusterSettings(builder -> 
                builder.serverSelectionTimeout(5000, java.util.concurrent.TimeUnit.MILLISECONDS))
            .applyToSocketSettings(builder -> 
                builder.connectTimeout(3000, java.util.concurrent.TimeUnit.MILLISECONDS));
                
        try (com.mongodb.client.MongoClient client = com.mongodb.client.MongoClients.create(settingsBuilder.build())) {
            // Try to ping the server
            client.getDatabase("admin").runCommand(new org.bson.Document("ping", 1));
        }
    }
}