package src;
import java.io.File;
import java.nio.Buffer;
import java.util.Scanner;

import database_parsers.DDLParser;
import database_parsers.DMLParser;

public class Main 
{

    public static void main(String[] args)
    {
        StorageManager storage_manager;
        BufferManager buffer_manager;
        Catalog catalog;
        String db_path;
        int page_size;
        int buffer_size;
        // Design: 
        // Input: java Main <db loc> <page size> <buffer size>
        // Check for:
        //      input length, assume db location exists and is accessible
        if (args.length != 3){
            //throw new Exception
            System.err.println("Error: invalid input format: unexpected number of args.\n" + 
                                "Try again with the input command in the format:\n" + 
                                "$java Main <db loc> <page size> <buffer size>");
            System.exit(-1);
        }
        System.out.println("Welcome to JottQL");

        db_path = args[0]; 
        page_size = Integer.parseInt(args[1]) * 8; // convert to bits
        buffer_size = Integer.parseInt(args[2]);

        System.out.println("Looking at " + db_path + " for existing database...");
        // Check if DB exists already. If not, create new DB at given path. 
        // Prints for db creation/finding
        
        File db_directory = new File(db_path);
        if (!db_directory.isDirectory()){
            System.out.println("No existing db found");
            System.out.println("Creating new db at " + db_path);
            db_directory.mkdir();
            catalog = new Catalog(db_path, page_size, buffer_size);
            System.out.println("New db created successfully");
            // If DB does not exist yet, use given page size and given buffer size
            System.out.println("Page size: " + page_size/8);
            System.out.println("Buffer size: " + buffer_size + "\n");
        }
        // If DB does exist already, ignore given page size, but use given buffer size 
        //      and restart the existing DB
        else {
            catalog = new Catalog(db_path, buffer_size);

            if(catalog.get_page_size() == 0) {
                System.out.println("There was no Catalog stored at " + catalog.get_catalog_path());
                System.out.println("Using Provided Size");
                catalog.set_page_size(page_size);
            }

            System.out.println("Database found...");
            System.out.println("Restarting the database...");
            System.out.println("\tIgnoring provided page size, using stored page size");
            System.out.println("Page size: " + catalog.get_page_size()/8);
            System.out.println("Buffer size: " + catalog.get_buffer_size() + "\n");   // should be equal to provided buff size
            System.out.println("Database restarted successfully\n");
        }
        System.out.println("Please enter commands, enter <quit> to shutdown the db\n");

        Scanner scan = new Scanner(System.in);
        String fullCommand = "";
        buffer_manager = new BufferManager(catalog);
        storage_manager = new StorageManager(catalog, page_size, catalog.get_db_loc(), buffer_manager);
        
        DDLParser ddl_parser = new DDLParser(catalog, storage_manager);
        DMLParser dml_parser = new DMLParser(catalog, storage_manager);
        Tokenizer tokenizer = new Tokenizer(catalog, ddl_parser, dml_parser);
        //storage_manager.test();
        //tokenizer.test();
        //catalog.catalog_to_file(); 
        //buffer_manager.shutdown();
        // Loop until user inputs "quit" to terminate the program
        while(true) {
            System.out.print("JottQL> ");
            String input = scan.nextLine();
            if (input.equalsIgnoreCase("quit")){
                scan.close();
                // TODO: implement functionality to shut down database 
                System.out.println("\nSafely shutting down the database...");
                System.out.println("Purging page buffer...");
                buffer_manager.shutdown();
                System.out.println("Saving catalog...\n");
                catalog.catalog_to_file();
                System.out.println("Exiting the database...\n");
                break;
            }
            else {

                // handles multi-line commands  

                if (input.equals(" ") || input.equals("")){
                    continue;
                }
                if(input.substring(input.length() - 1).compareTo(";") == 0){
                    fullCommand = fullCommand + " " + input;
                    tokenizer.tokenize(fullCommand);
                    fullCommand = "";
                    buffer_manager.print_buffer();
                    continue;
                }
                else{
                    fullCommand = fullCommand + " " + input;
                }
       
            }
            
        }
        
    }

}

