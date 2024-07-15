package src;

import java.lang.reflect.Array;
/*
 * File: Tokenizer.java
 * 
 * Desc: The Tokenizer will parse and tokenize string intended for Database Parsers
 * 
 * Authors: Alex Lee        al3774@rit.edu
 *          Merone Delnesa  mtd6620@rit.edu
 *          Isaac Mixon     igm3923@rit.edu
 *          Ethan Nunez     ern1274@rit.edu
 */
//import java.util.ArrayList;
//import java.util.List;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import database_parsers.DDLParser;
import database_parsers.DMLParser;

import java.util.regex.Matcher;
public class Tokenizer {
    public Tokenizer(){}
    public Tokenizer(Catalog table, DDLParser ddl_parser, DMLParser dml_parser) {
        this.table = table;
        this.ddl_parser = ddl_parser;
        this.dml_parser = dml_parser;
    }

    Catalog table = new Catalog("", 6, 1600);
    DDLParser ddl_parser;
    DMLParser dml_parser;
    //Catalog table = new Catalog("", 1600);
    /**
     * Tokenizes command line string, every command ends with ';'
     * 
     * @param String command string to tokenize to assign function to
     * @return None
     */
    // This Main is set up purely for testing purposes, did not want to put test
    // function in the actual main file to reduce chance of merge conflicts
    public static void main(String[] args) {
        Tokenizer toke = new Tokenizer();
        toke.test();
    }
    

    public void test() {
        String create_command = "CREATE TABLE BAZZLE( baz double PRIMARYKEY );";
        String create_command2 = "create table foo(baz integer primarykey, bar Double notnull, bazzle char(10) unique notnull);";

        String alter_command1 = "alter table foo drop bar;";
        String alter_command2 = "alter table foo add gar double;";
        String alter_command3 = "alter table foo add far double default 10.1;";
        String alter_command4 = "alter table foo add zar varchar(20) default \"hello world\";";
        String drop_command = "drop table foo;";
        String prior_insert = "insert into foo values (0 4 \"foo\");";
        String insert_command = "insert into foo values (1 \"foo1\" true 2.1);";
        String insert_command1 = "insert into foo values (1 \"foo\" true 2.1);";
        String insert_command2 = "insert into Students values (\"Ethan Nunez\" 21 0),(\"Alex Lee\" 21 1),(\"Merone Delnesa\" 20 0),(\"Isaac Mixon\" 20 1);";
        String insert_command3 = "insert into foo values (1 \"foo bar\" true 2.1),(3 \"baz\" true 4.14),(2 \"bar\" false 5.2),(5 \"true\" true null);";
        String display_command = "display info foo;";
        String display_command2 = "display schema;";
        String select_command = "select foo.id from foo, bar where foo.id > \"five\" and foo.id = bar.id;";
        String select_command2 = "select foo.id from foo, bar where foo.id = 5 and foo.id = bar.id orderby foo.id;";

        String quit_command = "quit";


        // new test commands
            // valid
        String create_3 = "create table pets(id integer primarykey, name varchar(20) unique notnull, species varchar(10) notnull, notes varchar(20));";
        String insert_4 = "insert into pets values (1 \"Socks\" \"Cat\" \"Crazy\"), (2 \"Rover\" \"Dog\" null), (3 \"Polly\" \"Parrot\" \"Mean\");";

        
        String insert_5 = "insert into pets values (4 \"Sam\" \"Snake\" null), (5 null null null), (6 \"Polly\" \"Parrot\" \"Mean\");"; // invalid
        String insert_6 = "insert into pets values (\"7\" \"Fifi\" \"Dog\" null);"; // invalid
        String insert_7 = "insert into pets values (8 \"Hamtaro\" \"Hamster\" \"Anime Hamster\"), (9 \"Pongo\" \"Dog\" \"Dalmation\");"; // valid

        String drop_2 = "drop table pets;";
        String insert_8 = "insert into pets values (10 \"Willa\" \"Cat\" \"Merones cat\");"; // invalid statement I removed the " ' " from Merone's cat because regex split on that, fix the regex to only split on () or ,
        
        
        tokenize(select_command);
        tokenize(select_command2);
        /*tokenize(create_command);
        tokenize(create_command2);
        tokenize(prior_insert);
        tokenize(alter_command1);
        tokenize(display_command);
        tokenize(display_command2);
        table.printRelations();
        tokenize(select_command);
        tokenize(alter_command2);
        tokenize(alter_command3);
        tokenize(alter_command4);
        tokenize(display_command);
        tokenize(display_command2);
        tokenize(select_command);
        table.printRelations();
        tokenize(insert_command);
        tokenize(insert_command1);
        tokenize(select_command);
        table.printRelations();
        tokenize(drop_command);
        table.printRelations();
        tokenize(select_command);
        tokenize(create_command2);
        tokenize(insert_command);
        tokenize(insert_command2);
        tokenize(insert_command3);
        tokenize(display_command);
        tokenize(display_command2);
        tokenize(select_command);
        table.catalog_to_file();
        tokenize(quit_command);*/
        /*tokenize(create_3);
        tokenize(insert_4);
        tokenize(select_command);
        tokenize(insert_5);
        tokenize(insert_6);
        tokenize(insert_7);
        tokenize(display_command);
        tokenize(select_command);
        tokenize(drop_2);
        tokenize(display_command2);
        tokenize(insert_8);
        //tokenize(display_command2);*/

    }

    /**
     * Taking in the command all on 1 line, breaking it up using regex splitting and
     * matching
     * We retrieve the command, table_name and optional tuples in accordance to the
     * commands requirements
     * 
     * @param command The entire command on 1 line
     * @return None
     */
    public void tokenize(String command) {
        //System.out.println("Command is: " + command);
        //String regex = "\\([^)(]*(?:\\([^)(]*(?:\\([^)(]*(?:\\([^)(]*\\)[^)(]*)*\\)[^)(]*)*\\)[^)(]*)*\\)|\\\"[A-Za-z.0-9_ ]*\\\"|[\\*A-Za-z.0-9_><!=]*";
        //String regex = "\\(.*\\)|\\d+\\.?\\d*|\".*\"|\\w*\\.?\\w*|[><=!]+";
        String regex = "[><=!*]+|\\(.*\\)|\\d+\\.?\\d*|\".*\"|\\w*\\.?\\w*";

        // Above regex is essentially matches all words while also keep anything in "()"
        // together
        ArrayList<String> command_tokens = new ArrayList<>();
        Matcher matcher = Pattern.compile(regex)
                .matcher(command);
        while (matcher.find()) {
            String found = matcher.group(0);
            //System.out.println("Found: " + found);
            if (!found.equals("")) {
                command_tokens.add(found);
            }
        }
        String keyword = command_tokens.get(0).toLowerCase();
        int numInputs = command_tokens.size();

        boolean success = false;

        switch (keyword) {
            case "create":
                if (numInputs < 2) {
                    System.out.println("Usage: create table <name> <a_name> <a_type> <constraint_1>;");
                    break;
                }
                String table_name = command_tokens.get(2);
                String table_info = command_tokens.get(3);
                ArrayList<ArrayList<String>> value_list = clean_create_string(table_info); 
                
                // check if table actually has attributes
                if(value_list.isEmpty()){
                    System.out.println("Error: Cannot create a table without attributes");
                    break;
                }
                else{
                    success = ddl_parser.create_table(table_name, value_list);
                    // System.out.println(success);

                }
             

                break;

            case "alter":
                if (numInputs < 5) {
                    System.out.println("Usage: alter table <name> drop <a_name>;");
                    System.out.println("       alter table <name> add <a_name> <a_type>;");
                    System.out.println("       alter table <name> add <a_name> <a_type> default <value>;");
                    break;
                }
                success = ddl_parser.alter_table(command_tokens);
                //System.out.println(success);
                break;

            case "drop":
                // System.out.println("Drop Command regex result: " + command_tokens);
                if (numInputs < 3) {
                    System.out.println("Usage: drop table <name>;");
                    break;
                }
                table_name = command_tokens.get(2);
                success = ddl_parser.drop_table(table_name);
                System.out.println(success);
                break;

            case "insert":
                //System.out.println("Insert Command regex result: " + command_tokens);
                if (numInputs < 5) {
                    System.out.println("Usage: insert into <name> values <tuples>;");
                    break;
                }
                // TODO: Take rest of tokens and parse variables for insert function
                table_name = command_tokens.get(2);
                ArrayList<String> table_values = new ArrayList<>();
                for (int i = 4; i < command_tokens.size(); i++) {
                    table_values.add(command_tokens.get(i));
                }
                value_list = clean_insert_string(table_values, matcher);
                //System.out.println("Values after cleaning insert: " + value_list);
                success = dml_parser.insert(table_name, value_list);
                System.out.println(success);
                break;

            case "display":

                if(numInputs == 2 || numInputs == 3){
                    success = dml_parser.display_info(command_tokens);
                    // System.out.println(success);
                }
                else{
                    System.out.println("Usage: display schema;\nUsage: display info <table_name>;");
                }

                // success = dml_parser.display_info(command_tokens);
                //System.out.println(success);
                break;

            case "select":
                // System.out.println("Select Command regex result: " + command_tokens);
            // Update to handle phase 2 specific inputs for select. No longer just "select * from <name>"
            // Select will always be at least 4 inputs, so this conditional does still hold.
            
                if (numInputs < 4) {
                    System.out.println("Usage: select <a_1>, ..., <a_n> from <t1>, ..., <tn>");
                    System.out.println("\t Optional: where <condition(s)> orderby <a_1>;");
                    break;
                }
                ArrayList<List<String>> partitions = clean_dml_command_string(command_tokens);
                //System.out.println("Select after cleaning tokens: " + partitions);
                success = dml_parser.select(partitions.get(0), partitions.get(1),partitions.get(2),partitions.get(3));

                System.out.println(success);

                break;

            case "update":
                if(numInputs < 8 ){
                    System.out.println("Usage: update <name> set <column_1> = <value>\t Optional: where <condition>");
                    break;
                }

                // System.out.println("Update Command regex result: " + command_tokens);

                String tablName = command_tokens.get(1);

                // separate command tokens list in two two sub-lists:
                // the list of updates ( col1 = val...)
                // the where list of conditions (col1 = val... )

                List<String> inputs = command_tokens.subList(3, command_tokens.size());

                int whereIndex = inputs.indexOf("where");

                // check if where list is there 
                if(whereIndex == -1){
                    success = dml_parser.update(tablName, new ArrayList<>(inputs), null);
                    break;
                }

                List<String> updateSublist = inputs.subList(0, whereIndex);
                List<String> whereSubtlist = inputs.subList(whereIndex + 1, inputs.size());


                ArrayList<String> updateList =  new ArrayList<>(updateSublist);
                ArrayList<String> whereList = new ArrayList<>(whereSubtlist);

             
                success = dml_parser.update(tablName, updateList, whereList);


                break;

            case "delete":
                if(numInputs < 3 || !command_tokens.get(1).equals("from")){
                    System.out.println("Usage: delete from <name> \t Optional: where <condition>");
                    break;
                }
                // System.out.println("Delete Command regex result: " + command_tokens);

                String tableName = command_tokens.get(2);
                if (command_tokens.size() > 3) {
                    if(command_tokens.get(3).equals("where")){

                        List<String> where_list = command_tokens.subList(4, command_tokens.size());
                        if(where_list == null){
                            success = dml_parser.delete(tableName, null);

                        }
                        else{
                            success = dml_parser.delete(tableName, where_list);

                        }

                    }
                }
                else {
                    success = dml_parser.delete(tableName, null);
                }
                
                
                break;

            case "quit":
                System.out.println("Quitting.... Have a good day");
                break;

            default:
                System.out.println("Doesn't match any command");
                break;
        }
    }

    /**
     * The table info/schema holds info about each attribute, these can be split up
     * into arrays with each array
     * holding their respective attribute info. The string needs to be cleared of
     * any "()" or ","
     * 
     * @param table_info Attribute info to be split up and paired together
     * @return Arrays holding attribute info
     */
    private ArrayList<ArrayList<String>> clean_create_string(String table_info) {
        ArrayList<ArrayList<String>> value_list = new ArrayList<>();
        for (String value : table_info.split(",")) {
            ArrayList<String> value_tokens = new ArrayList<>();
            for (String split_value : value.split("[\\(\\)]")) {
                if (!split_value.equals("")) {
                    for (String token : split_value.split(" ")) {
                        if (!token.equals("")) {
                            value_tokens.add(token);
                        }
                    }
                }
            }
            value_list.add(value_tokens);
        }
        return value_list;
    }

    /**
     * table values hold tuple values, the purpose is to split the strings into
     * their respective values in each index
     * while also cleaning the data of any "()" or ","
     * 
     * @param table_info Tuple info to be split up and paired together
     * @return Arrays holding tuple values
     */
    private ArrayList<ArrayList<String>> clean_insert_string(ArrayList<String> table_values, Matcher matcher) {
        ArrayList<ArrayList<String>> value_list = new ArrayList<>();
        for (String group : table_values) {
            for (String value : group.split(",")) {
                ArrayList<String> value_tokens = new ArrayList<>();
                for (String split_value : value.split("[\\(\\)]")) {
                    if (!split_value.equals("")) {
                        matcher = Pattern.compile("\\w+\\.*\\d*|\"[\\w\\s]*\"").matcher(split_value);
                        while (matcher.find()) {
                            String found = matcher.group(0);
                            if (!found.equals("")) {
                                value_tokens.add(found);
                            }
                        }
                    }
                }
                value_list.add(value_tokens);
            }
        }
        return value_list;
    }
    private ArrayList<List<String>> clean_dml_command_string(ArrayList<String> command_string) {
        // System.out.println("Select command: " + command_string);
        int commandIndex = command_string.indexOf("select");
        // System.out.println("Select index: " + commandIndex);
        int fromIndex =  command_string.indexOf("from");
        // System.out.println("From index: " + fromIndex);
        int whereIndex =  command_string.indexOf("where");
        // System.out.println("Where index: " + whereIndex);
        int orderIndex =  command_string.indexOf("orderby");
        // System.out.println("orderby index: " + orderIndex);

        

        List<String> command_list = null;
        if(fromIndex != -1) {
            command_list = command_string.subList(commandIndex+1, fromIndex); //selectIndex+1 because want to exclude keywords
        }
        List<String> from_list = null;
        if(whereIndex == -1 && orderIndex == -1) {
            from_list = command_string.subList(fromIndex+1, command_string.size());
        } else if (whereIndex == -1){ // orderby exists
            from_list = command_string.subList(fromIndex+1, orderIndex);
        } else { // where exists
            from_list = command_string.subList(fromIndex+1, whereIndex);
        }
        List<String> where_list = null;
        if(whereIndex != -1) {
            if(orderIndex == -1) {
                where_list = command_string.subList(whereIndex+1, command_string.size());
            } else {
                where_list = command_string.subList(whereIndex+1, orderIndex);
            }
        }
        List<String> order_list = null;
        if(orderIndex != -1) {
            order_list = command_string.subList(orderIndex+1, command_string.size());
        }
        
        ArrayList<List<String>> result = new ArrayList<>();
        result.add(command_list);
        result.add(from_list);
        result.add(where_list);
        result.add(order_list);
        
        return result;
    }

}
