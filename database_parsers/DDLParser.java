/*
 * File: DDLParser.java
 * 
 * Desc: The DDL Parser will manage all operations regards to Catalog and Pages
 * 
 * Authors: Alex Lee        al3774@rit.edu
 *          Merone Delnesa  mtd6620@rit.edu
 *          Isaac Mixon     igm3923@rit.edu
 *          Ethan Nunez     ern1274@rit.edu
 */
package database_parsers;

import java.io.File;
import java.util.ArrayList;

import src.Catalog;
import src.DataType;
import src.StorageManager;
import src.Table;
import src.Catalog.Relation;

public class DDLParser {
    /**
     * Constructor for DDLParser
     * @param cat
     * @param storage_manager
     */
    public DDLParser(Catalog cat, StorageManager storage_manager){
        this.cat = cat;
        this.storage_manager = storage_manager;
    }
    Catalog cat;
    StorageManager storage_manager;
    
    // All function assumes the parameters passed in are perfectly formatted
    // The tokenizer will be in a separate file because it will be used for DML parsing

    /**
      * Creates table info and attributes and stores within catalog as long as table does not already exist
      * @param table_name Table to create
      * @param attributes information about table variables/attributes
      * @return None
      */
    public boolean create_table(String table_name, ArrayList<ArrayList<String>> attributes){
        boolean is_duplicate = false;
        for (Relation relation: cat.get_table_schema()){
            if (table_name.equals(relation.get_name())) {
                is_duplicate = true;
            }
        }
        // before anything, check to see if a table already exists with the given name 
        if(is_duplicate == false){
            // System.out.println("Create " + table_name);
            // System.out.println("Attributes: " + attributes.toString());
            for (ArrayList<String> attr_info : attributes) {
                Object[] type = DataType.convertStringToType(attr_info.get(1));
                if (type == null) {
                    System.out.println("Error: string_type needs to be one of the following: integer, double, boolean, char or varchar");
                    return false;
                }
            }
            // Catalog.Relation new_relation = new Catalog.Relation(table_name, attributes, cat.get_next_id());
            // ArrayList<DataType> types = cat.add_relation(new_relation);
            

            // check for primary keys, only 1 allowed
            int primaryKeyCount = 0;

            for(int i = 0; i < attributes.size(); i++){
                if(attributes.get(i).contains("primarykey")){
                    primaryKeyCount++;
                }
            }
            if(primaryKeyCount > 0){
                if(primaryKeyCount > 1){
                    System.out.println("Error: Multiple primary keys");
                    return false;
                }
                else{
                    Catalog.Relation new_relation = new Catalog.Relation(table_name, attributes, cat.get_next_id());
                    ArrayList<DataType> types = cat.add_relation(new_relation);
                    if(types != null) {

                        storage_manager.createTable(new_relation.get_id(), new_relation.get_pkIndex(), types);
    
                    } else {
                        System.out.println("Error: Create was not successful");
                        return false;
                    }
                }

                
                
            }
            else{
                System.out.println("Error: No primary key defined");
                return false;
            }

            return true;

        }
        else{
            System.out.println("Error: Table " + table_name + " already exists.");
            return false;
        }

        
    }

    /**
      * Alters table info and attributes within catalog that gets applied to every existing record in table
      * @param table_name Table to alter
      * @param add True if adding attribubtes to table, False if removing attributes
      * @param attributes information about table variables/attributes to delete/add
      * @return None
      */
    public boolean alter_table(ArrayList<String> command_tokens){
        String table_name = command_tokens.get(2);
        String add_drop = command_tokens.get(3);
        String attr_name = command_tokens.get(4);
        DataType attr_type = null;
        int i = 5;
        Relation r = cat.get_relation(table_name);
        ArrayList<DataType> oldTypes = new ArrayList<>();
        for (DataType dataType : r.get_colTypes()) {
            oldTypes.add(dataType);
        }

        if(i == command_tokens.size()) { // if drop attribute, size is 5
            Catalog.Attribute new_a = new Catalog.Attribute(table_name, attr_name, null, null);
            i = cat.alter_relation(new_a, attr_type, add_drop);
            if (i != -2) {
                i = storage_manager.dropTableAttrib(r.get_id().intValue(), r.get_pkIndex().intValue(), oldTypes, i);
                r.remove_page(i);
            }
            return true;
        }

        ArrayList<String> table_values = new ArrayList<>();
        Object[] type = DataType.convertStringToType(command_tokens.get(i));
        i++;
        int size = (int) type[1];
        if(size == 0) {
            String size_str = command_tokens.get(6);
            for (String split_value : size_str.split("[\\(\\)]")) {
                if (!split_value.equals("")) {
                    size = Integer.parseInt(split_value);
                }
            }
            i++;
        }
        attr_type = new DataType((DataType.Type) type[0], size);
        for (;i < command_tokens.size(); i++) {
            table_values.add(command_tokens.get(i));
        }
        //System.out.println("ALTER TOKENS: " + command_tokens.toString());
        //System.out.println("Alter " + table_name);
        //System.out.println(add_drop + ": " + table_values.toString());
        Catalog.Attribute new_a = new Catalog.Attribute(table_name, attr_name, table_values, attr_type);
        i = cat.alter_relation(new_a, attr_type, add_drop);
        if(i == -1) {
            i = storage_manager.alterTable(r.get_id().intValue(), r.get_pkIndex().intValue(), oldTypes, attr_type, new_a.get_default_value());
            r.add_page(i);
        } else {
            System.out.println("Error: Failed to alter table");
            return false;
        }
        return true;
    }
    /**
      * Deletes table info and attributes from catalog as long as table exists
      * @param table_name Table to delete
      * @return None
      */
    public boolean drop_table(String table_name){
        // System.out.println("Drop " + table_name);
        Integer dropped_id = cat.drop_relation(table_name);
        if(dropped_id != null) {
            //System.out.println("Buffer");
            storage_manager.dropTable(dropped_id);
        } else {
            System.out.println("Error: Drop was not successful");
            return false;
        }
        System.out.println("Successfully dropped " + table_name);
        return true;
    }
}
