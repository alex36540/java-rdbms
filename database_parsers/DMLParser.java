/*
* File: DDLParser.java
* 
* Desc: The DML Parser will manage all operations regards to records and catalog
* 
* Authors: Alex Lee        al3774@rit.edu
*          Merone Delnesa  mtd6620@rit.edu
*          Isaac Mixon     igm3923@rit.edu
*          Ethan Nunez     ern1274@rit.edu
*/
package database_parsers;
import java.io.File;
import java.lang.reflect.Array;

import src.Catalog;
import src.Condition;
import src.DataType;
import src.StorageManager;
import src.Table;
import src.Catalog.Relation;

import java.util.ArrayList;
import java.util.List;


public class DMLParser {
    /**
     * Constructor for DML Parser
     * @param cat
     * @param storage_manager
     */
    public DMLParser(Catalog cat, StorageManager storage_manager){
        this.cat = cat;
        this.storage_manager = storage_manager;
    }
    String tables_path = "db_path" + File.separator + "tables" + File.separator;
    Catalog cat;
    StorageManager storage_manager;

      /**
      * Inserts into table records that satisfy the current table metadata constraints
      * @param table_name Table to insert records
      * @param values an array of records with proper attributes and satsifies constraints stated in catalog
      * @return None
      */
    public boolean insert(String table_name, ArrayList<ArrayList<String>> values) {
        Catalog.Relation table = cat.get_relation(table_name);
        if(table == null) {
            System.out.println("Error: Table does not exist");
            return false;
        }
        //System.out.println("Insert into " + table_name);
        //System.out.println(values.toString());

        ArrayList<Integer> unique_indexes = cat.get_unique_indexes(table_name);
        ArrayList<ArrayList<Object>> unique_values = new ArrayList<>();
        for(int i = 0; i < unique_indexes.size(); i++) {
          //System.out.println("Buffer");
          unique_values.add(storage_manager.getCol(table.get_id().intValue(), unique_indexes.get(i).intValue(), table.get_pkIndex(), table.get_colTypes()));
        }
        //System.out.println("Unique indexes" + unique_indexes.toString());
        //System.out.println("Unique values" + unique_values.toString());

        ArrayList<Object> pk_values = storage_manager.getCol(table.get_id().intValue(), table.get_pkIndex().intValue(),table.get_pkIndex().intValue(), table.get_colTypes());
        //ArrayList<Object> pk_values = new ArrayList<>();
        //System.out.println("Primary Key Values" + pk_values.toString());
        ArrayList<ArrayList<Object>> attributes = cat.verify_attributes(table, values);
        for (int i = 0; i < attributes.size(); i++) {
            ArrayList<Object> attribute = attributes.get(i);
            if (attribute.size() <= table.get_pkIndex()) {
                System.out.println("Error: Primary key not provided");
                return false;
            }
            boolean does_pk_exist = pk_values.contains(attribute.get(table.get_pkIndex()));
            if (does_pk_exist) {
                System.out.println("Error: primary key must be unique: " + attribute.get(table.get_pkIndex()));
                return false;
            }
            for (int j = 0; j < unique_values.size(); j++) {
                boolean attribute_index_exist = unique_values.get(j).contains(attribute.get(unique_indexes.get(j)));
                if (attribute_index_exist) {
                  System.out.println("Error: value of attribute must be unique: " + attribute.get(unique_indexes.get(j)));
                  return false;
                }
            }

            ArrayList<DataType> columnTypes = table.get_colTypes();

            // if type is char or varchar, check that the variable
            // is a valid length 

            for(int k = 0; k < values.size(); k++){
                for(int j = 0; j < values.get(k).size(); j++){

                    DataType.Type currentType = columnTypes.get(j).type;

                    String currentVariable = values.get(k).get(j);

                    switch(currentType){
                        case VARCHAR:
                            if(!(currentVariable.length() > 0 && currentVariable.length()-1 <= columnTypes.get(j).getSize() / 8)){
                                System.out.println("Error: Invalid variable length for attribute");
                                return false;
                            }
                            break;
                        case CHAR:
                            if(!(currentVariable.length() == columnTypes.get(j).getSize() / 8)){
                                System.out.println("Error: Invalid variable length for attribute");
                                return false;
                            }
                    }

                }
            }

            Integer amount_new_pages = storage_manager.insertRecord(table.get_id().intValue(), attribute, table.get_pkIndex(), table.get_colTypes());
            if (amount_new_pages > 0) {
                // System.out.println("Amount of new pages: " + amount_new_pages);
            }


            table.add_page(amount_new_pages);
            table.add_record();
        }
        if(attributes.size() < values.size()) {
            System.out.println("Error: Illegal Values : " + values.get(attributes.size()).toString());
            return false;
        }
        // System.out.println("Insertion is done");
        return true;
    }
    /**
    * Displays all table info: Table name, Schema, # of pages, # of records
    * @param table_name Table to display
    * @return None
    */
    public boolean display_info(ArrayList<String> command_tokens) {
        switch (command_tokens.get(1)) {
            case "schema":
                if (command_tokens.size() != 2){
                    System.out.println("Usage: display schema;");
                    return false;
                }
                // dml parser
                //System.out.println("Displaying Schema");
                System.out.println("Database location: " + cat.get_db_loc());
                System.out.println("Page size: " + cat.get_page_size()/8);
                System.out.println("Buffer size: " + cat.get_buffer_size());
                ArrayList<Catalog.Relation> relations = cat.get_table_schema();
                if (relations.size() == 0) {
                    System.out.println("No tables to display");
                }
                for (Catalog.Relation relation : relations) {
                    System.out.println("Table Schema: ");
                    System.out.println("Relation name: " + relation.get_name());
                    for (int i = 0; i < relation.get_attributes().size(); i++) {
                        Catalog.Attribute attribute = relation.get_attributes().get(i);
                        String attribute_name = "\t Attribute name: " + attribute.get_attribute_name();
                        String attribute_type = "\tAttribute type: " + relation.get_colTypes().get(i).type.name();
                        String attribute_constraints = "\tAttribute constraints: " + attribute.get_constraints().toString();
                        String attribute_default_value = "";
                        if(attribute.get_default_value() != null) {
                            attribute_default_value = "\tAttribute Default Value: " + attribute.get_default_value().toString();
                        }
                        System.out.println(attribute_name + attribute_type + attribute_constraints + attribute_default_value);
                    }
                }
                break;
            case "info":
            // dml parser
                if (command_tokens.size() != 3){
                    System.out.println("Usage: display info <table_name>;");
                    return false;
                }
                String table_name = command_tokens.get(2);
                // System.out.println("Display " + table_name);
                Catalog.Relation relation = cat.get_relation(table_name);
                if( relation == null) {
                    System.out.println("Error: Table does not exist");
                    return false;
                }
                System.out.println("Table Schema: ");
                System.out.println("Relation name: " + relation.get_name());
                for (int i = 0; i < relation.get_attributes().size(); i++) {
                    Catalog.Attribute attribute = relation.get_attributes().get(i);
                    String attribute_name = "\t Attribute name: " + attribute.get_attribute_name();
                    String attribute_type = "\tAttribute type: " + relation.get_colTypes().get(i).type.name();
                    String attribute_constraints = "\tAttribute constraints: " + attribute.get_constraints().toString();
                    String attribute_default_value = "";
                    if(attribute.get_default_value() != null) {
                        attribute_default_value = "\tAttribute Default Value: " + attribute.get_default_value().toString();
                    }
                    System.out.println(attribute_name + attribute_type + attribute_constraints + attribute_default_value);
                }
                System.out.println("Relation number of pages: " + relation.get_num_of_pages());
                System.out.println("Relation number of records: " + relation.get_num_of_records());
                break;
            default:
                System.out.println("Usage: display schema;\nUsage: display info;");
                break;
        }

        
        return true;
    }

    /**
    * Display all of the data in the table in an easy to read format, including column names.
    * @param table_name Table to select records from
    * @param values an array of records with proper attributes and satsifies constraints stated in catalog
    * Column names are included in retrieval, error if table doesn't exist
    * @return None
    */
    public boolean select(List<String> select_list, List<String> from_list,List<String> where_list, List<String> order_list ) {
        if(select_list == null || from_list == null) {
            System.out.println("Error: Don't know what to select or from where");
            return false;
        }
        // DONT NEED TO CHECK TABLES IN FROM_LIST BECAUSE PARSESELECT WILL DO IT DURING COLUMN VALIDATION
        ArrayList<Integer[]> values_to_select = cat.check_column_validity_and_retrieve_indices(select_list, from_list);
        if(values_to_select == null) {
            System.out.println("Error: Columns or Tables not validated");
            return false;
        }
        
        ArrayList<Integer[]> values_for_condition = where_list != null ? parseWhere(where_list, from_list) : null;
        Condition condition = null;

        if (values_for_condition != null) {
            condition = where_list != null ? new Condition(where_list, values_for_condition, cat) : null;
        }
        // Condition condition = where_list != null ? new Condition(where_list, values_for_condition, cat) : null;
        if (condition != null && condition.getRoot() == null) {
            System.out.println("Error: Error occurred during condition formation");
            return false;
        }

        if (values_for_condition == null && where_list != null) {
            return false;
        }
        

        ArrayList<Integer[]> values_to_orderby = order_list != null ? cat.check_column_validity_and_retrieve_indices(order_list, from_list) : null;
        if (values_to_orderby == null && order_list != null) {
            return false;
        }
        
        // Extract information from each table in the from_list
        ArrayList<Integer> tableIds = new ArrayList<>();
        ArrayList<DataType> all_types = new ArrayList<>(); // will need all table coltypes appended to each other
        ArrayList<Integer> condObjIndices = new ArrayList<>();

        for (String table_name : from_list) {
            Catalog.Relation table = cat.get_relation(table_name);
            if(values_for_condition != null) {
                for (Integer[] pair : values_for_condition) {
                    if(pair[0] == table.get_id()) {
                        condObjIndices.add(pair[1]+all_types.size()); 
                        // This guarantees that the object will be at index previous tables size + index within table
                    }
                }
            }
            
            tableIds.add(table.get_id());
            all_types.addAll(table.get_colTypes());
        }

        // Copy of above but for orderby instead of where
        ArrayList<DataType> all_types_ordered = new ArrayList<>(); 
        ArrayList<Integer> condObjIndices_ordered = new ArrayList<>();

        for (String table_name : from_list) {
            Catalog.Relation table = cat.get_relation(table_name);
            if(values_to_orderby != null) {
                Integer index = 0;
                for (Integer[] pair : values_to_orderby) {
                    if(pair[0] == table.get_id()) {
                        //condObjIndices_ordered.add(pair[1]+all_types_ordered.size()); 
                        condObjIndices_ordered.add(index, pair[1]+all_types_ordered.size()); 
                    }
                    index++;
                }
            all_types_ordered.addAll(table.get_colTypes());
            }
        }

         // Copy of above but for projection instead of where
         ArrayList<DataType> all_types_project = new ArrayList<>(); 
         ArrayList<Integer> condObjIndices_project = new ArrayList<>();
 
        for (String table_name : from_list) {
            Catalog.Relation table = cat.get_relation(table_name);
            if(values_to_select != null) {
                for (Integer[] pair : values_to_select) {
                    if(pair[0] == table.get_id()) {
                        condObjIndices_project.add(pair[1]+all_types_project.size()); 
                        // This guarantees that the object will be at index previous tables size + index within table
                    }
                }
            }
            
            all_types_project.addAll(table.get_colTypes());
        }

        ArrayList<ArrayList<Object>> data = null; 
        ArrayList<Integer> table_ids = new ArrayList<>();

        // SELECT * FROM table_name; 
        if (select_list.get(0).equals("*")) {
            for (String table_name: from_list){
                Catalog.Relation table = cat.get_relation(table_name);
                if(table == null) {
                    System.out.println("Error: Table does not exist: " + table_name);
                    return false;
                }
                for (int i = 0; i < table.get_attributes().size(); i++) {
                    Catalog.Attribute attribute = table.get_attributes().get(i);
                    String attribute_name = attribute.get_attribute_name();
                    if (!attribute_name.contains(".")){
                        attribute_name = table_name + "." + attribute_name;
                    }
                    System.out.print(attribute_name + "\t");
                }
                table_ids.add(table.get_id());
            }
            data = storage_manager.cartesianProduct(table_ids);
        }
        // SELECT col1, col2, ... FROM table_name;
        else {
            for (String col: select_list) {
                if (!col.contains(".")){
                    col = from_list.get(0) + "." + col;
                }
                System.out.print(col + "\t");
            }
            table_ids = new ArrayList<>();
            
            for (String table_name: from_list){
                Catalog.Relation table = cat.get_relation(table_name);
                if(table == null) {
                    System.out.println("Error: Table does not exist: " + table_name);
                    return false;
                }
                table_ids.add(table.get_id());
                // tables.add(table);
            }
            data = storage_manager.cartesianProduct(table_ids);
            
        }
        // WHERE CLAUSE IF NEEDED (FILTERING)
        if (where_list != null) {
            data = storage_manager.filterByCondition(data, condition, condObjIndices);
        }

        // ORDERBY IF NEEDED
        if (order_list != null) {
            int col_index = condObjIndices_ordered.get(0);
            data = storage_manager.orderBy(data, all_types_ordered, col_index);
        }

        // PROJECTING 
        if (select_list.get(0).equals("*") == false){
            data = storage_manager.projection(data, condObjIndices_project);
        }

        // PRINT OUT DATA
        for (ArrayList<Object> entry : data) {
            String valString = "";

            // Start a new line for each record
            System.out.println();
            for (Object val : entry) {
                // check for null
                if (val != null) {
                    valString = val.toString();
                }
                else {
                    valString = "null";
                }

                // print out each value separated by a tab
                System.out.print(valString + "\t");
            }
        }
        
        System.out.println();
        return true;
    }

    
    public ArrayList<Integer[]> parseWhere(List<String> where_list, List<String> from_list) {
        ArrayList<String> pruned_where_list = new ArrayList<>();
        for (String string : where_list) {
            //System.out.println("String is " + string);
            //System.out.println(Character.isDigit(string.charAt(0)));
            //System.out.println(string.charAt(0));

            if(!(string.contains("\"") || string.matches("<|>|>=|<=|<>|=|!=|and|or") || string.matches("-?\\d+(\\.\\d+)?"))){
                pruned_where_list.add(string);
            }

        }
        return cat.check_column_validity_and_retrieve_indices(pruned_where_list, from_list);
    }

    // stubbed out update() & delete()

    /**
     * Updates column of given table with new values  
     * @param table_name
     * @param column_list
     * @param values
     * @param where_list
     * @return
     */
    public boolean update(String table_name, ArrayList<String> updateList, ArrayList<String> whereList){

        if(whereList == null){

        }
        else{
            Relation fromTable = cat.get_relation(table_name);
            if(fromTable == null){
                System.out.println("Error: Table does not exist: " + table_name);
                return false;
            }
        
            int fromTableID = fromTable.get_id();
            int primaryKeyIndex = fromTable.get_pkIndex();
            
            ArrayList<String> prunedList = new ArrayList<>();
            
            // remove assignment operator
            for(String entry : updateList){
                if(!entry.equals("=")){
                    prunedList.add(entry);
                }
            }

            // if the length is uneven, the update list is invalid 
            if(prunedList.size() % 2 != 0){
                System.out.println("Error: invalid update list");
                return false;
            }



            ArrayList<String> from_list = new ArrayList<>();
            from_list.add(table_name);

            // ---------------------------------------------------
            ArrayList<Integer[]> values_for_condition = whereList != null ? parseWhere(whereList, from_list) : null;
            Condition condition = null;
            if (values_for_condition != null) {
                condition = whereList != null ? new Condition(whereList, values_for_condition, cat) : null;
            }
            String string = "";
            if(values_for_condition != null) {
                for (Integer[] integers : values_for_condition) {
                    string += ", (" + integers[0] + " , " + integers[1] + ")";
                }
                // System.out.println("Condition Values: " + string);
            }
            if (condition != null && condition.getRoot() == null) {
                System.out.println("Error: Error occurred during condition formation");
                return false;
            }
            // ---------------------------------------------------

            ArrayList<DataType> all_types = new ArrayList<>(); // will need all table coltypes appended to each other
            all_types.addAll(fromTable.get_colTypes());

            ArrayList<Integer> condObjIndices = new ArrayList<>();

            ArrayList<ArrayList<Object>> all_data = storage_manager.selectAll(fromTableID, fromTable.get_pkIndex(), fromTable.get_colTypes());

            if(values_for_condition != null) {
                for (Integer[] pair : values_for_condition) {
                    if(pair[0] == fromTableID) {
                        condObjIndices.add(pair[1]); 
                        // This guarantees that the object will be at index previous tables size + index within table
                    }
                }
            }


            // filter data by condition
            ArrayList<ArrayList<Object>> filteredData = storage_manager.filterByCondition(all_data, condition, condObjIndices);
            ArrayList<Catalog.Attribute> attributes = fromTable.get_attributes();
            ArrayList<String> attributeNames = new ArrayList<>();

            // get the list of attribute names to check against when 
            // parsing the list of updates to make
            for(Catalog.Attribute a : attributes){
                attributeNames.add(a.get_attribute_name());
            }

            // update these rows
            for(ArrayList<Object> row : filteredData){
                // System.out.println("Row: " + row);
                ArrayList<Object> temp = row;

                // loop through each (column, value) pair of updates to be made
                for(int i = 0; i < prunedList.size(); i = i + 2){

                    String col = prunedList.get(i);
                    String val = prunedList.get(i + 1);

                    int aIndex = 0; // attribute index 

                    // check if column name exists 
                    if(!attributeNames.contains(col)){
                        System.out.println("Error: Column " + col + "does not exist in table " + table_name);
                        return false;
                    }
                    else{
                        aIndex = attributeNames.indexOf(col);
                    }
                    
                    ArrayList<DataType> rowTypes = fromTable.get_colTypes();
                    // CHECK IF THIS IS AN UPDATE TO THE PRIMARY KEY 
                    if(aIndex == primaryKeyIndex){
                        ArrayList<Object> pkValues = storage_manager.getCol(fromTableID, fromTable.get_pkIndex(), fromTable.get_pkIndex(), fromTable.get_colTypes());
                        
                        Object convertedVal = DataType.convertStringToType(rowTypes.get(aIndex).type, val);

                        // check if the new primary key value is a duplicate  
                        if(pkValues.contains(convertedVal)){
                            System.out.println("Error: Duplicate primary key values");
                            return false;
                        }
                    }
                    else{

                        Object convertedVal = DataType.convertStringToType(rowTypes.get(aIndex).type, val);


                        // check if column requires unique values

                        ArrayList<String> aConstraints = attributes.get(aIndex).get_constraints();
                        if(aConstraints.contains("unique")){
                            ArrayList<Object> columnValues = storage_manager.getCol(fromTableID, aIndex, primaryKeyIndex, all_types);

                            if(columnValues.contains(convertedVal)){
                                System.out.println("Error: Duplicate value in unique attribute");
                                return false;
                            }
                        }
                        else{
                            // check if column requires not-null values 

                            if(val.equals("null") && aConstraints.contains("notnull")){
                                System.out.println("Error: Update of 'null' within not-null attribute");
                                return false;
                            }

                        }

                    }

                    // TODO: check if value is valid for column (?)
                    

                    temp.set(aIndex, DataType.convertStringToType(rowTypes.get(aIndex).type, val));
                }

                // update record
                int pages_changed = storage_manager.updateRecord(fromTableID, row.get(fromTable.get_pkIndex()), fromTable.get_pkIndex(), fromTable.get_colTypes(), temp);
                fromTable.add_page(pages_changed);
            }
        }
    return true;
    }

    /**
     * Deletes tuples from given table where provided conditions are true. 
     * @param table_name
     * @param where_list
     * @return
     */
    public boolean delete(String table_name, List<String> where_list){

        Relation fromTable = cat.get_relation(table_name);
            if(fromTable == null){
                System.out.println("Error: Table does not exist: " + table_name);
                return false;
            }
            
            
            int fromTableID = fromTable.get_id();

        
        if(where_list == null){
            // there is no "where" clause. delete everything from table
            ArrayList<Object> pkValues = storage_manager.getCol(fromTableID, fromTable.get_pkIndex(), fromTable.get_pkIndex(), fromTable.get_colTypes());

            for(Object pkVal : pkValues){

                // delete record
                int numPages = storage_manager.deleteRecord(fromTableID, pkVal, fromTable.get_pkIndex(), fromTable.get_colTypes());
                fromTable.remove_record();
                fromTable.remove_page(numPages);
            }

        }
        else{
            // delete tuples where condition is true

            ArrayList<String> from_list = new ArrayList<>();
            from_list.add(table_name);


            ArrayList<Integer[]> values_for_condition = where_list != null ? parseWhere(where_list, from_list) : null;
            Condition condition = null;

            if (values_for_condition != null) {
                condition = where_list != null ? new Condition(where_list, values_for_condition, cat) : null;
            }

            if (condition != null && condition.getRoot() == null) {
                System.out.println("Error Occurred during condition formation");
                return false;
            }

            ArrayList<DataType> all_types = new ArrayList<>(); // will need all table coltypes appended to each other
            all_types.addAll(fromTable.get_colTypes());

            ArrayList<Integer> condObjIndices = new ArrayList<>();


            ArrayList<ArrayList<Object>> all_data = storage_manager.selectAll(fromTableID, fromTable.get_pkIndex(), fromTable.get_colTypes());

            if(values_for_condition != null) {
                for (Integer[] pair : values_for_condition) {
                    if(pair[0] == fromTableID) {
                        condObjIndices.add(pair[1]); 
                        // This guarantees that the object will be at index previous tables size + index within table
                    }
                }
            }

            // filter data by condition
            ArrayList<ArrayList<Object>> filteredData = storage_manager.filterByCondition(all_data, condition, condObjIndices);

            // delete these rows
            for(ArrayList<Object> row : filteredData){
                int numPages = storage_manager.deleteRecord(fromTableID, row.get(fromTable.get_pkIndex()), fromTable.get_pkIndex(), fromTable.get_colTypes());
                fromTable.remove_record();
                fromTable.remove_page(numPages);
            }
        }
        return true;
    }
}