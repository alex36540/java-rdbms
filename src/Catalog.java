package src;

import java.util.ArrayList;
import java.util.List;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Type;
/*
 * File: Catalog.java
 * 
 * Desc: The Catalog will be the storage for any and all metadata for the database. 
 * Catalog is represented in data structure and can write itself to file
 * 
 * Authors: Alex Lee        al3774@rit.edu
 *          Merone Delnesa  mtd6620@rit.edu
 *          Isaac Mixon     igm3923@rit.edu
 *          Ethan Nunez     ern1274@rit.edu
 */
public class Catalog {
    public static class Attribute {
        String relation_name;
        String attribute_name;
        ArrayList<String> constraints;
        Object default_value = null;
        /**
         * Default constructor
         * @param rel_name
         */
        public Attribute (String rel_name) {
            relation_name = rel_name;
        }
        /**
         * Parametized constructor
         * @param rel_name
         * @param name
         * @param constraints
         * @param type
         */
        public Attribute(String rel_name, String name, ArrayList<String> constraints, DataType type) {
            this.relation_name = rel_name;
            this.attribute_name = name;
            this.constraints = constraints;
            if(constraints != null) {
                check_for_default(type);
            }
        }
        /**
         * Returns the name of the attribute
         * @return name of attribute
         */
        public String get_attribute_name() {
            return this.attribute_name;
        }
        /**
         * Returns constraints of the attribute
         * @return list of constraints
         */
        public ArrayList<String> get_constraints() {
            return this.constraints;
        }
        /**
         * Returns the default value of the attribute
         * @return the default value 
         */
        public Object get_default_value() {
            return this.default_value;
        }
        /**
         * Given the constraints, if it is not null, 
         * check if any of the constraints are "default". 
         * This function, if default is found, assigns current attribute default value
         * 
         * @param DataType The type that default value is supposed to be if found
         * @return None
         */
        private void check_for_default(DataType type) {
            int default_i = 0;
            boolean is_default = false;
            for (int i = 0; i < constraints.size(); i++) {
                String constraint = constraints.get(i);
                if(constraint.equals("default")) {
                    is_default = true;
                    default_i = i;
                }
            }
            if(is_default) {
                
                this.default_value = DataType.convertStringToType(type.type, constraints.get(default_i+1));
                constraints.remove(default_i);
                constraints.remove(default_i);
            }
        }
        /**
         * Given the type of attribute, this function essentially
         * breaks down each variable integral to identifying the attribute
         * adding them sequentially to a binary string and returns binary string 
         * The binary string returned is formatted (var_name type stored) like:
         * "len_attr_name INTEGER, attr_name VARCHAR, num_constraints INTEGER, 
         * len_constraint1 INTEGER, constraint1 VARCHAR (repeat for constraint num_constraints times),
         *  is_default_null fBOOLEAN (next 2 entries if true, stop if false), len_default DataType.Type, default DataType.Type"
         * @param DataType The type that default value is supposed to be if found
         * @return None
         */
        public String toBinaryString(DataType type) {
            String bString = "";
            bString += DataType.toBinaryString(attribute_name, DataType.Type.VARCHAR, 0);
            bString += DataType.toBinaryString(constraints.size(), DataType.Type.INTEGER, DataType.INTEGER_SIZE);
            for (int j = 0; j < constraints.size(); j++) {
                String constraint = constraints.get(j);
                bString += DataType.toBinaryString(constraint, DataType.Type.VARCHAR, 0);
            }
            if (default_value == null) {
                bString += DataType.toBinaryString(false, DataType.Type.BOOLEAN, DataType.BOOLEAN_SIZE);
            } else {
                bString += DataType.toBinaryString(true, DataType.Type.BOOLEAN, DataType.BOOLEAN_SIZE);
                bString += DataType.toBinaryString(default_value, type.type, type.getSize());
            }
            return bString;
        }
        /**
         * Given the type of attribute, this function essentially
         * breaks down each variable integral to identifying the attribute
         * adding them sequentially to a binary string and returns binary string 
         * The binary string taken in is formatted (var_name type stored) like:
         * "len_attr_name INTEGER, attr_name VARCHAR, num_constraints INTEGER, 
         * len_constraint1 INTEGER, constraint1 VARCHAR (repeat for constraint num_constraints times),
         *  is_default_null BOOLEAN (next 2 entries if true, stop if false), len_default DataType.Type, default DataType.Type"
         * @param String bString the binary string that is getting decoded
         * @param int offset, the pointer to the current index in bString 
         * @param DataType type the type that the attribute is and will get default value as type
         * @return incremented offset
         */
        public int fromBinaryString(String bString, int offset, DataType type) {
            String len_name_str = bString.substring(offset, offset+DataType.INTEGER_SIZE);
            offset += DataType.INTEGER_SIZE;
            Integer len_name = (Integer) DataType.fromBinaryString(len_name_str, DataType.Type.INTEGER, DataType.INTEGER_SIZE) * 8;
            String attr_name = bString.substring(offset, offset+len_name);
            offset += len_name;
            this.attribute_name = (String) DataType.fromBinaryString(attr_name, DataType.Type.CHAR, len_name);
            
            String num_const_str = bString.substring(offset, offset+DataType.INTEGER_SIZE);
            offset += DataType.INTEGER_SIZE;
            Integer num_of_constraints = (Integer) DataType.fromBinaryString(num_const_str, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
            this.constraints = new ArrayList<>();
            for (int i = 0; i < num_of_constraints; i++) {
                String len_const_str = bString.substring(offset, offset+DataType.INTEGER_SIZE);
                offset += DataType.INTEGER_SIZE;
                Integer len_constraint = (Integer) DataType.fromBinaryString(len_const_str, DataType.Type.INTEGER, DataType.INTEGER_SIZE) * 8;
                
                String constraint = bString.substring(offset, offset+len_constraint);
                offset += len_constraint;
                this.constraints.add((String) DataType.fromBinaryString(constraint, DataType.Type.CHAR, len_constraint));
            }

            String isnull = bString.substring(offset, offset+DataType.BOOLEAN_SIZE);
            offset += DataType.BOOLEAN_SIZE;
            boolean has_default = (boolean) DataType.fromBinaryString(isnull, DataType.Type.BOOLEAN, DataType.BOOLEAN_SIZE);
            if (has_default) {
                if (type.type == DataType.Type.VARCHAR) {
                    String len_default_str = bString.substring(offset, offset+DataType.INTEGER_SIZE);
                    offset += DataType.INTEGER_SIZE;
                    Integer len_default = (Integer) DataType.fromBinaryString(len_default_str, DataType.Type.INTEGER, DataType.INTEGER_SIZE) * 8;

                    
                    String default_value = bString.substring(offset, offset+len_default);
                    offset += len_default;
                    this.default_value = DataType.fromBinaryString(default_value, DataType.Type.CHAR, len_default);
                } else {
                    String default_value = bString.substring(offset, offset+type.getSize());
                    offset += type.getSize();
                    this.default_value = DataType.fromBinaryString(default_value, type.type, type.getSize());
                }
            }
            return offset;  
        }

        /**
         * Checks if an attribute is the primary key
         * @return true if the attribute is the primary key, false otherwise
         */
        public boolean is_primary() {
            for (String constraint: constraints) {
                if(constraint.toLowerCase().equals("primarykey")) {
                    return true;
                }
            }
            return false;
        }

        /**
         * This function compares 2 Attribute objects by comparing their names
         * @param Object The second Attribute object to compare to
         * @return true if same attribute_name; false otherwise
         */
        @Override
        public boolean equals(Object o) {
            boolean same = false;
            if (o != null && o instanceof Attribute){
                same = this.attribute_name == ((Attribute) o).attribute_name;
            }
            return same;
        }
    }


    public static class Relation {
        Integer num_of_pages = 0;
        Integer num_of_records = 0;
        Integer id;
        Integer pkIndex;
        String name;
        int num_of_attributes;
        ArrayList<DataType> colTypes;
        ArrayList<DataType> oldColTypes = null;
        ArrayList<Attribute> attributes_metadata;
        String path_to_table;

        public Relation() {}

        /**
         * Paramatized constructor
         * @param name
         * @param attr_list
         * @param id
         */
        public Relation(String name, ArrayList<ArrayList<String>> attr_list, int id) {
            this.name = name;
            this.id = Integer.valueOf(id);
            this.num_of_attributes = attr_list.size();
            this.colTypes = new ArrayList<DataType>();
            this.attributes_metadata = new ArrayList<Attribute>();
            this.path_to_table = name+".tbl";// TODO: figure out path to table

            // check for double primaries
            int primaryKeyCount = 0; 
            for(ArrayList<String> attribute: attr_list){
                if(attribute.contains("primarykey")){
                    primaryKeyCount++;
                }
            }

            if(primaryKeyCount > 1){
                System.out.println("Error: More than 1 primary key entered.");
            }
            else{
                for (ArrayList<String> attr_info : attr_list) {
                    int i = 2;
                    Object[] type = DataType.convertStringToType(attr_info.get(1));
                    int size = (int) type[1];
                    if(size == 0) {
                        size = Integer.parseInt(attr_info.get(2));
                        i++;
                    }
                    DataType attr_type = new DataType((DataType.Type) type[0], size);
                    colTypes.add(attr_type);
                    attributes_metadata.add(new Attribute(name, attr_info.get(0), new ArrayList<String>(attr_info.subList(i, attr_info.size())), attr_type)); // Constraints are passed into attribute
                    
                    if(attributes_metadata.get(attributes_metadata.size() - 1).is_primary()) {
                        
                        if(this.pkIndex == null) {
                            this.pkIndex = Integer.valueOf(attributes_metadata.size()-1);
                        } 
                    }
                }
            }
            
        }

        /**
         * Returns the id of a given relation
         * @return id of the relation if it exists, null otherwise
         */
        public Integer get_id() {
            return this.id;
        }
        /**
         * Returns the name of a given relation 
         * @return name of the relation if it exists, null otherwise
         */
        public String get_name() {
            return this.name;
        }

         /**
         * Returns the pkIndex of a given relation
         * @return pkIndex of the relation if it exists, null otherwise
         */
        public Integer get_pkIndex(){
            return this.pkIndex;
        }
        /**
         * Returns the number of attributes of a given relation
         * @return pkIndex of the relation if it exists, null otherwise
         */
        public Integer get_num_of_attributes(){
            return this.num_of_attributes;
        }
        /**
         * Returns the attribute Types of a given relation
         * @return colTypes of the relation if it exists, null otherwise
         */
        public ArrayList<DataType> get_colTypes(){
            return this.colTypes;
        }
        /**
         * Returns the attributes of a given relation
         * @return ArrayList<Attribute> of the relation if it exists, null otherwise
         */
        public ArrayList<Attribute> get_attributes(){
            return this.attributes_metadata;
        }
        /**
         * Returns the number of pages of a given relation 
         * @return number of pages of the relation if it exists, null otherwise
         */
        public Integer get_num_of_pages() {
            return this.num_of_pages;
        }

        /**
         * Returns the number of records of a given relation 
         * @return number of records of the relation if it exists, null otherwise
         */
        public Integer get_num_of_records() {
            return this.num_of_records;
        }


        /**
         * This function adds Attribute new_a to relation given the attribute and datatype of attribute
         * @param Attribute new_a the attribute to be added
         * @param DataType The type that default value is supposed to be if found
         * @return None
         */
        public void add_attr(Attribute new_a, DataType type) {
            this.oldColTypes = new ArrayList<>();
            for (DataType oldType : colTypes) {
                oldColTypes.add(new DataType(oldType.type, oldType.size));
            }
            this.attributes_metadata.add(new_a);
            this.num_of_attributes++;
            this.colTypes.add(type);
        }
        /**
         * This function removes Attribute attribute given the index of attribute
         * @param int index of attribute to be removed
         * @return None
         */
        public void remove_attr(int index_to_remove) {
            this.oldColTypes = new ArrayList<>();
            for (DataType oldType : colTypes) {
                oldColTypes.add(new DataType(oldType.type, oldType.size));
            }
            attributes_metadata.remove(index_to_remove);
            colTypes.remove(index_to_remove);
            num_of_attributes--;
        }

        public ArrayList<DataType> getOldTypes() {
            return this.oldColTypes;
        }

        // Functions adding and removing pages/records

        public void add_page(int amt_of_new) {
            this.num_of_pages += amt_of_new;
        }
        public void remove_page(int amt_deleted) {
            this.num_of_pages-= amt_deleted;
        }

        public void add_record() {
            this.num_of_records++;
        }
        public void remove_record() {
            this.num_of_records--;
        }
        public void reset_old_cols() {
            this.oldColTypes = null;
        }

        /**
         * This function essentially
         * breaks down each variable integral to identifying the relation data structure
         * adding them sequentially to a binary string and returns binary string 
         * The binary string returned is formatted (var_name type stored) like:
         * "num_pages INTEGER, num_of_recs INTEGER, len_rel_name INTEGER, rel_name VARCHAR, len_path INTEGER, path VARCHAR, 
         *  num_attributes INTEGER, 
         *  type_char CHAR, attribute.toBinaryString() (repeat for Attribute num_attributes times),
         * @return bString the binary string that holds the decomposition of relation
         */
        public String toBinaryString() {
            String bString = "";
            bString += DataType.toBinaryString(num_of_pages, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
            bString += DataType.toBinaryString(num_of_records, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
            bString += DataType.toBinaryString(name, DataType.Type.VARCHAR, 0);
            bString += DataType.toBinaryString(path_to_table, DataType.Type.VARCHAR, 0);
            bString += DataType.toBinaryString(num_of_attributes, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
            bString += DataType.toBinaryString(id, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
            bString += DataType.toBinaryString(pkIndex, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
            for (int i = 0; i < num_of_attributes; i++) {
                String attrType = String.valueOf(colTypes.get(i).type.name().charAt(0));
                bString += DataType.toBinaryString(attrType, DataType.Type.CHAR, 8); // Expected to be 1 character only
                bString += DataType.toBinaryString(colTypes.get(i).getSize(), DataType.Type.INTEGER, DataType.INTEGER_SIZE); // Make new Data type from these by
                                                                                                                            // by reading in a char then a integer, a function for char to be corresponded to type
                Attribute attr = attributes_metadata.get(i);
                bString += attr.toBinaryString(colTypes.get(i));
            }
            return bString;
        }
        /**
         * This function essentially
         * breaks down each variable integral to identifying the relation data structure
         * adding them sequentially to a binary string and returns binary string 
         * The binary string returned is formatted (var_name type stored) like:
         * "num_pages INTEGER, num_of_recs INTEGER, len_rel_name INTEGER, rel_name VARCHAR, len_path INTEGER, path VARCHAR, 
         *  num_attributes INTEGER, 
         *  type_char CHAR, attribute.fromBinaryString(type) (repeat for Attribute num_attributes times),
         * @return bString the binary string that holds the decomposition of relation
         */
        public int fromBinaryString(String bString, int offset) {
            String num_pages_str = bString.substring(offset, offset+DataType.INTEGER_SIZE);
            offset += DataType.INTEGER_SIZE;
            Integer num_of_pages = (Integer) DataType.fromBinaryString(num_pages_str, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
            this.num_of_pages = num_of_pages;

            String num_rec_str = bString.substring(offset, offset+DataType.INTEGER_SIZE);
            offset += DataType.INTEGER_SIZE;
            Integer num_of_records = (Integer) DataType.fromBinaryString(num_rec_str, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
            this.num_of_records = num_of_records;

            String len_rel_str = bString.substring(offset, offset+DataType.INTEGER_SIZE);
            offset += DataType.INTEGER_SIZE;
            Integer len_rel = (Integer) DataType.fromBinaryString(len_rel_str, DataType.Type.INTEGER, DataType.INTEGER_SIZE) * 8;
            String rel_name = bString.substring(offset, offset+len_rel);
            offset += len_rel;
            this.name = (String) DataType.fromBinaryString(rel_name, DataType.Type.CHAR, len_rel);

            String len_path_str = bString.substring(offset, offset+DataType.INTEGER_SIZE);
            offset += DataType.INTEGER_SIZE;
            Integer len_path = (Integer) DataType.fromBinaryString(len_path_str, DataType.Type.INTEGER, DataType.INTEGER_SIZE) * 8;
            String path = bString.substring(offset, offset+len_path);
            offset += len_path;
            this.path_to_table = (String) DataType.fromBinaryString(path, DataType.Type.CHAR, len_path);

            String num_attr_str = bString.substring(offset, offset+DataType.INTEGER_SIZE);
            offset += DataType.INTEGER_SIZE;
            Integer num_of_attr = (Integer) DataType.fromBinaryString(num_attr_str, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
            this.num_of_attributes = num_of_attr;

            String id_str = bString.substring(offset, offset+DataType.INTEGER_SIZE);
            offset += DataType.INTEGER_SIZE;
            Integer id = (Integer) DataType.fromBinaryString(id_str, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
            this.id = id;

            String pkIndex_str = bString.substring(offset, offset+DataType.INTEGER_SIZE);
            offset += DataType.INTEGER_SIZE;
            Integer pkIndex = (Integer) DataType.fromBinaryString(pkIndex_str, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
            this.pkIndex = pkIndex;
            this.attributes_metadata = new ArrayList<>();
            this.colTypes = new ArrayList<>();
            for (int i = 0; i < num_of_attr; i++) {
                String len_type_str = bString.substring(offset, offset+8);
                offset += 8;
                String type_str = (String) DataType.fromBinaryString(len_type_str, DataType.Type.CHAR, 8);
                String len_type_size_str = bString.substring(offset, offset+DataType.INTEGER_SIZE);
                offset += DataType.INTEGER_SIZE;
                Integer type_size = (Integer) DataType.fromBinaryString(len_type_size_str, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
                DataType.Type curr_type = convert_to_type(type_str);
                if (curr_type == DataType.Type.VARCHAR || curr_type == DataType.Type.CHAR) {type_size = type_size / 8;}
                this.colTypes.add(new DataType(curr_type, type_size));
                this.attributes_metadata.add((new Attribute(this.name)));
                offset = this.attributes_metadata.get(i).fromBinaryString(bString, offset, this.colTypes.get(i));
            }
            return offset;            
        }
        /**
         * This function takes in a String type which is a one letter string
         * and maps it to its respective type based on the first letter of the DataType.Type enum
         * @param String type_char the string to be converted
         * @return DataType type of DataType.Type
         */
        public DataType.Type convert_to_type(String type) {
            switch(type) {
                case "I" : return DataType.Type.INTEGER;
                case "D" : return DataType.Type.DOUBLE;
                case "B" : return DataType.Type.BOOLEAN;
                case "C" : return DataType.Type.CHAR;
                case "V" : return DataType.Type.VARCHAR;
            }
            System.out.println("Error: Something messed up in convert_to_type: " + type);
            return null;
        }
        /**
         * This function compares 2 Relation objects by comparing their names
         * @param Object The second Relation object to compare to
         * @return true if same relation name; false otherwise
         */
        @Override
        public boolean equals(Object o) {
            boolean same = false;
            if (o != null && o instanceof Relation){
                same = this.name == ((Relation) o).name;
            }
            return same;
        }
        public Integer is_col_in_relation(String colName) {
            for (int i = 0; i < this.attributes_metadata.size(); i++) {
                Attribute attribute = this.attributes_metadata.get(i);
                if(attribute.attribute_name.equals(colName)) {
                    return i;
                }
            }
            return null;
        }
    }
    String db_loc;
    int page_size;
    int buffer_size;
    int id_counter = 0;
    ArrayList<Relation> relations_metadata = new ArrayList<>();
    String catalog_path = "catalog_test.cat"; // Change path later on when path is figured out
    // Path will always be hardcoded, when storing catalog, the path will not be stored

    // Constructors
    public Catalog(String bString){catalog_from_file(bString);}
    public Catalog(String loc, int page_size, int buffer_size) {
        this.db_loc = loc;
        catalog_path = loc + catalog_path;
        this.page_size = page_size;
        this.buffer_size = buffer_size;
    }
    public Catalog(String loc, int buffer_size) {
        this.db_loc = loc;
        catalog_path = loc + catalog_path;
        byte[] bytes = null;
        this.buffer_size = buffer_size;
        try (FileInputStream fis = new FileInputStream(catalog_path); 
            ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[1024 * 16];
            int bytesRead = 0;
            while ((bytesRead = fis.read(buffer)) != -1) {
                baos.write(buffer, 0, bytesRead);
            }
            bytes = baos.toByteArray();
        } catch (Exception e) {}
        if (bytes != null) {
            String bString = DataType.bArrayToBString(bytes);
            catalog_from_file(bString);
            if (relations_metadata.size() == 0) {
                this.id_counter = 0;
            }
            else {
                this.id_counter = relations_metadata.get(relations_metadata.size() - 1 ).id + 1;
            }
            this.buffer_size = buffer_size;
        } else {
            System.out.println("Error: File was empty");
        }
        // If file doesn't exist, catalog will be initialized, just empty
    }

    // Getters
    public int get_next_id() {
        int to_return = id_counter;
        this.id_counter++;
        return to_return;
    }
    public Integer get_page_size() {
        return this.page_size;
    }
    public Integer get_buffer_size() {
        return this.buffer_size;
    }
    public String get_db_loc() {
        return this.db_loc;
    }
    public String get_catalog_path() {
        return this.catalog_path;
    }
    public ArrayList<Relation> get_table_schema(){
        return this.relations_metadata;
    }
    public void set_page_size(int new_size) {
        this.page_size = new_size;
    }
    /**
    * This function takes in a Relation new_r
    * and checks if it is a valid relation to add
    * @param Relation new_r the relation to be added
    * @return true if successfully added, false otherwise
    */
    public ArrayList<DataType> add_relation(Relation new_r) {
        if (relations_metadata.contains(new_r)){
            System.out.println("Error: This relation already exists");
            return null;
        }
        if(new_r.num_of_attributes == new_r.colTypes.size() && new_r.attributes_metadata.size() == new_r.num_of_attributes){
            this.relations_metadata.add(new_r);
            return new_r.colTypes;
        }
        System.out.println("Error: The number of attributes does not match the size of attributes list");
        return null;
    }
    /**
    * This function takes in a Attribute to_add_drop, DataType
    * and checks if it is a valid relation to add
    * @param Attribute to_add_drop the attribute to be added
    * @param DataType type the data type of attribute
    * @param String string containing if add or drop attribute
    * @return true if successfully added or dropped, false otherwise
    */
    public int alter_relation(Attribute to_add_drop, DataType type, String add_drop) {
        //TODO: Alter relation will be the hardest in my opinion, we must account for 
        // adding values to existing tables and to catalog
        // or dropping values from existing tables and from catalog
        // While still respecting integrity constraints
        for (Relation relation: relations_metadata){
            if(relation.name.equals(to_add_drop.relation_name)){
                if(add_drop.equals("add")){
                    if(relation.attributes_metadata.contains(to_add_drop)) {
                        System.out.println("Error: Attribute already exists");
                        return -2;
                    }
                    relation.add_attr(to_add_drop, type);
                    return -1;

                } else if(add_drop.equals("drop")){
                    for (int i = 0; i < relation.attributes_metadata.size(); i++){
                        Attribute attr = relation.attributes_metadata.get(i);
                        if(attr.attribute_name.equals(to_add_drop.attribute_name)){
                            if(checkConstraintsDrop(attr.constraints)){
                                relation.remove_attr(i);
                                // System.out.println("Removed");

                                return i;
                            } else {
                                return -2;
                            }
                        }
                    }
                    System.out.println("Error: Attribute doesn't exist");  
                } else {
                    System.out.println("Error: Only add or drop");
                    return -2;
                }
            }
        }
        System.out.println("Error: Relation does not exist");
        return -2;
    }
    /**
    * This function takes in a String table_name
    * and checks if it is a valid relation to drop
    * @param String table_name the name of the table to be dropped
    * @return true if successfully dropped, false otherwise
    */
    public Integer drop_relation(String table_name) { 
        Relation r = get_relation(table_name);

        if(r != null){
            Integer to_return = r.id;
            relations_metadata.remove(r);
            return to_return;
        }
        else{
            System.out.println("Error: Table never existed");
            return null;
        }
    }
    
    /**
    * This function checks if a attribute can be dropped by checking its constraints for primarykey
    * if it has primarykey, it cannot be dropped
    * @param ArrayList<String> Constraints of the attribute to be dropped
    * @return true if successfully added, false otherwise
    */
    public boolean checkConstraintsDrop(ArrayList<String> constraints) {
        for (String constraint: constraints){
            if(constraint.toLowerCase().equals("primarykey")){
                System.out.println("Error: Cannot delete attribute that is a primary key");
                return false;
            }
        }
        return true;
    }
    /**
    * This function prints catalog relations purely for testing purposes
    */
    public void printRelations(){
        System.out.println("id counter at: " + id_counter);
        System.out.println("catalog_path: " + catalog_path);
        System.out.println("page size: " + page_size/8);
        System.out.println("buffer size: " + buffer_size);
        System.out.println("PRINTING RELATIONS");
        for (Relation relation: relations_metadata){
            System.out.println("Relation num of pages: " + relation.num_of_pages);
            System.out.println("Relation num of records: " + relation.num_of_records);
            System.out.println("Relation name: " + relation.name);
            System.out.println("Relation number of attributes: " + relation.num_of_attributes);
            System.out.println("Relation ID: " + relation.id);
            System.out.println("Relation Primary Key Index: " + relation.pkIndex);
            //System.out.println("Relation column types: " + relation.colTypes.toString());
            for (DataType type : relation.colTypes) {
                System.out.println("Attribute Type: " + type.type.name() + " Attribute Size: " + type.size);
            }
            System.out.println("Attribute names below");
            for (Attribute attribute: relation.attributes_metadata){
                System.out.println("Attribute name: " + attribute.attribute_name);
                System.out.println("Attribute constraints: " + attribute.constraints.toString());
            }
            System.out.println("New Relation path: " + relation.path_to_table+"\n");
        }
    }
    /**
    * This function essentially
    * breaks down each variable integral to identifying the catalog
    * adding them sequentially to a binary string and converts binary string to byte array and then write byte array to file
    * The binary string is formatted (var_name type stored) like:
    * "num_of_relations INTEGER, relation.toBinaryString() (repeat for relation num_of_relations times)"
    * @return None
    */
    public void catalog_to_file(){ 
        String bString = "";
        int num_of_relations = relations_metadata.size();
        bString += DataType.toBinaryString(page_size, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
        bString += DataType.toBinaryString(num_of_relations, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
        for (int i = 0; i < num_of_relations; i++) {
            Relation relation = relations_metadata.get(i);
            bString += relation.toBinaryString();
        }
        //System.out.println("DECONSTRUCTED CATALOG");
        //System.out.println(bString);
        byte[] byteArray = DataType.bStringToBArray(bString);
        File f = new File(catalog_path);
        try (FileOutputStream fos = new FileOutputStream(catalog_path)) {
            fos.write(byteArray);
            //System.out.println("Catalog binary string successfully written to file");
        } catch (IOException e) {
            System.err.println("Error: Error writing binary string to file: " + e.getMessage());
        }
        
    }
    /**
    * This function essentially
    * reconstructs each variable integral to identifying the catalog
    * composing them sequentially from a binary string
    * The binary string is formatted (var_name type stored) like:
    * "num_of_relations INTEGER, relation.fromBinaryString() (repeat for relation num_of_relations times)"
    * @return None
    */
    public void catalog_from_file(String bString){
        //System.out.println("CONSTRUCTION CATALOG");
        //System.out.println(bString);
        int offset = 0;

        String page_size_str = bString.substring(offset, offset+DataType.INTEGER_SIZE);
        offset += DataType.INTEGER_SIZE;
        Integer page_size = (Integer) DataType.fromBinaryString(page_size_str, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
        this.page_size = page_size;

        String num_relations_str = bString.substring(offset, offset+DataType.INTEGER_SIZE);
        offset += DataType.INTEGER_SIZE;
        Integer num_of_relations = (Integer) DataType.fromBinaryString(num_relations_str, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
        for (int i = 0; i < num_of_relations; i++) {
            Relation new_r = new Relation();
            offset = new_r.fromBinaryString(bString, offset);
            this.relations_metadata.add(new_r);
        }
    }

    /**
     * Returns a list of the indexes of the attributes in a relation 
     * that have a unique constraint
     * @param relation_name
     * @return list of indexes
     */
    public ArrayList<Integer> get_unique_indexes(String relation_name){

        Relation r = get_relation(relation_name);

        if(r != null){

            ArrayList<Attribute> attributes = r.attributes_metadata;
            ArrayList<Integer> indexes = new ArrayList<>();

            // Add the index of every attribute w/ constraint of "unique" to list of 
            // indexes & return 
            for(int i = 0; i < attributes.size(); i++){
                for(int j = 0; j < attributes.get(i).constraints.size(); j++){
                    if(attributes.get(i).constraints.get(j).contains("unique")){
                        indexes.add(i);
                    }
                }
            }

            return indexes;
            
        }
        else{
            // If the table is not found, return null 
            return null; 
        }
        
    }

    /**
     * Returns a list of the indexes of the attributes in a relation 
     * that have not null constraint
     * @param relation 
     * @return list of indexes
     */
    public ArrayList<Integer> get_not_null_indexes(Relation relation){

        ArrayList<Attribute> attributes = relation.attributes_metadata;
        ArrayList<Integer> indexes = new ArrayList<>();

        // Add the index of every attribute w/ constraint of "unique" to list of 
        // indexes & return 
        for(int i = 0; i < attributes.size(); i++){
            if(attributes.get(i).get_constraints().contains("notnull")) {
                indexes.add(i);
            }
        }

        return indexes;
    }

    /**
     * Returns a list of attributes which have been verified as valid
     * Only checks if not null if attribute has constraint notnull
     * unique and primarykey constraints are checked outside of this function
     * using the pruned ArrayList that satisfies the notnull constraints and data conversion
     * @param relation
     * @param attributes
     * @return list of verified attributes
     */
    public ArrayList<ArrayList<Object>> verify_attributes(Relation relation, ArrayList<ArrayList<String>> attributes){
        ArrayList<ArrayList<Object>> verified = new ArrayList<>();

        ArrayList<Object> temp = new ArrayList<>();
        ArrayList<DataType> columnTypes = relation.get_colTypes();
        ArrayList<Integer> notNullIndexes = get_not_null_indexes(relation);

        //System.out.println("Not null indexes: " + notNullIndexes.toString());

        for(int i = 0; i < attributes.size(); i++){
            for(int j = 0; j < attributes.get(i).size(); j++){

                String currentVariable = attributes.get(i).get(j);
                //System.out.println("I and J " + i + ": " + j);
                //System.out.println("Current Variable: " + currentVariable);
                if (notNullIndexes.contains(j)) {
                    if (currentVariable.equals("")||currentVariable.equals("null")) {
                        return verified;
                    }
                }
                Object curr_var = null;
                if (!(currentVariable.equals("")||currentVariable.equals("null"))) {
                    DataType currentType = columnTypes.get(j);
                    curr_var = DataType.convertStringToType(currentType.type, currentVariable);
                    if (curr_var == null) {
                        System.out.println("Error: Can't convert to type: " + currentVariable + " to " + currentType.type.name());
                        return verified;
                    }
                }

            
                

                temp.add(curr_var);
            }
            verified.add(temp);
            temp = new ArrayList<>();
        }

        return verified;

    }

    /**
     * Returns a relation given its name 
     * @param relation_name
     * @return relation object  
     */
    public Relation get_relation(String relation_name) {
        for (Relation relation : relations_metadata) {
            if (relation_name.equals(relation.name)) {
                return relation;
            }
        }
        return null;
    }
    /**
     * Returns a relation given its name 
     * @param relation_id
     * @return relation object  
     */
    public Relation get_relation(int relation_id) {
        for (Relation relation : relations_metadata) {
            if (relation_id == relation.get_id()) {
                return relation;
            }
        }
        return null;
    }
    public ArrayList<Integer[]> getAllIndicesFromTables(ArrayList<Relation> relations) {
        ArrayList<Integer[]> values_with_indices = new ArrayList<>();
        for (Relation relation : relations) {
            for (int i = 0; i < relation.attributes_metadata.size(); i++) {
                values_with_indices.add(new Integer[]{relation.id, i});
            }
        }
        return values_with_indices;
    }

    public ArrayList<Integer[]> check_column_validity_and_retrieve_indices(List<String> columns, List<String> table_names) {
        ArrayList<Integer[]> values_with_indices = new ArrayList<>();
        ArrayList<Relation> relations = new ArrayList<>();
        for (String table_name : table_names) {
            Relation result = get_relation(table_name);
            if(result == null) {
                System.out.println("Error: Table is not valid in column validity check: " + table_name);
                return null;
            }
            relations.add(result);
        }
        // BELOW IS COLUMN VALIDITY AND INDEX POPULATION
        for (String column : columns) {
            Integer colIndex = null;
            Integer relation_id = null;
            if(column.equals("*")){return getAllIndicesFromTables(relations);} // Tables are validated therefore select all attributes
            // it is assumed that if * is selected, it is alone
            int columnCount = 0;
            if(column.contains(".")) { // if contains . then it is tableName.attributeName
                String[] pair = column.split("\\.");
                for (Relation relation : relations) {
                    if(relation.name.equals(pair[0])) {
                        colIndex = relation.is_col_in_relation(pair[1]);
                        relation_id = relation.get_id();
                        columnCount += colIndex != null ? 1:0;
                        break;
                    }
                }
            } else {
                for (Relation relation : relations) {
                    colIndex = relation.is_col_in_relation(column);
                    relation_id = relation.get_id();
                    columnCount += colIndex != null ? 1:0; 
                    // col in relation to return index otherwise null
                }
            }
            if(columnCount != 1) {
                System.out.println("Error: Vague column therefore not valid: " + column);
                return null;
            }
            values_with_indices.add(new Integer[]{relation_id, colIndex});
        }
        return values_with_indices;
    }

     
}
