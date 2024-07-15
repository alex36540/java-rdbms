/*
 * File: StorageManager.java
 * 
 * Desc: The storage manager will manage all file operations
 * 
 * Authors: Alex Lee        al3774@rit.edu
 *          Merone Delnesa  mtd6620@rit.edu
 *          Isaac Mixon     igm3923@rit.edu
 *          Ethan Nunez     ern1274@rit.edu
 */
package src;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.BitSet;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;


public class StorageManager 
{
   private Catalog catalog;
   private final int pageSize;
   private String dbLoc;
   private BufferManager bufferManager;

   /*
   * TEMP TEST FUNCTION
   */
   public void test() {
      


      ArrayList<DataType> types1 = new ArrayList<>();
      types1.add(new DataType(DataType.Type.INTEGER, DataType.INTEGER_SIZE));
      // types1.add(new DataType(DataType.Type.INTEGER, DataType.INTEGER_SIZE));
      //  types1.add(new DataType(DataType.Type.DOUBLE, DataType.DOUBLE_SIZE));
      //  types1.add(new DataType(DataType.Type.BOOLEAN, DataType.BOOLEAN_SIZE));
      //  types1.add(new DataType(DataType.Type.CHAR, 10));
      // types1.add(new DataType(DataType.Type.VARCHAR, 8));

      Table t1 = new Table(0, 0, types1, pageSize, bufferManager, 0);

      ArrayList<Object> data1 = new ArrayList<>();
      // data1.add(null);
      data1.add(Integer.valueOf(1));
      // data1.add(Double.valueOf(1029320.11));
      // data1.add(Boolean.valueOf(true));
      // data1.add("ga");
      // data1.add("wee");

      ArrayList<Object> data2 = new ArrayList<>();
      data2.add(Integer.valueOf(2));

      ArrayList<Object> data3 = new ArrayList<>();
      data3.add(Integer.valueOf(3));

      ArrayList<Object> data4 = new ArrayList<>();
      data4.add(Integer.valueOf(4));

      ArrayList<Object> data5 = new ArrayList<>();
      data5.add(Integer.valueOf(5));

      Record r1 = new Record(types1, data1);
      Record r2 = new Record(types1, data2);
      Record r3 = new Record(types1, data3);
      Record r4 = new Record(types1, data4);
      Record r5 = new Record(types1, data5);
   
      t1.insertRecord(r1);
      t1.insertRecord(r2);
      t1.insertRecord(r2);
      t1.insertRecord(r4);
      t1.insertRecord(r5);

      String path = getTblPath(t1.getId());

      t1.write(path);

      // Table tt2 = Table.read(path, 0, 0, types1, pageSize, bufferManager);


   }

   public StorageManager(Catalog catalog, int pageSize, String dbLoc, BufferManager bufferManager) {
      this.catalog = catalog;
      this.pageSize = pageSize;
      this.dbLoc = dbLoc;
      this.bufferManager = bufferManager;
   }

   /**
   * Gets a page from a table by number
   * 
   * @param tbl Table to look at
   * @param pagenum Number of the page
   * @return the page with its data
   */
   public Page getPage(int tblId, int pageId) {
      Catalog.Relation relation = catalog.get_relation(tblId);
      Table table = new Table(tblId, relation.get_pkIndex(), relation.get_colTypes(), catalog.get_page_size(), bufferManager, relation.get_num_of_pages());
      HashSet<Integer> pagesToSkip = table.getPageIdSet();

      try (FileInputStream fis = new FileInputStream(getTblPath(tblId)+ ".tbl");
        ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[pageSize];
            int bytesRead;
            int pages_left_in_table = catalog.get_relation(tblId).get_num_of_pages();
            while ((bytesRead = fis.read(buffer)) != -1 || pages_left_in_table < 1) {
                byte[] byteArray;
                baos.write(buffer, 0, bytesRead);
                byteArray = baos.toByteArray();
                String bString = DataType.bArrayToBString(byteArray);
                Page page = Page.fromBinaryString(bString, pageSize, tblId, catalog.get_relation(tblId).get_colTypes(), pagesToSkip, null);
                if (page != null) {
                  bufferManager.addPageToBuffer(page);

                  if(page.getTblId() == tblId && page.getId() == pageId) {
                     return page;
                   }
                }
                pages_left_in_table--;
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
      return null;
   }

   public String getTblPath(int id) {
      return dbLoc + "tables" + File.separator + id;
   }

   /**
   * Inserts a record into a table 
   *
   * @param tbl Table to insert into
   * @param record The record to insert
   * @param path The filepath to the table files
   */
   public int insertRecord(int tableId, ArrayList<Object> data, int pkIndex, ArrayList<DataType> colTypes) {
      Table t = new Table(tableId, pkIndex, colTypes,pageSize,bufferManager,catalog.get_relation(tableId).get_num_of_pages());
      Table.read(getTblPath(tableId), t, null);
      //System.out.println("Num pages: " + t.getNumPages());
      //System.out.println("Num pages: " + t.getPages().size());
      
      // Safeguard against bad inserts
      if (data.size() != colTypes.size()) {
         System.out.println("Error: Insert failed, number of data " + data.size() + " does not match number of columns " + colTypes.size());

         return 0;
      }

      Record r = new Record(colTypes, data);
      //System.out.println("About to do insert record");
      int pagesAdded =  t.insertRecord(r);

      return pagesAdded;
}


   /**
   * Deletes a record from a table by primary key 
   *
   * @return how many pages were removed
   */
   public int deleteRecord(int tableId, Object pkVal, int pkIndex, ArrayList<DataType> colTypes) {
      Table t = new Table(tableId, pkIndex, colTypes,pageSize,bufferManager,catalog.get_relation(tableId).get_num_of_pages());
      Table.read(getTblPath(tableId), t, null);
      return t.deleteRecord(pkVal);
   }

   /**
   * Updates record in a table by primary key 
   * @return net page number change from this operation
   */
   public int updateRecord(int tableId, Object pkVal, int pkIndex, ArrayList<DataType> colTypes, ArrayList<Object> newData) {
      int pagesRemoved = deleteRecord(tableId, pkVal, pkIndex, colTypes);
      int pagesAdded = insertRecord(tableId, newData, pkIndex, colTypes);

      return pagesAdded - pagesRemoved;
   }

   /**
    * Creates an empty table, adds it to the arraylist, and creates an empty file for it
    * @param id
    * @param pkIndex
    * @param colTypes
    */
   public void createTable(int id, int pkIndex, ArrayList<DataType> colTypes) {
      Table t = new Table(id, pkIndex, colTypes, pageSize, bufferManager, 0);

      t.write(getTblPath(id));
   }

   /**
    * Deletes a table file
    * @param id
    */
   public void dropTable(int id) {
      File tableFile = new File(getTblPath(id)+".tbl"); 
      if (!tableFile.delete()) { 
         System.out.println("Error: Table does not exist, or could not be deleted");
      } 
   }

   /**
    * Returns all of the data in one column of a given table
    * @param tableId
    * @param colIndex
    * @param pkIndex
    * @param colTypes
    * @return list of data 
    */
   public ArrayList<Object> getCol(int tableId, int colIndex, int pkIndex, ArrayList<DataType> colTypes) {
      Table t = new Table(tableId, pkIndex, colTypes,pageSize,bufferManager,catalog.get_relation(tableId).get_num_of_pages());
      Table.read(getTblPath(tableId), t, null);

      ArrayList<Record> records = t.getAllRecords();
      ArrayList<Object> colData = new ArrayList<>();

      for (Record r : records) {
         Object val = r.getData().get(colIndex);
         colData.add(val);
      }
      return colData;
   }

   /**
    * Returns all of the data in a table
    * @param tableId
    * @param pkIndex
    * @param colTypes
    * @return data in table 
    */
   public ArrayList<ArrayList<Object>> selectAll(int tableId, int pkIndex, ArrayList<DataType> colTypes) {
      int numPages = catalog.get_relation(tableId).get_num_of_pages();

      // Gets available pages from the buffer manager
      Table t = new Table(tableId, pkIndex, colTypes, pageSize, bufferManager, numPages);
      Table.read(getTblPath(tableId), t, null);

      ArrayList<Record> records = t.getAllRecords();
      ArrayList<ArrayList<Object>> allData = new ArrayList<>();

      for (Record r : records) {
         ArrayList<Object> row = r.getData();
         allData.add(row);
      }

      return allData;
   }

   /**
    * Appends a given attribute to the table
    * @param tableId
    * @param pkIndex
    * @param oldColTypes
    * @param typeToAdd
    * @param defaultVal
    * @return net change in # of pages
    */
   public int alterTable(int tableId, int pkIndex, ArrayList<DataType> oldColTypes, DataType typeToAdd, Object defaultVal) {
      Table t = new Table(tableId, pkIndex, oldColTypes, pageSize, bufferManager, catalog.get_relation(tableId).get_num_of_pages());
      Table.read(getTblPath(tableId), t, null);

      int res = t.addAttribute(typeToAdd, defaultVal);

      return res;
   }

   /**
    * Drops an attribute from the table, given by its index
    * @param tableId
    * @param pkIndex
    * @param oldColTypes
    * @param indexToDrop
    * @return the net change in # of pages
    */
   public int dropTableAttrib(int tableId, int pkIndex, ArrayList<DataType> oldColTypes, int indexToDrop) {
      int numPages = catalog.get_relation(tableId).get_num_of_pages();
      Table t = new Table(tableId, pkIndex, oldColTypes, pageSize, bufferManager, numPages);

      if (t.getPages().size() < numPages) {
         Table.read(getTblPath(tableId), t, null);
      }
      return t.dropAttribute(indexToDrop);
   }

    /**
     * Finds the cartesian product of ONLY TWO tables
     * @param t1 
     * @param t2 
     * @return
     */
    public ArrayList<ArrayList<Object>> twoSetProduct(ArrayList<ArrayList<Object>> t1, ArrayList<ArrayList<Object>> t2) {
      ArrayList<ArrayList<Object>> result = new ArrayList<>();

      // for every row in original
      for (ArrayList<Object> oldRow : t1) {
          
         // for every row in second table
         for (ArrayList<Object> newRow : t2) {
            ArrayList<Object> temp = new ArrayList<>(oldRow);
            temp.addAll(newRow); // appends elements of the newRow to the array
            result.add(temp);
          }
      }
      return result;
   }

   /**
    * Returns an arraylist of rows. Contains all possible combinations of data between all of the tables.
    * Appends table data to each row in the order of the IDs
    * @param tableIds
    * @return All rows of the cartesian product
    */
    public ArrayList<ArrayList<Object>> cartesianProduct(ArrayList<Integer> tableIds) {
      ArrayList<ArrayList<Object>> output = new ArrayList<>();
      
      for (int i = 0; i < tableIds.size(); i++) {
         // Retrieve each table and its records
         int id = tableIds.get(i);
         Catalog.Relation r = catalog.get_relation(id);
         Table t = new Table(id, r.get_pkIndex(), r.get_colTypes(), catalog.get_page_size(), bufferManager, r.get_num_of_pages());
         Table.read(getTblPath(id), t, null);
         ArrayList<ArrayList<Object>> records = t.getAllRecordsAsList();

         // If first table, just add it all 
         if (i == 0) {
            output = new ArrayList<>(records);
            continue;
         }
         else {
            output = twoSetProduct(output, records);
         }
      }

      return output;
    }

   /**
    * Returns the rows that satisfy the given condition
    * @param allData result of a cartesian product of some tables
    * @return Rows that satisfy the condition.
    */
   public ArrayList<ArrayList<Object>> filterByCondition(ArrayList<ArrayList<Object>> data, Condition c, ArrayList<Integer> compareObjectIndices) {
      ArrayList<ArrayList<Object>> output = new ArrayList<>();
      
      for (ArrayList<Object> row : data) {
         ArrayList<Object> objectsToCompare = new ArrayList<>();
         for (Integer objIndex : compareObjectIndices) {
            objectsToCompare.add(row.get(objIndex));
         }
         if (Condition.evaluateCondition(c.getRoot(), objectsToCompare)) {
            
            output.add(row);
         }
      }

      return output;
   }

   /**
    * Returns the data ordered by a specific column
    * @param data input data
    * @param colTypes types of the data to make sure comparison
    * @param index the index of the column to sort by
    * @return sorted data
    */
   public ArrayList<ArrayList<Object>> orderBy(ArrayList<ArrayList<Object>> data, ArrayList<DataType> colTypes, int index) {
         // Define a custom comparator to compare rows based on the value at the specified index
        Comparator<ArrayList<Object>> comparator = new Comparator<ArrayList<Object>>() {
            @Override
            public int compare(ArrayList<Object> row1, ArrayList<Object> row2) {
               // Compare the values at the specified index
               Object val1 = row1.get(index);
               Object val2 = row2.get(index);

               switch (colTypes.get(index).type) {
                  case INTEGER:
                     return ((Integer) val1).compareTo((Integer) val2);
                  case DOUBLE:
                     return ((Double) val1).compareTo((Double) val2);
                  case BOOLEAN:
                     return ((Boolean) val1).compareTo((Boolean) val2);
                  case CHAR:
                  case VARCHAR:
                     return ((String) val1).compareTo((String) val2);
                  default:
                     return 0;
               }
            }
        };

        // Sort the data using the comparator
        Collections.sort(data, comparator);

        // Return the sorted data
        return data;
   }

   /**
    * Returns only the columns specified 
    * @param data input data
    * @param cols the columns that will be outputted
    * @param tables the tables that the columns are from
    * @return projected data
    */
   public ArrayList<ArrayList<Object>> projection(ArrayList<ArrayList<Object>> data, ArrayList<Integer> cols) {

      ArrayList<ArrayList<Object>> result = new ArrayList<>();
      for (ArrayList<Object> row : data) {
         ArrayList<Object> newRow = new ArrayList<>();
         for (int i : cols) {
            newRow.add(row.get(i));
         }
         result.add(newRow);
      }
      return result;
   }
 }