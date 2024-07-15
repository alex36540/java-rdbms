/*
 * File: Table.java
 * 
 * Desc: The class that represents a table
 * 
 * Authors: Alex Lee        al3774@rit.edu
 *          Merone Delnesa  mtd6620@rit.edu
 *          Isaac Mixon     igm3923@rit.edu
 *          Ethan Nunez     ern1274@rit.edu
 */
package src;

import java.util.LinkedList;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;

public class Table 
{
    private LinkedList<Page> pages;
    private ArrayList<DataType> colTypes;
    private int pkIndex;
    private int numPages;
    private int id;
    private int pageSize;
    private int pageIdCounter = 0;
    public BufferManager bufferManager;

    /**
     * Constructor when creating a new table
     */
    public Table(int id, int pkIndex, ArrayList<DataType> colTypes, int pageSize, BufferManager bufferManager, int numPages) {
        this.pages = bufferManager.populateTable(id);
        this.colTypes = colTypes;
        this.pkIndex = pkIndex;
        this.id = id; 
        this.numPages = numPages;
        this.pageSize = pageSize;
        this.bufferManager = bufferManager;
        /*this.pages = new LinkedList<>();
        LinkedList<Page> buffer_pages = bufferManager.getPagesInBuffer();
        for (Page page : buffer_pages) {
            if(page.getTblId() == id) {
                addPage(page);
            }
        } */
    }

    public Table(int id, int pageSize, BufferManager bufferManager) {
        this.pages = bufferManager.populateTable(id);
        this.colTypes = null;
        this.pkIndex = 0;
        this.id = id; 
        this.pageSize = pageSize;
        this.bufferManager = bufferManager;
    }

    /**
     * Inserts given Record object into the Table
     * @param record
     * @return returns the number of new pages added
     */
    public int insertRecord(Record record) {
        int pagesAdded = 0;

        // check for incorrect inserts
        ArrayList<Object> data = record.getData();
        if (data.size() != colTypes.size()) {
            System.out.println("Error: Insertion failed, data of size " + data.size() + " does not match columns of size " + colTypes.size());

            return pagesAdded;
        }

        // check for records that are just way too big (larger than possible available space)
        // Pages automatically hold 3 integers, so make sure to account for that
        if (record.getSize() > pageSize - DataType.INTEGER_SIZE * 3) {
            //System.out.println("Error: Insertion failed, record is larger than possible free space on a page");

            return pagesAdded;
        }
        
        //System.out.println("Inserting record of size: " + record.getSize());

        // If no pages
        if (pages.size() == 0) {
            Page p = new Page(pageSize, id, pageIdCounter); // counter will be zero
            p.insertRecord(record, 0); 
            pages.add(p);
            pagesAdded++;
            bufferManager.pushChange(p);
            bufferManager.addPageToBuffer(p);
            

            //System.out.println("Creating new page");
            return pagesAdded;
        }
        else {
            // Check the last 
            int indexToInsert = 0;

            for (int i = 0; i < pages.size(); i++) {
                Page p = pages.get(i);
                int numRecords = p.getNumRecords();
                indexToInsert = p.indexToAdd(record, pkIndex, colTypes);

                //System.out.println("\tOn page ID: " + p.getId() + " with " + p.getFreePtr() + "/" + pageSize + " space used");

                // If the index to add is equal to the size it means that the records belongs at the end of the page,
                // need to check next page to see if it goes there. If not, try inserting in this page.
                if (indexToInsert == numRecords && (i + 1) < pages.size()) {
                    if (pages.get(i + 1).indexToAdd(record, pkIndex, colTypes) != 0) {
                        bufferManager.addPageToBuffer(pages.get(i+1));
                        //System.out.println("\tvalue belongs in next page");
                        continue;
                    }
                }

                // Check duplicate
                if (indexToInsert != numRecords) {
                    if (DataType.compare("=", colTypes.get(pkIndex), p.getRecords().get(indexToInsert).getData().get(pkIndex), data.get(pkIndex))) {
                        System.out.println("Error: Insertion failed, duplicate primary key values");
                        return pagesAdded;
                    }
                }
                
                // Check overrun
                if (p.getFreePtr() + record.getSize() > pageSize) {
                    //System.out.println("\tSplitting page");

                    LinkedList<Page> splitPages = p.split(); // guaranteed to be an arraylist with 2 pages

                    Page p1 = splitPages.get(0);
                    Page p2 = splitPages.get(1);

                    p1.setId(i);
                    p2.setId(i + 1);
                    pagesAdded++;

                    if (indexToInsert <= numRecords / 2) {
                        p1.insertRecord(record, indexToInsert);
                        //System.out.println("\tInserting to left page at index: " + indexToInsert);
                    }
                    else {
                        //System.out.println("\tInserting to right page at index: " + (indexToInsert / 2));
                        p2.insertRecord(record, indexToInsert / 2);
                    }
                    
                    // update all of the pages ahead of this one, incrementing the ID of each one to make space for the new page
                    for (int j = i + 1; j < pages.size(); j++) {
                        Page pageToInc = pages.get(j);
                        pageToInc.setId(j + 1);
                        
                        bufferManager.pushChange(pageToInc);
                        bufferManager.addPageToBuffer(pageToInc);
                    }
                    //System.out.println("Finished Incrementing page ids");

                    // remove old page and add the split to pages
                    pages.remove(i);
                    pages.add(i, p2);
                    pages.add(i, p1); // reverse order because p1 will push p2 forward after adding it second
                    /*String pagestring = "";
                    for (Page print_p : pages) {
                        pagestring += print_p.getId() +" , ";
                    }
                    System.out.println("Order of pages: " + pagestring);
                    bufferManager.print_buffer();*/
                    bufferManager.pushChange(p1);
                    bufferManager.addPageToBuffer(p1);
                    bufferManager.pushChange(p2);
                    bufferManager.addPageToBuffer(p2);
                    /*pagestring = "";
                    for (Page print_p : pages) {
                        pagestring += print_p.getId() +" , ";
                    }
                    System.out.println("Order of pages after push to buffer: " + pagestring);
                    bufferManager.print_buffer();*/

                    return pagesAdded;
                    
                }
                else {
                    //System.out.println("\tInserting to page " + i + " at index " + indexToInsert);
                    //System.out.println("\tInserting to page " + p.getId() + " at index " + indexToInsert);
                    p.insertRecord(record, indexToInsert);
                    bufferManager.pushChange(p);
                    bufferManager.addPageToBuffer(p);
                    
                    

                    
                    return pagesAdded;
                }
            }
            return pagesAdded;
        }
    }

    /**
     * Deletes a record from the table
     * @param pkValue
     * @return the number of pages removed
     */
    public int deleteRecord(Object pkValue) {
        for (int i = 0; i < pages.size(); i++) {
            Page p = pages.get(i);

            if (p.deleteRecord(pkValue, pkIndex)) {
                if (p.getNumRecords() == 0){
                    // Shift IDs all pages in front of the page that will be removed
                    for (int j = i + 1; j < pages.size(); j++) {
                        Page pageToDecrement = pages.get(j);
                        pageToDecrement.setId(j - 1);

                        bufferManager.pushChange(pageToDecrement);
                        bufferManager.addPageToBuffer(pageToDecrement);
                    }
                    
                    pages.remove(i);
                    bufferManager.deletePageFromHardWare(p);
                    return 1;
                }
                
            }
        }

        // System.out.println("Error: Deletion failed, no record with this primary key exists");

        return 0;
    }

    /**
     * Adds given page to Table
     * @param p
     */
    public void addPage(Page p) {
        // Check if first page
        if (pages.isEmpty()) {
            pages.add(p);
            return;
        }
        
        // check for duplicate page IDs and do not insert if page is already there
        //System.out.println("This is num of pages: " + pages.size());
        for (int i = 0; i < pages.size(); i++) {
            if (pages.get(i).getId() == p.getId()) {
                return;
            }
        }

        // Insert the page at the correct spot in ID order TODO remove???
        int insertIndex = 0;
        for (int i = 0; i < pages.size(); i++) {
            if (pages.get(i).getId() > p.getId()) {
                insertIndex = i;
                break;
            } else {
                insertIndex = i + 1;
            }
        }
        pages.add(insertIndex, p);
    }

    public ArrayList<Record> getAllRecords() {
        ArrayList<Record> allRecords = new ArrayList<>();

        for (Page p : pages) {
            
            //bufferManager.addPageToBuffer(p); commented out because if a page is in the table it should already be in buffer
            for (Record r : p.getRecords()) {
                allRecords.add(r);
            }
        }

        return allRecords;
    }

    public ArrayList<ArrayList<Object>> getAllRecordsAsList() {
        ArrayList<ArrayList<Object>> allRecords = new ArrayList<>();

        for (Page p : pages) {
            
            //bufferManager.addPageToBuffer(p); commented out because if a page is in the table it should already be in buffer
            for (Record r : p.getRecords()) {
                allRecords.add(r.getData());
            }
        }

        return allRecords;
    }

    public ArrayList<DataType> getColTypes() {
        return colTypes;
    }

    public LinkedList<Page> getPages() {
        return pages;
    }

    public int getId() {
        return this.id;
    }

    public int getpkIndex() {
        return this.pkIndex;
    }
    public int getPageSize() {
        return this.pageSize;
    }
    public int getNumPages() {
        return this.numPages;
    }

    /**
     * returns a set of all page pageIDs currently in this table object
     * @return
     */
    public HashSet<Integer> getPageIdSet() {
        HashSet<Integer> hs = new HashSet<>();
        String skipstring = "";
        for (Page p : pages) {
            hs.add(p.getId());
            skipstring += p.getId() + " , ";
        }
        // System.out.println("Skipping pages: " + skipstring);

        return hs;
    }
    
    /**
     * Converts this table object into a binary string
     * @return the binary string
     */
    private String toBinaryString() {
        String bString = "";

        // Add number of pages
        //System.out.println("Saving amount of pages: " + pages.size());
        //bString += DataType.toBinaryString((Integer) pages.size(), DataType.Type.INTEGER, DataType.INTEGER_SIZE);

        // Add pages
        for (Page p : pages) {
            bString += p.toBinaryString();
        }

        return bString;
    }

    /**
     * Returns a table from a binary string
     * @param bString the binary string
     * @param pkIndex primary key index
     * @param colTypes types data structure
     * @param pageSize size of the pages in bits
     * @return the table
     */
    /// BELOW MIGHT NOT BE NECESSARY FUNCTION BECAUSE WE ARE POPULATING A TABLE DATA STRUCTURE THE FILE IS MADE OF PAGES
    /*
    private static Table fromBinaryString(String bString, int id, int pkIndex, ArrayList<DataType> colTypes, int pageSize, BufferManager bufferManager, int numOfPages) {
        int offset = 0;
        Table t = new Table(id, pkIndex, colTypes, pageSize, bufferManager, numOfPages);
    

        // Get numPages
        String numPagesStr = bString.substring(offset, offset + DataType.INTEGER_SIZE);
        int numPages = (Integer) DataType.fromBinaryString(numPagesStr, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
        offset += DataType.INTEGER_SIZE;
        //System.out.println("Num pages in fromBinaryString: " + numPages);
        // Add each page to the table
        for (int i = 0; i < numOfPages; i++) {
            String pageStr = bString.substring(offset, offset + pageSize);
            Page p = Page.fromBinaryString(pageStr, pageSize, id, colTypes); // TODO KEEP TRACK OF ID
            t.addPage(p);
            offset += pageSize;
        }
        
        return t;
    }
     */

    /**
     * Writes the entire table to a file in binary
     * @param path
     */
    public void write(String path) {
        String bString = this.toBinaryString(); 
        byte[] byteArray = DataType.bStringToBArray(bString);

        // Write byte array to file
        File f = new File(path + ".tbl");
        f.getParentFile().mkdirs();
        try (FileOutputStream fos = new FileOutputStream(path + ".tbl")) {
            fos.write(byteArray);
            //System.out.println("Binary string successfully written to file.");
        } catch (IOException e) {
            System.err.println("Error: Error writing binary string to file: " + e.getMessage());
            
        }
    }

    /**
     * Reads a table in from a file
     * @param path file path
     * @param pkIndex index of primary key
     * @param colTypes types data structure
     * @param pageSize size of the page in bits
     * @return the entire table as an object
     */
    public static Table read(String path, Table table, ArrayList<DataType> oldColTypes) { 
        HashSet<Integer> pagesToSkip = table.getPageIdSet();
        int amt_pages_before = table.pages.size();
        //System.out.println("Pages already in table: " + amt_pages_before);

        // If all pages are already in the table
        if (pagesToSkip.size() ==  table.getNumPages()) {
            // System.out.println("Skipped all pages in read");
            return table;
        }
        int pages_left_in_table = table.getNumPages() - pagesToSkip.size();
        
        // Read in file into a byte array
        try (FileInputStream fis = new FileInputStream(path+ ".tbl");
        ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            //byte[] initial = new byte[DataType.INTEGER_SIZE/8];
            int bytesRead;
            //bytesRead = fis.read(initial);
            //baos.write(initial, 0, bytesRead);
            
            
            //byteArray = baos.toByteArray();
            //String bString = DataType.bArrayToBString(byteArray);

            //int pages_left_in_table = (Integer) DataType.fromBinaryString(bString, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
            
            // System.out.println("Pages Left in table is: " + pages_left_in_table);

            byte[] buffer = new byte[table.getPageSize()];
            while (pages_left_in_table > 0 && (bytesRead = fis.read(buffer)) != -1) {
                byte[] byteArray;
                baos.write(buffer, 0, bytesRead);
                byteArray = baos.toByteArray();
                String bString = DataType.bArrayToBString(byteArray);
                Page page = Page.fromBinaryString(bString, table.getPageSize(), table.getId(), table.getColTypes(), pagesToSkip, oldColTypes);

                // if page is null, it was skipped due to already being in the table
                if (page != null) {
                    // System.out.println("Page was not null: " + page.getId());
                    table.bufferManager.addPageToBuffer(page);
                    table.addPage(page);
                    pages_left_in_table--;
                } 
                // System.out.println("Bytes read in: " + bytesRead);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
        // System.out.println("Pages left: " + pages_left_in_table);
        int amt_pages_after = table.pages.size();

        // System.out.println("pages in table before/after read: " + amt_pages_before +"/"+amt_pages_after);

        // Convert byte array to binary string
        return table;
    }

    /**
     * Reads a table in from a file
     * @param path file path
     * @param pkIndex index of primary key
     * @param colTypes types data structure
     * @param pageSize size of the page in bits
     * @return the entire table as an object
     */
    // public static void read_insert(String path, Object pkValue, Table table) {
    //     byte[] byteArray; 
    //     // Read in file into a byte array
    //     try (FileInputStream fis = new FileInputStream(path+ ".tbl");
    //     ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
    //         byte[] buffer = new byte[table.getPageSize()];
    //         int bytesRead;
    //         int pages_left_in_table = table.getNumPages();
    //         boolean foundPage = false;
    //         while ((bytesRead = fis.read(buffer)) != -1 && !foundPage && pages_left_in_table > 0) {
    //             baos.write(buffer, 0, bytesRead);
    //             byteArray = baos.toByteArray();
    //             String bString = DataType.bArrayToBString(byteArray);
    //             Page page = Page.fromBinaryString(bString, table.getPageSize(), table.getId(), table.getColTypes(), );
    //             table.bufferManager.addPageToBuffer(page);
    //             table.addPage(page);
    //             foundPage = page.insert_pkValue_smaller(table.getpkIndex(), pkValue, table.getColTypes().get(table.getpkIndex()));
    //             pages_left_in_table--;
    //         }
    //     } catch (IOException e) {
    //         e.printStackTrace();
    //     }
    // }

    /**
     * Goes through the buffer manager to find the page, if unsuccessful it then reads the table 
     * from the file and gets the page from that, and pushes it to the buffer manager
     * 
     * @param id
     * @return Page 
     */
    public Page getPage(int id){
        LinkedList<Page> buffered_pages = this.bufferManager.getPagesInBuffer();
        for (Page page : buffered_pages) {
            if (page.getId() == id) {
                return page;
            }
        }
        // If page is not in buffer manager
        int table_id = this.id;
        Page page_matches = new Page(this.pageSize, table_id, id); 

        for (Page page : this.pages) {
            if (page.getId() == id) {
                bufferManager.addPageToBuffer(page);
                page_matches = page;
            }
        }

        return page_matches;
    }

    /**
     * Appends an attribute (column) to the table
     * @param attrib
     * @param defaultVal
     * @return the net number of pages that were added
     */
    public int addAttribute(DataType attrib, Object defaultVal) {
        //System.out.println("Adding Attribute: " + attrib.type.name());
        colTypes.add(attrib);
        ArrayList<Record> recordsToChange = new ArrayList<>();
        int pagesRemoved = 0;
        int pagesAdded = 0;

        // Go through and copy records
        for (int i = 0; i < pages.size(); i++) {
            Page p = pages.get(i);
            
            // copy the records from each page
            for (Record r : p.getRecords()) {
                recordsToChange.add(r);
            }

            pagesRemoved++;
        }

        // DELETE ALL PAGES
        this.pages.clear();

        // Change all of the records and add them
        for (int i = 0; i < recordsToChange.size(); i++) {
            Record r = recordsToChange.get(i);
            
            r.setTypes(colTypes);
            r.appendData(defaultVal, attrib);

            pagesAdded += this.insertRecord(r);
        }
        //System.out.println("pages added: " + pagesAdded + " pages removed: " + pagesRemoved);
        return pagesAdded - pagesRemoved;
    }



    /**
     * drops an attribute (column) from the table
     * @param index
     * @return the net number of pages that were subtracted
     */
    public int dropAttribute(int index) {
        if (index == pkIndex) {
            System.out.println("Error: Drop attribute failed, cannot remove primary key");
            return 0;
        }
        
        DataType droppedType = colTypes.get(index);
        colTypes.remove(index);
        ArrayList<Record> recordsToChange = new ArrayList<>();
        int pagesRemoved = 0;
        int pagesAdded = 0;

        // Go through and copy records
        for (int i = 0; i < pages.size(); i++) {
            Page p = pages.get(i);
            
            // copy the records from each page
            for (Record r : p.getRecords()) {
                recordsToChange.add(r);
            }

            pagesRemoved++;
        }

        // DELETE ALL PAGES
        this.pages.clear();

        // Change all of the records and add them
        for (Record r : recordsToChange) {
            r.removeData(index, droppedType);
            r.setTypes(colTypes);
            
            pagesAdded += this.insertRecord(r);
        }

        return pagesAdded - pagesRemoved;
    }
}
