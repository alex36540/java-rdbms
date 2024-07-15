/*
 * File: Page.java
 * 
 * Desc: The class that represents a page
 * 
 * Authors: Alex Lee        al3774@rit.edu
 *          Merone Delnesa  mtd6620@rit.edu
 *          Isaac Mixon     igm3923@rit.edu
 *          Ethan Nunez     ern1274@rit.edu
 */
package src;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.HashSet;
import java.util.Iterator;

public class Page 
{
    private int size; // max size in BITS
    private int numRecords;
    private ArrayList<Record> records;
    private int freePtr; // points to the end of free space in the page
    private int tableId;
    private int id;

    /**
     * Default Page constructor
     * @param size
     */
    public Page(int size, int tableId, int id) {
        this.numRecords = 0;
        this.records = new ArrayList<>();
        this.size = size;
        this.freePtr = DataType.INTEGER_SIZE * 3; // Initially has 3 integers storing # of records and this pointer to free space
        this.tableId = tableId;
        this.id = id;
    }

    @Override
        public boolean equals(Object o) {
            boolean same = false;
            if (o != null && o instanceof Page){
                same = this.id == ((Page) o).id && this.tableId == ((Page)o).tableId;
            }
            return same;
        }


    /**
     * Parameterized Page constructor
     * @param size
     * @param records
     * @param freePtr
     * @param parentTable
     */
    public Page(int size, ArrayList<Record> records, int freePtr, int tableId, int id) {
        this.size = size;
        this.numRecords = records.size();
        this.records = records;
        this.freePtr = freePtr;
        this.tableId = tableId;
        this.id = id;
    }

    public int indexToAdd(Record r, int pkIndex, ArrayList<DataType> colTypes) {
        ArrayList<Object> dataToInsert = r.getData();
        boolean goNext = false;

        for (int i = 0; i < records.size(); i++) {
            Object currentData = records.get(i).getData().get(pkIndex);
            Object toInsertPk = dataToInsert.get(pkIndex);
            DataType pkType = colTypes.get(pkIndex);
            
            

            // compare records based on pk
            switch (pkType.type) {
                case INTEGER:
                    goNext = (Integer) toInsertPk > (Integer) currentData;
                    break;
                case DOUBLE:
                    goNext = (Double) toInsertPk > (Double) currentData;
                    break;
                case BOOLEAN:
                    goNext = !(Boolean) currentData;
                    break;
                case CHAR:
                case VARCHAR:
                    goNext = ((String) toInsertPk).compareTo((String) currentData) > 0 ? true : false;
                    break;
                default:
                    break;
            }

            // check if you have reached the end
            if (i == records.size() - 1) {
                if (goNext) {
                    return i + 1;
                }
                else return i;
            }
            else if (!goNext) {
                return i;
            }
        }

        return 0;
    }


    public void insertRecord(Record r, int index) {
        this.records.add(r);
        numRecords++;
        freePtr += r.getSize();
    }

    public boolean insert_pkValue_smaller(int pkIndex, Object pkValue, DataType pkType) {
        Record first_record = records.get(0);
        return DataType.isFirstBiggerThanSecond(first_record.getData().get(0), pkValue, pkType.type);
    }

    /**
     * Converts this page into a binary string
     * @return the string
     */
    public String toBinaryString() {
        String bString = "";

        /*
         * Header has ID # of records, and ptr to free space
         */
        //System.out.println("Saving page id: " + id);
        bString += DataType.toBinaryString((Integer) id, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
        bString += DataType.toBinaryString((Integer) numRecords, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
        bString += DataType.toBinaryString((Integer) freePtr, DataType.Type.INTEGER, DataType.INTEGER_SIZE);

        /*
         * Add each record
         */
        // System.out.println("Page: " + tableId + " : " + id);
        for (Record r : records) {
            bString += r.toBinaryString();
        }

        // Pad out the string to the size of the page
        if (bString.length() >= size) {
            return bString;
        } 
        else {
            String format = "%-" + size + "s";
            String paddedString = String.format(format, bString).replace(' ', '0');
            return paddedString;
        }
    }

    /**
     * Returns a page from a valid binary string
     * @param bString
     * @param size page size in bits
     * @param parentTable
     * @param skipSet set of page IDs to skip reading because they are already in the table
     * @return the page
     */
    public static Page fromBinaryString(String bString, int size, int tableId, ArrayList<DataType> types, HashSet<Integer> toSkip, ArrayList<DataType> oldColTypes) {
        ArrayList<Record> records = new ArrayList<>();
        int offset = 0;

        // get id, numRecords and freePtr
        String idStr = bString.substring(offset, offset + DataType.INTEGER_SIZE);
        int id = (Integer) DataType.fromBinaryString(idStr, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
        offset += DataType.INTEGER_SIZE;
        // System.out.println("Read in page id: " + id);

        if (toSkip != null && toSkip.contains(id)) {
            return null;
        }

        String numRecordStr = bString.substring(offset, offset + DataType.INTEGER_SIZE);
        int numRecords = (Integer) DataType.fromBinaryString(numRecordStr, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
        offset += DataType.INTEGER_SIZE;

        String freePtrStr = bString.substring(offset, offset + DataType.INTEGER_SIZE);
        int freePtr = (Integer) DataType.fromBinaryString(freePtrStr, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
        offset += DataType.INTEGER_SIZE;

        // Get records now
        for (int i = 0; i < numRecords; i++) {
            Record r = Record.fromBinaryString(bString, offset, types, oldColTypes);
            records.add(r);
            offset += r.getSize();
        }

        return new Page(size, records, freePtr, tableId, id);
    }

    /**
     * Returns list of records from Page 
     * @return list of records
     */
    public ArrayList<Record> getRecords() {
        return records;
    }

    public int getFreePtr() {
        return freePtr;
    }

    public int getNumRecords () {
        return numRecords;
    }

    public int getMaxSize() {
        return size;
    }

    /**
     * Removes a record from a page
     * @param pk the value of the primary key of the record to delete
     * @param pkIndex the column index of the primary key
     * @return if a record was sucessfully deleted from this page (if it existed in the first place)
     */
    public boolean deleteRecord(Object pk, int pkIndex) {
        for (Iterator<Record> it = records.iterator(); it.hasNext();) {
            Record r = it.next();
            if (r.getData().get(pkIndex) == pk) {
                it.remove();
                freePtr -= r.getSize();
                numRecords--;
                return true;
            }
        }

        return false;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public int getTblId() {
        return tableId;
    }

    public LinkedList<Page> split() {
        LinkedList<Page> newPages = new LinkedList<>();
        Page p1 = new Page(this.size, this.tableId, this.id);
        Page p2 = new Page(this.size, this.tableId, 0); // ID will be set correctly by the table
        

        // Split records between the two pages
        for (int i = 0; i < numRecords; i++) {
            if (i <= numRecords / 2) {
                p1.insertRecord(this.records.get(i), i);
            }
            else {
                p2.insertRecord(this.records.get(i), (i - 1) / 2);
            }
        }

        newPages.add(p1);
        newPages.add(p2);
        return newPages;
    }

}
