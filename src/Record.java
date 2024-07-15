/*
 * File: Record.java
 * 
 * Desc: The class that represents a single record
 * 
 * Authors: Alex Lee        al3774@rit.edu
 *          Merone Delnesa  mtd6620@rit.edu
 *          Isaac Mixon     igm3923@rit.edu
 *          Ethan Nunez     ern1274@rit.edu
 */
package src;

import java.util.ArrayList;
import java.util.BitSet;


public class Record
{
    private ArrayList<DataType> types; // 
    private ArrayList<Object> data;

    private BitSet nullMap; // Bitmap that tells which data elements are null
    private int numCols; // How many columns
    private int size = 0; // Size in bits


    /**
     * Constructor for making a record with data
     * @param parentTable
     * @param data
     */
    public Record(ArrayList<DataType> colTypes, ArrayList<Object> data) {
        this.types = colTypes;
        this.data = data;
        this.numCols = types.size();
                this.nullMap = new BitSet(numCols);
        this.size += DataType.INTEGER_SIZE;
        this.size += numCols; // Add null bitmap to record size
        

        // Setting nullMap to keep track of null values
        // Also keeping track of size
        for (int i = 0; i < numCols; i++) {
            Object currentVal = data.get(i);
            // Check for null values
            if (currentVal == null) {
                nullMap.set(i, true);
            }
            else {
                nullMap.set(i, false);

                // look at type and determine size
                DataType t = types.get(i);
                

                // Update size based on data type 
                this.size += getDataSize(currentVal, t);

            }
        }
    }

    /**
     * Returns list of data of the Record
     * @return list of data
     */
    public ArrayList<Object> getData() {
        return data;
    }

    /**
     * Adds to the size based on the datatype. Varchar must be counted specially
     * @param t
     */
    public int getDataSize(Object val, DataType t) {
        int s = 0;

        if (t.type == DataType.Type.VARCHAR) {
            s += DataType.INTEGER_SIZE;
            s += ((String) val).length() * 8;
        }
        else {
            s += t.getSize();
        }

        return s;
    }

    /**
     * Returns size of Record
     * @return size
     */
    public int getSize() {
        return size;
    }

    /**
     * Converts the record into a BitSet
     * @return the BitSet
     */
    public String toBinaryString() {
        String bString = "";
        bString += DataType.toBinaryString((Integer) numCols, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
       
        String typestring = "";
        for (DataType type: types) {
            typestring += type.type.name() + " ";
        }
        //System.out.println("This record has num cols: " + numCols + " num of data: " + data.size() + " types: " + typestring);


        // add nullmap
        for (int i = 0; i < numCols; i++) {
            bString += nullMap.get(i) ? "1" : "0";
        }

        // iterate through data and manipulate bits
        for (int i = 0; i < data.size(); i++) {
            // Skip if null
            if (nullMap.get(i)) {
                continue;
            }

            // Get information about data
            DataType currentType = types.get(i);
            Object currentData = data.get(i);
            int dataSize = currentType.getSize();
            if (currentData != null) {
                bString += DataType.toBinaryString(currentData, currentType.type, dataSize);
            }
        }
        
        return bString;
    }


    public static Record fromBinaryString(String bString, int offset, ArrayList<DataType> types, ArrayList<DataType> oldColTypes) {
        // Init variables
        ArrayList<Object> data = new ArrayList<>();
        //int numCols = types.size();

        String numColStr = bString.substring(offset, offset + DataType.INTEGER_SIZE);
        int numCols = (Integer) DataType.fromBinaryString(numColStr, DataType.Type.INTEGER, DataType.INTEGER_SIZE);
        offset += DataType.INTEGER_SIZE;
        //System.out.println("Old Col Types: " + oldColTypes+" Num of Cols: " + numCols);
        // If numCols don't match the size of the types, it is a old record aka unaltered record
        if (oldColTypes != null || numCols != types.size()) {
            //System.out.println("Old Col Types: " + oldColTypes+" Num of Cols: " + numCols);
            types = oldColTypes;
        }
        // Read in the nullmap
        BitSet nullMap = new BitSet(numCols);

        for (int i = 0; i < numCols; i++) {
            nullMap.set(i, bString.charAt(offset + i) == '0' ? false : true);
        }

        offset += numCols;

// Iterate through data and insert data
        for (int i = 0; i < numCols; i++) {
            // Check if null
            if (nullMap.get(i)) {
                data.add(null);
                continue;
            }

            // Get size
            //System.out.println("offset: " + offset + " numCols: " + numCols + " i: " + i);
            DataType currentType = types.get(i);
            int dataSize = currentType.getSize();
            Object itemToAdd = new Object();
            String dataString = "";

            // Get data and incr offset
            if (currentType.type != DataType.Type.VARCHAR) {
                // Iterate over the bitset and create a binary string 
                dataString = bString.substring(offset, offset + dataSize);

                itemToAdd = DataType.fromBinaryString(dataString, currentType.type, dataSize);
                offset += dataSize;
            }
            else {
                // Iterate over the integer that determines varchar size first
                String intString = bString.substring(offset, offset + DataType.INTEGER_SIZE);
                Integer strLen = ((Integer) DataType.fromBinaryString(intString, DataType.Type.INTEGER, DataType.INTEGER_SIZE)) * 8;
                offset += DataType.INTEGER_SIZE;
                //System.out.println("Length of varchar is " + strLen);

                // Then use length to get the varchar bits
                String varCharString = bString.substring(offset, offset + strLen);
                itemToAdd = DataType.fromBinaryString(varCharString, DataType.Type.CHAR, strLen);
                offset += strLen;
            }
            data.add(itemToAdd);
        }


        return new Record(types, data);
    }

    public void appendData(Object val, DataType type) {
        this.data.add(val);
        if (val == null) {
            //System.out.println("Setting false: " + (this.numCols-1));
            nullMap.set(this.numCols-1,true);
            return;
        }
        nullMap.set(this.numCols-1,false);
        this.size += getDataSize(val, type);
    }

    public void removeData(int index, DataType t) {
        this.size -= getDataSize(data.get(index), t);
        this.data.remove(index);
    }

    public void setTypes(ArrayList<DataType> newTypes) {
        this.types =  newTypes;
        this.numCols = newTypes.size();
        this.nullMap = new BitSet(numCols);
        // BELOW ASSUMES YOU HAVENT ADDED THE VALUE YET SO num col - 1
        for (int i = 0; i < numCols-1; i++) {
            Object currentVal = data.get(i);
            if (currentVal == null) {
                nullMap.set(i, true);
            }
            else {
                nullMap.set(i, false);
            }
        }
    }

}
