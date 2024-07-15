package src;


public class DataType {
    // Enum for data types
    public enum Type {
        INTEGER,
        DOUBLE,
        BOOLEAN,
        CHAR,
        VARCHAR;
    }

    // Sizes of data in bits (except varchar)
    public static final int INTEGER_SIZE = 32;
    public static final int DOUBLE_SIZE = 64;
    public static final int BOOLEAN_SIZE = 1;

    public final Type type;
    public final int size;

    /**
     * Constructor
     * @param type
     * @param size
     */
    public DataType(Type type, int size) {
        this.type = type;
        int expectedSize = 0;

        switch (type) {
            case INTEGER:
                expectedSize = INTEGER_SIZE;
                this.size = size;
                break;
            case DOUBLE:
                expectedSize = DOUBLE_SIZE;
                this.size = size;
                break;
            case BOOLEAN:
                expectedSize = BOOLEAN_SIZE;
                this.size = size;
                break;
            case CHAR:
            case VARCHAR:
                this.size = size * 8;
                expectedSize = size;
                break;
            default:
                this.size = size;
                expectedSize = size;
                break;
        }

        if (expectedSize != size) {
            throw new IllegalArgumentException("Size of data does not match expected size of: " + expectedSize);
        }
    }

    /**
     * Returns size of data type 
     * @return size
     */
    public int getSize() {
        return size;
    }

    /**
     * Turns data into a binary string, provided it is of one of the types provided in the Type enum
     * @param data
     * @return binary string
     */
    public static String toBinaryString(Object data, Type t, int sizeInBits) {
        String bString = "";

        // create binary string based on type
        switch (t) {
            case INTEGER:
                Integer convertedInt = (Integer) data;

                // Convert to binary string
                bString = Integer.toBinaryString(convertedInt);

                break;
            case DOUBLE:
                Double convertedDbl = (Double) data;
                long dblBits = Double.doubleToLongBits(convertedDbl.doubleValue());

                bString = Long.toBinaryString(dblBits);

                break;
            case BOOLEAN:
                Boolean convertedBool = (Boolean) data;
                bString = convertedBool.booleanValue() ? "1" : "0";
                break;
            case CHAR:
                String convertedStr = (String) data;
                byte[] bytes = convertedStr.getBytes();

                // Iterate through the bytes and add to string
                for (byte b : bytes) {
                    int val = b;
                    for (int j = 0; j < 8; j++) {
                        bString += (val & 128) == 0 ? "0" : "1";
                        val <<= 1;
                    }
                }
                break;
            case VARCHAR:
                String convertedVarStr = (String) data;
                Integer strLen = Integer.valueOf(convertedVarStr.length());
                byte[] strBytes = convertedVarStr.getBytes();

                // First add length of string
                bString = toBinaryString(strLen, Type.INTEGER, INTEGER_SIZE);

                // Iterate through the bytes and add varchar to string
                for (byte b : strBytes) {
                    int val = b;
                    for (int j = 0; j < 8; j++) {
                        bString += (val & 128) == 0 ? "0" : "1";
                        val <<= 1;
                    }
                }

                //System.out.println("VARCHAR BSTRING: " + bString);
                break;
        }

        // Pad the binary string with leading zeros if not a varchar
        if (t != Type.VARCHAR) {
            if (bString.length() < sizeInBits) {
                bString = String.format("%" + sizeInBits + "s", bString).replace(' ', '0');
            }
        }

        return bString;
    }
     
    /**
     *  Turns binary string back into data
     * @param bString
     * @param t
     * @param size
     * @return object
     */
    public static Object fromBinaryString(String bString, Type t, int size) {
        // Create data based on type
        switch (t) {
            case INTEGER:
                // Create and add integer to data
                Integer convertedInt = Integer.parseUnsignedInt(bString, 2);

                //System.out.println("Added int: " + convertedInt);
                return convertedInt;
            case DOUBLE:
                // Double needs an extra step
                long l = Long.parseUnsignedLong(bString, 2);
                Double convertedDbl = Double.longBitsToDouble(l);

                //System.out.println("Added double: " + convertedDbl);
                return convertedDbl;
            case BOOLEAN:
                boolean convertedBool = bString.equals("1") ? true : false;

                //System.out.println("Added bool: " + convertedBool);
                return convertedBool;
            case CHAR:
                StringBuilder convertedStr = new StringBuilder();

                // Get string character by character
                int charIndex = 0;
                while (charIndex < bString.length()) {
                    String substring = bString.substring(charIndex, Math.min(charIndex + 8, bString.length()));
                    int charCode = Integer.parseInt(substring, 2);
                    convertedStr.append((char) charCode);
                    charIndex += 8;
                }

                // System.out.println("Added char(x): " + convertedStr.toString());
                return convertedStr.toString();
            case VARCHAR:
                /*
                *  Will be unimplemented because to read a varchar from binary, just need to read 
                *  the integer that determines length, then the string that follows using CHAR
                */
                return null;
        }
        return null;
    }

    /**
     * Converts given binary string into a byte array 
     * @param bString
     * @return byte array  
     */
    public static byte[] bStringToBArray(String bString) {
        int remainder = bString.length() % 8;

        // Pad the binary string with zeros if necessary
        if (remainder != 0) {
        bString += "0".repeat(8 - remainder);
        }

        // Convert binary string to byte array
        byte[] byteArray = new byte[bString.length() / 8];
        for (int i = 0; i < byteArray.length; i++) {
            String byteString = bString.substring(i * 8, (i + 1) * 8);
            byteArray[i] = (byte) Integer.parseInt(byteString, 2);
        }

        return byteArray;
    }

    /**
     * Converts given byte array into a binary string 
     * @param byteArray
     * @return binary string 
     */
    public static String bArrayToBString(byte[] byteArray) {
        StringBuilder binaryStringBuilder = new StringBuilder();
        for (byte b : byteArray) {
            String binaryString = String.format("%8s", Integer.toBinaryString(b & 0xFF)).replace(' ', '0');
            binaryStringBuilder.append(binaryString);
        }

        return  binaryStringBuilder.toString();
    }
    /**
     * Converts a string that is the lowercase form of DataType.Types enum
     * return respective DataType.Type
     * @param String string_type
     * @return Object[] array with 2 values, DataType.Type and DataType.size
     */
    public static Object[] convertStringToType(String string_type) {
        string_type = string_type.toLowerCase();
        switch(string_type) {
            case "integer":
            case "int":
                return new Object[]{DataType.Type.INTEGER, INTEGER_SIZE};
            case "double":
                return new Object[]{DataType.Type.DOUBLE, DOUBLE_SIZE};
            case "boolean":
                return new Object[]{DataType.Type.BOOLEAN, BOOLEAN_SIZE};
            case "char":
                return new Object[]{DataType.Type.CHAR, 0};
            case "varchar":
                return new Object[]{DataType.Type.VARCHAR, 0};
        }
            return null;
        //throw new IllegalArgumentException("string_type needs to be one of the following: integer, double, boolean, char or varchar");
    }
    /**
     * Converts a value that is the of the DataType.Type type
     * return respective converted value
     * @param Type type DataType
     * @param String value, a string representation of the value
     * @return Object, the converted value passed back with Object Wrapper
     */
    public static Object convertStringToType(Type type, String value) {
        switch (type) {
            case INTEGER:
                try {
                    int d = Integer.parseInt(value);
                    return d;
                } catch (NumberFormatException | NullPointerException e) {
                    System.out.println("Error: Failed to process int");
                    return null;
                    //throw new NumberFormatException("Value can't be parsed as integer: " + value);
                }
            case DOUBLE:
                try {
                    Double d = Double.parseDouble(value);
                    return d;
                } catch (NumberFormatException | NullPointerException e) {
                    System.out.println("Error: Failed to process double");
                    return null;
                    //throw new NumberFormatException("Value can't be parsed as double: " + value);
                }
            case BOOLEAN:
                return value.equals("true");
            case CHAR:
            case VARCHAR:
                if(value.contains("\"")) {
                    return value;
                }
        }
        return null;
        //throw new IllegalArgumentException("Type needs to be one of the following: INTEGER, DOUBLE, BOOLEAN, CHAR or VARCHAR");
    }

    public static boolean isFirstBiggerThanSecond(Object first, Object second, Type type) {
        switch (type) {
            case INTEGER:
                try {
                    
                    return (Integer) first > (Integer) second;
                } catch (NumberFormatException e) {
                    System.out.println("Error: Failed to process int");
                    return false;
                    //throw new NumberFormatException("Value can't be parsed as integer: " + value);
                }
            case DOUBLE:
                try {
                    
                    return (Double) first > (Double) second;
                } catch (NumberFormatException e) {
                    System.out.println("Error: Failed to process double");
                    return false;
                    //throw new NumberFormatException("Value can't be parsed as double: " + value);
                }
            case BOOLEAN:
                return (Boolean) first && (Boolean) second;
            case CHAR:
            case VARCHAR:
                int result = ((String) first).compareTo((String) second);
                if (result > 0) {
                    return true;
                }
                return false;
        }
        return false;
    }
    public static Boolean compareInteger(String operator, Object first, Object second) {
        try {
            switch(operator) {
                case "=":
                    return (Integer) first == (Integer) second;
                case "!=":
                    return (Integer) first != (Integer) second;
                case ">":
                    return (Integer) first > (Integer) second;
                case ">=":
                    return (Integer) first >= (Integer) second;
                case "<":
                    return (Integer) first < (Integer) second;
                case "<=":
                    return (Integer) first <= (Integer) second;
            } 
        } catch (NumberFormatException e) {
            System.out.println("Error: Failed to process int");
            return false;
        }
        return false;
    }
    public static Boolean compareDouble(String operator, Object first, Object second) {
        try {
            switch(operator) {
                case "=":
                    return Double.compare((Double)first,(Double)second) == 0;
                case "!=":
                    return Double.compare((Double)first,(Double)second) != 0;
                case ">":
                    // if (Double.compare((Double)first,(Double)second) > 0) {
                    //     return true;
                    //}
                    return (Double) first > (Double) second;
                case ">=":
                    return Double.compare((Double)first,(Double)second) >= 0;
                case "<":
                    // if (Double.compare((Double)first,(Double)second) < 0) {
                    //     return true;
                    // }
                    return (Double) first < (Double) second;
                case "<=":
                    return Double.compare((Double)first,(Double)second) <= 0;
            } 
        } catch (NumberFormatException e) {
            System.out.println("Error: Failed to process Double");
            return false;
        }
        return false;
    }
    public static Boolean compareBoolean(String operator, Object first, Object second) {
        try {
            switch(operator) {
                case "and":
                    return (Boolean) first && (Boolean) second;
                case "or":
                    return (Boolean) first || (Boolean) second;
            } 
        } catch (NumberFormatException e) {
            System.out.println("Error: Failed to process Boolean");
            return false;
        }
        return false;
    }
    public static Boolean compareString(String operator, Object first, Object second) {
        int result = ((String) first).compareTo((String) second);
        switch(operator) {
            case "=":
                return result == 0;
            case "!=":
                return result != 0;
            case ">":
                return result > 0;
            case ">=":
                return result >= 0;
            case "<":
                return result < 0;
            case "<=":
                return result <= 0;
        }
        return false;
    }

    public static Boolean compare(String operator, DataType t, Object first, Object second) {
        switch(t.type) {
            case INTEGER:
                return DataType.compareInteger(operator, first, second);
            case DOUBLE:
                return DataType.compareDouble(operator, first, second);
            case BOOLEAN:
                return DataType.compareBoolean(operator, first, second);
            case CHAR:
            case VARCHAR:
                return DataType.compareString(operator, first, second);
            default:
                return false;
        }

    }
}
