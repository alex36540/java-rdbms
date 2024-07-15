package src;
import java.util.ArrayList;
import java.util.List;

public class Condition
{
    CompareNode root;
    public Condition() {
        root = null;
    }
    public Condition(List<String> where_list, ArrayList<Integer[]> values_for_condition, Catalog cat) {
        Integer tableObjectIndex = 0;
        int where_list_index = 0;
        // Iterate over where_list until it hits "and" or "or" then you have your first condition
        // Figure it out mate'
        while(where_list_index != where_list.size()) {
            String leftSide = where_list.get(where_list_index);
            where_list_index++;
            if(leftSide.equals("and") || leftSide.equals("or")) {
                CompareNode rightCondition = null;
                String leftSide2 = where_list.get(where_list_index);
                where_list_index++;
                String operator2 = where_list.get(where_list_index);
                where_list_index++;
                String rightSide2 = where_list.get(where_list_index);
                where_list_index++;
                Catalog.Relation relation1 = cat.get_relation(values_for_condition.get(tableObjectIndex)[0]);
                DataType.Type type1 = relation1.colTypes.get(values_for_condition.get(tableObjectIndex)[1]).type; // Double check to make sure that table id then attribute id in values for condition
                // Check if the right side object is constant
                // If so, check if constant is same type as left side object, return null if not, add condition to root under and or or
                if((Character.isDigit(rightSide2.charAt(0)) || rightSide2.contains("\""))) {
                    if(DataType.convertStringToType(type1, rightSide2) == null) {
                        System.out.println("Error: Constant does not match type");
                        this.root = null;
                        return;
                    }
                    rightCondition = new CompareNode(operator2, tableObjectIndex, rightSide2, type1, type1);
                    tableObjectIndex += 1;
                } else {
                    Catalog.Relation relation2 = cat.get_relation(values_for_condition.get(tableObjectIndex+1)[0]);
                    DataType.Type type2 = relation2.colTypes.get(values_for_condition.get(tableObjectIndex+1)[1]).type;
                    rightCondition = new CompareNode(operator2, tableObjectIndex, tableObjectIndex+1, type1, type2);
                    tableObjectIndex += 2;
                } 
                CompareNode joinNode = new CompareNode(leftSide, rightCondition);
                this.root = CompareNode.addNode(this.root, joinNode);
            } else { // is normal condition
                CompareNode normCondition = null;
                String operator = where_list.get(where_list_index);
                where_list_index++;
                String rightSide = where_list.get(where_list_index);
                where_list_index++;
                Catalog.Relation relation1 = cat.get_relation(values_for_condition.get(tableObjectIndex)[0]);
                DataType.Type type1 = relation1.colTypes.get(values_for_condition.get(tableObjectIndex)[1]).type; // Double check to make sure that table id then attribute id in values for condition
                // Check if the right side object is constant
                // If so, check if constant is same type as left side object, return null if not, add condition to root under and or or
                if((Character.isDigit(rightSide.charAt(0)) || rightSide.contains("\""))) {
                    if(DataType.convertStringToType(type1, rightSide) == null) {
                        System.out.println("Error: Constant does not match type after table object: " + tableObjectIndex);
                        this.root = null;
                        return;
                    }
                    normCondition = new CompareNode(operator, tableObjectIndex, rightSide, type1, type1);
                    tableObjectIndex += 1;
                } else {
                    Catalog.Relation relation2 = cat.get_relation(values_for_condition.get(tableObjectIndex+1)[0]);
                    DataType.Type type2 = relation2.colTypes.get(values_for_condition.get(tableObjectIndex+1)[1]).type;
                    if(type1 != type2) {
                        System.out.println("Error: Table Object types do not match: " + type1.name() +" : " + type2.name());
                        this.root = null;
                        return;
                    }
                    normCondition = new CompareNode(operator, tableObjectIndex, tableObjectIndex+1, type1, type2);
                    tableObjectIndex += 2;
                }
                this.root = CompareNode.addNode(this.root, normCondition);
            }
        } 
    }
    public CompareNode getRoot() {
        return this.root;
    }
    
    public static class CompareNode {
        String operator;
        Integer indexObj1;
        Integer indexObj2;
        Object constant; // Only need constant because the constant value is confirmed to be only on the right side
        DataType.Type type1;
        DataType.Type type2;
        CompareNode left;
        CompareNode right;
        //Boolean visited = false;
    
        CompareNode(String op, CompareNode rightChild) {
            this.operator = op;
            this.right = rightChild;
            this.left = null;
        }
        CompareNode(String op, Integer obj1Index, Integer obj2Index, DataType.Type type1, DataType.Type type2) {
            this.operator = op;
            this.indexObj1 = obj1Index;
            this.indexObj2 = obj2Index;
            this.type1 = type1;
            this.type2 = type2;
            this.right = null;
            this.left = null;
        }
        CompareNode(String op, Integer obj1Index, String constant, DataType.Type type1, DataType.Type type2) {
            this.operator = op;
            this.indexObj1 = obj1Index;
            this.indexObj2 = null;
            this.constant = DataType.convertStringToType(type2, constant);
            this.type1 = type1;
            this.type2 = type2;
            this.right = null;
            this.left = null;
        }

        public String getOp() {
            return this.operator;
        }
        public DataType.Type getType1() {
            return this.type1;
        }
        public DataType.Type getType2() {
            return this.type2;
        }
        public Integer getIndex1() {
            return this.indexObj1;
        }
        public Integer getIndex2() {
            return this.indexObj2;
        }
        public Object getConstant() {
            return this.constant;
        }
        // The root node will be set to this every time we add any sort of condition
        // If the root node is not reset to the result after calling this
        // It will cause loss of conditions
        // I'm going to make it so we parse ahead for the right child when making a new node
        // But for each new node, the left child will be null and filled in as we add it
        public static CompareNode addNode(CompareNode rootNode, CompareNode nodeToAdd) {
            if(nodeToAdd.getOp().equals("or")) {
                // This means the root node becomes the left child in every situation possible
                // System.out.println("Adding an OR, making the left child the root");
                nodeToAdd.setLeft(rootNode);
                //System.out.println("Printing current root");
                //nodeToAdd.printNode();
                //System.out.println("Done printing OR");
                return nodeToAdd;
            } else if (nodeToAdd.getOp().equals("and")) {
                // Check if root node is normal condition if so root becomes left child
                // System.out.println("Adding an AND");
                if(!(rootNode.getOp().equals("and")|| rootNode.getOp().equals("or"))) {
                    // System.out.println("Root was normal Condition");
                    nodeToAdd.setLeft(rootNode);
                    return nodeToAdd;
                }
                // Traverse the right side of the root node until encountering a child that
                // is a normal condition
                CompareNode parentNode = rootNode;
                CompareNode currNode = rootNode.right;
                while(currNode != null) { // currNode should never be null if we're traversing the tree
                                        // But worse stuff has happened
                    if(!(currNode.getOp().equals("and") || currNode.getOp().equals("or"))){
                        nodeToAdd.setLeft(currNode);
                        parentNode.setRight(nodeToAdd);
                        //System.out.println("Set AND left child to normal condition and parent node right child to node to add");
                        //parentNode.printNode();
                        //System.out.println("Finished printing parent node");
                        return rootNode; // ENSURE THAT CHANGES IN PARENT NODE REFLECT IN ROOT NODE
                                        // ELSE IT DOES NOT WORK AS INTENDED
                    }
                    parentNode = currNode;
                    currNode = currNode.right;
                }
            } else { // THE node to add is a normal condition 
                if (rootNode == null) {
                    // System.out.println("Is the first condition in a tree");
                    return nodeToAdd;
                }
            }
            return rootNode; // THIS RETURN STATEMENT SHOULD NEVER EVER HAPPEN
        }
        public void setLeft(CompareNode newLeft) {
            this.left = newLeft;
        }
        public void setRight(CompareNode newRight) {
            this.right = newRight;
        }
        public void printNode() {
            if(this.left != null) {
                System.out.println("Recursing into left");
                this.left.printNode();
            } else {
                System.out.println("Index of Object to compare is: " + indexObj1);
            }
            System.out.println("Operator: " + operator);
            if(this.right != null) {
                System.out.println("Recursing into right");
                this.right.printNode();
            } else {
                System.out.println("Index of Object to compare is: " + indexObj2);
            }
        }
    }
    public static void main(String args[])
    {
        // lambda expression to implement above
        // functional interface. This interface
        // by default implements abstractFun()
        ArrayList<Object> objectsToCompare = new ArrayList<>();
        objectsToCompare.add((Object) 5); //0
        objectsToCompare.add((Object) 2); //1
        objectsToCompare.add((Object) 2); //2
        objectsToCompare.add((Object) 3); //3
        objectsToCompare.add((Object) 1); //4
        objectsToCompare.add((Object) 13); //5
        objectsToCompare.add((Object) 12); //6
        objectsToCompare.add((Object) 12); //7
        objectsToCompare.add((Object) 14); //8
        objectsToCompare.add((Object) 15); //9
        //ArrayList<DataType.Type> typesForConversion = new ArrayList<>();
        //ArrayList<String> comparisonOperators = new ArrayList<>();
        // This calls above lambda expression and prints 10.
        //executeConditions(objectsToCompare, typesForConversion, comparisonOperators, null);
        // Testing Condition Tree Formation below
        Condition cond = new Condition();
        CompareNode root = new CompareNode("=", 0, "5", DataType.Type.INTEGER, DataType.Type.INTEGER);
        cond.root = CompareNode.addNode(cond.root, root);
        Boolean eval_result = evaluateCondition(cond.root, objectsToCompare);
        System.out.println(eval_result);
        //root.printNode();
        //System.out.println("ADDING AND");
        CompareNode condition2 = new CompareNode("=", 1, 2, DataType.Type.INTEGER, DataType.Type.INTEGER);
        CompareNode Node1 = new CompareNode("and", condition2);
        cond.root = CompareNode.addNode(cond.root, Node1);
        eval_result = evaluateCondition(cond.root, objectsToCompare);
        System.out.println(eval_result);
        //eval_result = evaluateCondition(root, objectsToCompare);
        //System.out.println(eval_result);
        //root.printNode();
        //System.out.println("ADDING OR");
        /* 
        CompareNode condition3 = new CompareNode("=", 4, 5, DataType.Type.INTEGER, DataType.Type.INTEGER);
        CompareNode Node2 = new CompareNode("and", condition3);
        cond.root = CompareNode.addNode(cond.root, Node2);
        //eval_result = evaluateCondition(root, objectsToCompare);
        //System.out.println(eval_result);
        //root.printNode();
        CompareNode condition4 = new CompareNode("=", 6, 7, DataType.Type.INTEGER, DataType.Type.INTEGER);
        CompareNode Node3 = new CompareNode("or", condition4);
        cond.root = CompareNode.addNode(cond.root, Node3);
        //eval_result = evaluateCondition(root, objectsToCompare);
        //System.out.println(eval_result);
        //root.printNode();
        CompareNode condition5 = new CompareNode("=", 8, "14", DataType.Type.INTEGER, DataType.Type.INTEGER);
        CompareNode Node4 = new CompareNode("and", condition5);
        cond.root = CompareNode.addNode(cond.root, Node4);
        //root.printNode();
        eval_result = evaluateCondition(cond.root, objectsToCompare);
        System.out.println(eval_result);*/
    }

    public static boolean executeConditions(ArrayList<Object> objectsToCompare, ArrayList<DataType.Type> typesForConversion, ArrayList<String> comparisonOperators, ArrayList<Object> constants) {
        
        //boolean result =  condition.run(objectsToCompare, typesForConversion, comparisonOperators);
        //System.out.println(result);
        return true;//result;
    }

    public static boolean evaluateCondition(CompareNode condition, ArrayList<Object> objectsToCompare) {
        if(condition.getOp().equals("and")) {
            Boolean result1 = evaluateCondition(condition.left, objectsToCompare);
            //System.out.println("And Result 1: " + result1);
            Boolean result2 = evaluateCondition(condition.right, objectsToCompare);
            //System.out.println("And Result 2: " + result2);
            return result1 && result2;
        } else if (condition.getOp().equals("or")) {
            Boolean result1 = evaluateCondition(condition.left, objectsToCompare);
            //System.out.println("Or Result 1: " + result1);
            Boolean result2 = evaluateCondition(condition.right, objectsToCompare);
            //System.out.println("Or Result 2: " + result2);
            return result1 || result2;
        }
        // A normal condition
        DataType.Type type1 = condition.getType1();
        DataType.Type type2 = condition.getType2();
        Object obj1 = null;
        if(condition.getIndex1() == null) {
            System.out.println("Big issue, There should be no constant for the left side or the index for obj1 shouldn't be null");
        } else {
            // System.out.println("Index for obj1: " + condition.getIndex1());
            obj1 = objectsToCompare.get(condition.getIndex1());
        }
        Object obj2 = null;
        if(condition.getIndex2() == null) {
            obj2 = condition.getConstant();
            // System.out.println("Constant is: " + obj2);
        } else {
            //System.out.println("Index for obj2: " + condition.getIndex2());
            obj2 = objectsToCompare.get(condition.getIndex2());
        }
        
        if(!type1.name().equals(type2.name())) {
            System.out.println("Error: Types are different");
            return false;
        }
        switch(type1) {
            case INTEGER:
                return DataType.compareInteger(condition.getOp(), obj1, obj2);
            case DOUBLE:
                return DataType.compareDouble(condition.getOp(), obj1, obj2);
            case BOOLEAN:
                return DataType.compareBoolean(condition.getOp(), obj1, obj2);
            case CHAR:
            case VARCHAR:
                return DataType.compareString(condition.getOp(), obj1, obj2);
        }
        System.out.println("UH OH");
        return false; // SHOULD NEVER TOUCH THIS RETURN STATEMENT
    }
}
