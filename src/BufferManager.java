/*
 * File: BufferManager.java
 * 
 * Desc: The buffer manager will manage pages in memory
 * 
 * Authors: Alex Lee        al3774@rit.edu
 *          Merone Delnesa  mtd6620@rit.edu
 *          Isaac Mixon     igm3923@rit.edu
 *          Ethan Nunez     ern1274@rit.edu
 */
package src;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

public class BufferManager {
    private int buffer_size;
    Queue<Page> pages = new LinkedList<Page>();
    ArrayList<ArrayList<Integer>> rewrite = new ArrayList<>();
    Catalog cat; 

    /**
     * Constructor for Buffer Manager
     */
    public BufferManager(Catalog cat) {
        this.cat = cat;
        this.buffer_size = cat.get_buffer_size();
    }

    public LinkedList<Page> getPagesInBuffer() {
        return new LinkedList<>(this.pages);
    }
    public String getDbLoc() {
        return cat.get_catalog_path();
    }
    public void printPages() {
        String str = "";
        for (Page p : this.pages) {
            str += "("+ p.getTblId()+" , " +p.getId()+"), ";
        }
        System.out.println(str);
    }
    
    /**
     * Adds a page to the buffer
     * @param p page given to buffer manager to add to buffer
     */
    public void addPageToBuffer(Page p) {
        //printPages();
        if(pages.remove(p)) {
            pages.add(p);
            return;
        }
        if (pages.size() == buffer_size ) {
            // least recently used 
            // LRU will be the page at the front of the queue, we will update buffer as pages are used
            //      in order to keep track of LRU. if page used, it will be added to the end of the 
            //      pages list. 
            boolean should_rewrite = should_rewrite(pages.element());
            if (should_rewrite) {
                writePageToHardware(pages.poll());
            } else {
                pages.poll();
            }
        }
        // if page already in buffer, move it to the end (update or add)
        pages.add(p);
    }
    public boolean should_rewrite(Page p) {
        // System.out.println("Checking page: " + p.getTblId() +" : " + p.getId() + " rewrite");
        boolean removed = rewrite.removeIf(page_rewrites -> p.getTblId() == page_rewrites.get(0) && p.getId() == page_rewrites.get(1));
        return removed;
    }
    public void pushChange(Page p) {
        for (ArrayList<Integer> change : rewrite) {
            if(change.get(0) == p.getTblId() && change.get(1) == p.getId()) {
                return ;
            }
        }
        rewrite.add(new ArrayList<>(Arrays.asList(p.getTblId(), p.getId())));
    }
    public LinkedList<Page> populateTable(int tblId) {
        LinkedList<Page> table_pages = new LinkedList<>();
        String pagestring = "";
        for (Page page : pages) {
            if(page.getTblId() == tblId) {
                pagestring += page.getId() + " , ";
                table_pages.add(page);
            }
        } 
        // System.out.println("Populating: " + pagestring);
        return table_pages;
    }

    /**
     * Writes the LRU page to hardware 
     * @param p LRU page
     */
    public void writePageToHardware(Page p) {
        int table_id = p.getTblId();
        String table_path = cat.get_db_loc() + "tables" + File.separator + table_id;
        Catalog.Relation relation = cat.get_relation(table_id);
        if (relation != null) {
            Table table = new Table(table_id, relation.get_pkIndex(), relation.get_colTypes(), cat.get_page_size(), this, relation.get_num_of_pages());
            table.addPage(p);
            refactor_rewrites(table);
            //System.out.println("Table id: " + table_id + " Page id: " + p.getId());
            Table.read(table_path, table, relation.getOldTypes());
            print_buffer();
            
            
            //table.addPage(p); // TODO addPages skips adding a page in the table if it's already there, should this be removed?
            table.write(table_path);
            //pages.remove(p);
        }
    }
    public void deletePageFromHardWare(Page p) {
        int table_id = p.getTblId();
        String table_path = cat.get_db_loc() + "tables" + File.separator + table_id;
        Catalog.Relation relation = cat.get_relation(table_id);
        Table table = new Table(table_id, relation.get_pkIndex(), relation.get_colTypes(), cat.get_page_size(), this, relation.get_num_of_pages());
        //refactor_rewrites(table); #TODO this may or may not have any effect, we have yet to test this
        HashSet<Integer> pagesToSkip = table.getPageIdSet();

        try (FileInputStream fis = new FileInputStream(table_path+ ".tbl");
        ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            byte[] buffer = new byte[cat.get_page_size()];
            int bytesRead;
            int pages_left_in_table = relation.get_num_of_pages();
            while ((bytesRead = fis.read(buffer)) != -1 || pages_left_in_table > 1) {
                byte[] byteArray;
                baos.write(buffer, 0, bytesRead);
                byteArray = baos.toByteArray();
                String bString = DataType.bArrayToBString(byteArray);
                Page page = Page.fromBinaryString(bString, cat.get_page_size(), table_id, relation.get_colTypes(), pagesToSkip, relation.getOldTypes()); 
                
                // check if page was skipped
                if (page != null) {
                    if(!(page.getTblId() == table_id && page.getId() == p.getId())) {
                        table.addPage(page);
                    }
                    addPageToBuffer(page);
                }
                pages_left_in_table--;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        table.write(table_path);
    }

    public void refactor_rewrites(Table table) {
        //System.out.println("Checking rewrites");
        for (Page page: table.getPages()) {
            boolean removed = rewrite.removeIf(page_rewrites -> page.getTblId() == page_rewrites.get(0) && page.getId() == page_rewrites.get(1));
            if (removed) {
                System.out.println("Altered Page was removed from rewrites during writePagetoHardware: " + page.getTblId() + " : " + page.getId());
            }
        }
    }

    public void print_buffer() {
        String pagestring = "";
        String rewritestring = "";
        for (Page page : pages) {
            pagestring += page.getTblId() + " : " + page.getId() + " ,";
        }
        for (ArrayList<Integer> changes: rewrite) {
            rewritestring += changes.get(0) + " : " + changes.get(1) + ", ";
        }
        //System.out.println("Buffer Size: " + buffer_size + " Pages: " + pagestring + " rewrite: " + rewritestring);
    }

    public void shutdown() {
        for (Page page : pages) {
            if(should_rewrite(page)) {
                writePageToHardware(page);
            }
        }
        pages.clear();
    }

}
