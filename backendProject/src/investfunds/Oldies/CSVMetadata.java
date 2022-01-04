/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package investfunds.Oldies;

import java.util.ArrayList;

/**
 *
 * @author rlcancian
 */
class CSVMetadata {

    public int indexOfField(String fieldName) {
        for (int i=0; i<this.CSVFields.size(); i++) {
            if (CSVFields.get(i).getCampo().equals(fieldName)){
                return i;
            }
        }
        return -1;
    }
    
    /**
     * @return the filename
     */
    public String getFilename() {
        return filename;
    }

    /**
     * @param filename the filename to set
     */
    public void setFilename(String filename) {
        this.filename = filename;
    }

    /**
     * @return the path
     */
    public String getPath() {
        return path;
    }

    /**
     * @param path the path to set
     */
    public void setPath(String path) {
        this.path = path;
    }

    /**
     * @return the CSVFields
     */
    public ArrayList<CSVField> getCSVFields() {
        return CSVFields;
    }

    /**
     * @param CSVFields the CSVFields to set
     */
    public void setCSVFields(ArrayList<CSVField> CSVFields) {
        this.CSVFields = CSVFields;
    }
        
    private String filename;
    private String path;
    private ArrayList<CSVField> CSVFields = new ArrayList<>();
    
}
