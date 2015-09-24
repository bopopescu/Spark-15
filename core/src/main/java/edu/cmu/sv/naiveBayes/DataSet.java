package edu.cmu.sv.naiveBayes;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;



public class DataSet {

    private boolean[] isCategory;
    public ArrayList<ArrayList<Double>> features;
    public ArrayList<Double> labels;
    private int numAttributes;
    private int numInstnaces;

    public  void dataReset(ArrayList<ArrayList<Double>> trainStructure, ArrayList<Double> Label) {
        
            
            numAttributes = trainStructure.get(1).size();
            isCategory = new boolean[numAttributes + 1];
            for (int i = 0; i < isCategory.length; i++) {
                //isCategory[i] = Integer.parseInt(attInfo[i]) == 1 ? true : false;
            	isCategory[i] = true;
            }
            numInstnaces = Label.size();
            
            features = new ArrayList<ArrayList<Double>>();
            labels = new ArrayList<Double>();
            System.out.println("reading " + numInstnaces + " exmaples with " + numAttributes + " attributes");

            features = trainStructure;
            labels = Label;           
            
        
    }
    
    
    public DataSet(String path) {
        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String[] attInfo = reader.readLine().split(","); // attributes info
            numAttributes = attInfo.length - 1;
            isCategory = new boolean[numAttributes + 1];
            for (int i = 0; i < isCategory.length; i++) {
                isCategory[i] = Integer.parseInt(attInfo[i]) == 1 ? true : false;
            	//isCategory[i] = true;
            }
            numInstnaces = 0;
            
            features = new ArrayList<ArrayList<Double>>();
            labels = new ArrayList<Double>();
            System.out.println("reading " + numInstnaces + " exmaples with " + numAttributes + " attributes");

            String line;
            while ((line = reader.readLine()) != null) {
                String[] atts = line.split(",");
                ArrayList<Double> temp = new ArrayList<Double>();
                for (int i = 0; i < atts.length - 1; i++) {
                    temp.add(Double.parseDouble(atts[i]));
                }
                features.add(temp);
                labels.add(Double.parseDouble(atts[atts.length - 1]));
                numInstnaces ++;
            }
            reader.close();
        } catch (FileNotFoundException ex) {
            Logger.getLogger(DataSet.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
            Logger.getLogger(DataSet.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public boolean[] getIsCategory() {
        return isCategory;
    }

    public double[][] getFeatures() {
        double[][] array = new double[features.size()][];
        for(int i = 0;i < features.size();i ++) {
            ArrayList<Double> row = features.get(i);
            array[i] = new double[row.size()];
            for(int j = 0;j < row.size();j ++) {
            	array[i][j] = row.get(j);
            }
        }
        return array;
    }

    public double[] getLabels() {
    	double[] array = new double[labels.size()];
    	for(int i = 0;i < labels.size();i ++) {
    		array[i] = labels.get(i);
    	}
        return array;
    }

    public int getNumAttributes() {
        return numAttributes;
    }

    public int getNumInstnaces() {
        return numInstnaces;
    }
    
    public void addNewData(double[] feature, double prob) {
    	ArrayList<Double> newFeatures = new ArrayList<Double>();
    	for(int i = 0;i < feature.length;i ++) {
    		newFeatures.add(feature[i]);
    	}
        this.features.add(newFeatures);
        labels.add(prob);
        numInstnaces ++;
    }
}
