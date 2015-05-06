package NaiveBayes;

import java.util.ArrayList;

import NaiveBayes.DataSet;
import NaiveBayes.Evaluation;

public class test {

    public static void main(String[] args) {
    	
        //String[] dataPaths = new String[]{"breast-cancer.data", "segment.data"};
        String[] dataPaths = new String[]{"segment1.data"};
        for (String path : dataPaths) {
        	//ArrayList<ArrayList<Double>> features = new ArrayList<ArrayList<Double>>();
        	//<Double> labels = new ArrayList<Double>();
            DataSet dataset = new DataSet(path);
            
            
            //blabla
            

            Evaluation eva = new Evaluation(dataset, "NaiveBayes");
            
            dataset.dataReset(dataset.features, dataset.labels);

            eva.crossValidation(1);
            
            //double[] testonly = {202.0,41.0,9.0,0.0,0.0,0.944448,0.772202,1.11111,1.0256,123.037,111.889,139.778,117.444,-33.4444,50.2222,-16.7778,139.778,0.199347,-2.29992};
            
           double[] testonly = {1,1};
           double prediction = eva.predict(testonly);
           System.out.println("Print Results: "+prediction);
            
            // print mean and standard deviation of accuracy
            System.out.println("Dataset:" + path + ", mean and standard deviation of accuracy:" + eva.getAccMean() + "," + eva.getAccStd());
        }
    }
}
