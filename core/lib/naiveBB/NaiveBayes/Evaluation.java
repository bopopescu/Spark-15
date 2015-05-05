package NaiveBayes;

import java.util.Arrays;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Evaluation {

    private String clsName;
    private DataSet dataset;
    private double accMean;
    private double accStd;
    private double rmseMean;
    private double rmseStd;
    private int nextTrain;
    private classifier bayesClassifier;

    public Evaluation() {
    }

    public Evaluation(DataSet dataset, String clsName) {
		nextTrain = 0;
        this.dataset = dataset;
        this.clsName = clsName;
    }

    public void crossValidation(int option) {
        int fold = 10; //changed from 10
        Random random = new Random(2013);
        int[] permutation = new int[10000];
        for (int i = 0; i < permutation.length; i++) {
            permutation[i] = i;
        }
        for (int i = 0; i < 10 * permutation.length; i++) {
            int repInd = random.nextInt(permutation.length);
            int ind = i % permutation.length;

            int tmp = permutation[ind];
            permutation[ind] = permutation[repInd];
            permutation[repInd] = tmp;
        }

        int[] perm = new int[dataset.getNumInstnaces()];
        int ind = 0;
        for (int i = 0; i < permutation.length; i++) {
            if (permutation[i] < dataset.getNumInstnaces()) {
                perm[ind++] = permutation[i];
            }
        }

        int share = dataset.getNumInstnaces() / fold;

        boolean[] isCategory = dataset.getIsCategory();
        double[][] features = dataset.getFeatures();
        double[] labels = dataset.getLabels();

        boolean isClassification = isCategory[isCategory.length - 1];

        double[] measures = new double[fold];
        for (int f = 0; f < fold; f++) {
            try {
                int numTest = f < fold - 1 ? share : dataset.getNumInstnaces() - (fold - 1) * share;
                double[][] trainFeatures = new double[dataset.getNumInstnaces() - numTest][dataset.getNumAttributes()];
                double[] trainLabels = new double[dataset.getNumInstnaces() - numTest];
                double[][] testFeatures = new double[numTest][dataset.getNumAttributes()];
                double[] testLabels = new double[numTest];

                int indTrain = 0, indTest = 0;
                for (int j = 0; j < dataset.getNumInstnaces(); j++) {
                    if ((f < fold - 1 && (j < f * share || j >= (f + 1) * share)) || (f == fold - 1 && j < f * share)) {
                        System.arraycopy(features[perm[j]], 0, trainFeatures[indTrain], 0, dataset.getNumAttributes());
                        trainLabels[indTrain] = labels[perm[j]];
                        indTrain++;
                    } else {
                        System.arraycopy(features[perm[j]], 0, testFeatures[indTest], 0, dataset.getNumAttributes());
                        testLabels[indTest] = labels[perm[j]];
                        indTest++;
                    }
                }

                bayesClassifier = (classifier) Class.forName("NaiveBayes." + clsName).newInstance();
                bayesClassifier.train(isCategory, trainFeatures, trainLabels);
                
                if (option == 1) {
	                double error = 0;
	                for (int j = 0; j < testLabels.length; j++) {
	                    double prediction = bayesClassifier.predict(testFeatures[j]);
	                    //System.out.println("See the features: "+Arrays.toString(testFeatures[j]));
	
	                    if (isClassification) {
	                        if (prediction != testLabels[j]) {
	                            error = error + 1;
	                        }
	                    } else {
	                        error = error + (prediction - testLabels[j]) * (prediction - testLabels[j]);
	                    }
	                }
	                if (isClassification) {
	                    measures[f] = 1 - error / testLabels.length;//accuracy = 1 - error
	                } else {
	                    measures[f] = Math.sqrt(error / testLabels.length);
	                }
	                //return c;
                }
                else if (option == 0) {
                	//double[] testonly = {218.0,178.0,9.0,0.111111,0.0,0.833333,0.547722,1.11111,0.544331,59.6296,52.4444,75.2222,51.2222,-21.5556,46.7778,-25.2222,75.2222,0.318996,-2.04055};
                	double[] testonly = {1456};
                	//System.out.println("See the features1: "+Arrays.toString(testFeatures[2]));
                	//System.out.println("See the features2: "+Arrays.toString(testonly));
                	double prediction = bayesClassifier.predict(testonly);
                	
                	System.out.println("New fold:");
                	System.out.println("Print Results: "+prediction);
                	System.out.println("Print Truth: "+5);
                	System.out.println();
                }
                // } else if (option == 2) {
                // 	return c;
                // }
                
                
            } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
                Logger.getLogger(Evaluation.class.getName()).log(Level.SEVERE, null, ex);
            }
        }        
        
        double[] mean_std = mean_std(measures);
        if (isClassification) {
            accMean = mean_std[0];
            accStd = mean_std[1];
        } else {
            rmseMean = mean_std[0];
            rmseStd = mean_std[1];
        }
		// return null;
    }

//    public double bPrediction (double[] input) {
//    	double prediction = c.predict(input);
//    	return prediction;
//    }
    
    public double[] mean_std(double[] x) {
        double[] ms = new double[2];
        int N = x.length;

        ms[0] = 0;
        for (int i = 0; i < x.length; i++) {
            ms[0] += x[i];
        }
        ms[0] /= N;

        ms[1] = 0;
        for (int i = 0; i < x.length; i++) {
            ms[1] += (x[i] - ms[0]) * (x[i] - ms[0]);
        }
        ms[1] /= (N - 1);

        return ms;
    }

    public double getAccMean() {
        return accMean;
    }

    public double getAccStd() {
        return accStd;
    }

    public double getRmseMean() {
        return rmseMean;
    }

    public double getRmseStd() {
        return rmseStd;
    }
    
    public void retrain() {
        this.crossValidation(2);
    }
    
    public double predict(double[] features) {
//        if(nextTrain == 50000) {
//            this.retrain();
//            nextTrain = 0;
//        }
        double prob = bayesClassifier.predict(features);
        dataset.addNewData(features, prob);
 //       nextTrain++;
        return prob;
    }
}
