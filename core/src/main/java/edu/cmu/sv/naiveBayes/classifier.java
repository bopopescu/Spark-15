/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.cmu.sv.naiveBayes;

import java.io.Serializable;

public abstract class classifier implements Cloneable, Serializable {

    public abstract void train(boolean[] isCategory, double[][] features, double[] labels);

    public abstract double predict(double[] features);
}
