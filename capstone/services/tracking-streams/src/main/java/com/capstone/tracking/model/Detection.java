package com.capstone.tracking.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Detection {
    public int[] bbox;
    public double confidence;
    public int class_id;
    public String class_name;
}
