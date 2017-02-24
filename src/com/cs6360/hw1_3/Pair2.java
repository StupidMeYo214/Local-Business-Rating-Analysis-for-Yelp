package com.cs6360.hw1_3;


public class Pair2{
	private String id;
	private double avg;
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public double getAvg() {
		return avg;
	}
	public void setAvg(double avg) {
		this.avg = avg;
	}
	public Pair2(String id, double avg){
		this.id = id;
		this.avg = avg;
	}
}
