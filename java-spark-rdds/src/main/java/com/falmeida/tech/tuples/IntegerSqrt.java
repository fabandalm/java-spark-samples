package com.falmeida.tech.tuples;

import java.io.Serializable;

public class IntegerSqrt implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Integer number;
	private Double sqrtNumber;
	
	public IntegerSqrt(Integer number) {
		this.number = number;
		this.sqrtNumber = Math.sqrt(number);
	}

	public Integer getNumber() {
		return number;
	}

	public void setNumber(Integer number) {
		this.number = number;
	}

	public Double getSqrtNumber() {
		return sqrtNumber;
	}

	public void setSqrtNumber(Double sqrtNumber) {
		this.sqrtNumber = sqrtNumber;
	}

	@Override
	public String toString() {
		return "IntegerSqrt [number=" + number + ", sqrtNumber=" + sqrtNumber + "]";
	}
	
}
