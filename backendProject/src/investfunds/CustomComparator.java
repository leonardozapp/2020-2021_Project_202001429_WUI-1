/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package investfunds;

import investfunds.Oldies.CSV2SQLThreadSQLGen;
import java.util.Comparator;
import java.util.function.Function;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

/**
 *
 * @author rlcancian
 */
public class CustomComparator implements Comparator<CSV2SQLThreadSQLGen>{

	/**
	 *
	 * @return
	 */
	@Override
	public Comparator<CSV2SQLThreadSQLGen> reversed() {
		return Comparator.super.reversed(); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 *
	 * @param cmprtr
	 * @return
	 */
	@Override
	public Comparator<CSV2SQLThreadSQLGen> thenComparing(Comparator<? super CSV2SQLThreadSQLGen> cmprtr) {
		return Comparator.super.thenComparing(cmprtr); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 *
	 * @param <U>
	 * @param fnctn
	 * @param cmprtr
	 * @return
	 */
	@Override
	public <U> Comparator<CSV2SQLThreadSQLGen> thenComparing(Function<? super CSV2SQLThreadSQLGen, ? extends U> fnctn, Comparator<? super U> cmprtr) {
		return Comparator.super.thenComparing(fnctn, cmprtr); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 *
	 * @param <U>
	 * @param fnctn
	 * @return
	 */
	@Override
	public <U extends Comparable<? super U>> Comparator<CSV2SQLThreadSQLGen> thenComparing(Function<? super CSV2SQLThreadSQLGen, ? extends U> fnctn) {
		return Comparator.super.thenComparing(fnctn); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 *
	 * @param tif
	 * @return
	 */
	@Override
	public Comparator<CSV2SQLThreadSQLGen> thenComparingInt(ToIntFunction<? super CSV2SQLThreadSQLGen> tif) {
		return Comparator.super.thenComparingInt(tif); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 *
	 * @param tlf
	 * @return
	 */
	@Override
	public Comparator<CSV2SQLThreadSQLGen> thenComparingLong(ToLongFunction<? super CSV2SQLThreadSQLGen> tlf) {
		return Comparator.super.thenComparingLong(tlf); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 *
	 * @param tdf
	 * @return
	 */
	@Override
	public Comparator<CSV2SQLThreadSQLGen> thenComparingDouble(ToDoubleFunction<? super CSV2SQLThreadSQLGen> tdf) {
		return Comparator.super.thenComparingDouble(tdf); //To change body of generated methods, choose Tools | Templates.
	}

	/**
	 *
	 * @param t
	 * @param t1
	 * @return
	 */
	@Override
	public int compare(CSV2SQLThreadSQLGen t, CSV2SQLThreadSQLGen t1) {
		return (int) (t.getDatafilelength()-t1.getDatafilelength());
	}
}