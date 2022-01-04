/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package investfunds;

/**
 *
 * @author rlcancian
 */
public interface ThreadCompleteListener {

	/**
	 *
	 * @param thread
	 */
	void notifyOfThreadComplete(final Thread thread);
}
