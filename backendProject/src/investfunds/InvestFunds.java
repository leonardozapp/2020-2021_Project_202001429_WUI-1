/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package investfunds;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rlcancian
 */
public class InvestFunds {

	/**
	 * @param args the command line arguments
	 */
	public static void main(String[] args) {
		InvestFundsManager manager;
		try {
			manager = new InvestFundsManager();
			manager.updateInformation();
		} catch (Exception ex) {
			System.out.println("ERRO: Impossível conectar ao banco de dados");
			Logger.getLogger(InvestFunds.class.getName()).log(Level.SEVERE, null, ex);
		} finally {
		}
	}

}
