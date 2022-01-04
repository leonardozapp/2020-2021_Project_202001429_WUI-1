/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package investfunds;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 *
 * @author rlcancian
 */
public class InvestFundsManagerIT {

	/**
	 *
	 */
	public InvestFundsManagerIT() {
	}

	/**
	 *
	 */
	@BeforeAll
	public static void setUpClass() {
	}

	/**
	 *
	 */
	@AfterAll
	public static void tearDownClass() {
	}

	/**
	 *
	 */
	@BeforeEach
	public void setUp() {
	}

	/**
	 *
	 */
	@AfterEach
	public void tearDown() {
	}

	/**
	 * Test of reportLastUpdate method, of class InvestFundsManager.
	 */
	@Test
	public void testReportLastUpdate() {
		System.out.println("reportLastUpdate");
		InvestFundsManager instance = new InvestFundsManager();
		instance.reportLastUpdate();
		// TODO review the generated test code and remove the default call to fail.
		fail("The test case is a prototype.");
	}

	/**
	 * Test of updateInformation method, of class InvestFundsManager.
	 */
	@Test
	public void testUpdateInformation() {
		System.out.println("updateInformation");
		InvestFundsManager instance = new InvestFundsManager();
		instance.updateInformation();
		// TODO review the generated test code and remove the default call to fail.
		fail("The test case is a prototype.");
	}

	/**
	 * Test of recordAction method, of class InvestFundsManager.
	 */
	@Test
	public void testRecordAction() {
		System.out.println("recordAction");
		ActionResultLogger.ActionResult ar = null;
		InvestFundsManager instance = new InvestFundsManager();
		instance.recordAction(ar);
		// TODO review the generated test code and remove the default call to fail.
		fail("The test case is a prototype.");
	}

	/**
	 * Test of getActionResult method, of class InvestFundsManager.
	 */
	@Test
	public void testGetActionResult() {
		System.out.println("getActionResult");
		ActionResultLogger.ActionResult infos = null;
		InvestFundsManager instance = new InvestFundsManager();
		ActionResultLogger.ActionResult expResult = null;
		ActionResultLogger.ActionResult result = instance.getActionResult(infos);
		assertEquals(expResult, result);
		// TODO review the generated test code and remove the default call to fail.
		fail("The test case is a prototype.");
	}

}
