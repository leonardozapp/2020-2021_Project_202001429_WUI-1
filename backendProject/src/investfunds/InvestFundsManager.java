/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package investfunds;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rlcancian
 */
public class InvestFundsManager implements ActionResultLogger {

	private final String remoteCVMDir;
	private final String localCVMDir;
	private final String databasename;
	private final String databaseUsername;
	private final String databasePassword;
	private final Downloader downloader;
	//private final CVS2SQLConverter converter;
	private final CVMDatabaseManager cvmdbman;
	private final String JDBC_DRIVER;
	private final String DB_URL;
	private final boolean forceDownload;
	private final boolean forceSkipDownload;
	private final boolean forceProcessFiles;
	private final boolean forceCalculate;
	private Connection connection;
	private int numReportedErrors;

	/**
	 *
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public InvestFundsManager() throws ClassNotFoundException, SQLException {
		remoteCVMDir = "http://dados.cvm.gov.br/dados/";
		localCVMDir = "./CVM/dados/";
		databasename = "investfunds";
		////// Banco remoto
		//databaseUsername = "remote";
		//databasePassword = "remotepasswd";
		//DB_URL = "jdbc:mysql://fundosinvest.inf.ufsc.br:3306/"; //"jdbc:mysql://localhost/";
		///// Banco local
		databaseUsername = "investfunds";
		databasePassword = "investfunds";
		DB_URL = "jdbc:mysql://localhost/";
		//
		JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
		numReportedErrors = 0;
		forceDownload = false;
		forceSkipDownload = false;
		forceProcessFiles = false;
		forceCalculate = false;
		downloader = new Downloader(forceDownload);
		downloader.setActionResultLogger(this);
		cvmdbman = new CVMDatabaseManager(databasename, databaseUsername, databasePassword, localCVMDir, JDBC_DRIVER, DB_URL, forceProcessFiles, forceCalculate);
		cvmdbman.setActionResultLogger(this);
		ConnectToDB();
	}

	private void ConnectToDB() throws ClassNotFoundException, SQLException {
		Class.forName(this.JDBC_DRIVER);
		connection = DriverManager.getConnection(this.DB_URL + this.databasename, this.databaseUsername, this.databasePassword);
		connection.setAutoCommit(false);
		//Statement st = (Statement) connection.createStatement();
		//st.execute(sql);
		//connection.commit();
		//connection.close();
	}

	/**
	 *
	 */
	public void reportLastUpdate() {
		try {
			String acaoReg;
			String table = "`" + databasename + "`.`logs_atividades`";
			Statement st = (Statement) connection.createStatement();
			String sql = "select date from " + table + " where action='" + ActionResultLogger.Action.BEGIN_UPDATE.toString() + "' order by date desc";
			java.sql.ResultSet rs = st.executeQuery(sql);
			if (rs.next()) {
				Date beginUpdate = new Date();
				// TODO : Será que a tualiza a data?
				beginUpdate.setTime(rs.getTimestamp("date").getTime());
				SimpleDateFormat dtf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				System.out.println("\nRegistro de ações e erros ocorridos a partir de " + dtf.format(beginUpdate));
				sql = "select * from " + table + " where date>='" + dtf.format(beginUpdate) + "'order by date asc";
				rs = st.executeQuery(sql);
				if (rs.next()) {
					do {
						acaoReg = rs.getDate("date").toString() + " " + rs.getTime("date").toString();
						if (rs.getBoolean("HasErrors")) {
							acaoReg += "; ERROR";
						}
						acaoReg += "; " + rs.getString("action");
						if (rs.getString("remoteURI") != null) {
							acaoReg += "; remote='" + rs.getString("remoteURI") + "'";
						}
						if (rs.getString("localURI") != null) {
							acaoReg += "; local='" + rs.getString("localURI") + "'";
						}
						if (rs.getLong("result") > 0) {
							acaoReg += "; result=" + rs.getLong("result");
						}
						if (rs.getString("message") != null) {
							acaoReg += "; message=\"" + rs.getString("message") + "\"";
						}
						System.out.println(acaoReg);
					} while (rs.next());
				} else {
					System.out.println("Nenhuma atualização encontrada.");
				}
			} else {
				System.out.println("Nenhum início de atualização encontrado.");
			}
		} catch (SQLException ex) {
			Logger.getLogger(InvestFundsManager.class.getName()).log(Level.SEVERE, null, ex);
		}
		System.out.println("Fim do registro\n");
		//ActionResultLogger.ActionResult ar = this.getActionResult(new ActionResultLogger.ActionResult(ActionResultLogger.Action.BEGIN_UPDATE.toString(), null, null, null, null, null,  null));
//if (ar != null) {
//	Date beginUpdate = ar.date;
//} else {
//	System.out.println("Nenhuma atualização encontrada.");
//}
	}

	/**
	 *
	 */
	public void updateInformation() {
		boolean isOk = cvmdbman.createDBInfra();// converter.createDBInfra();
		reportLastUpdate();
		recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.BEGIN_UPDATE.toString(), null, null, null, Boolean.FALSE, null, new Date(), Boolean.FALSE));
		/*
		isOk = downloadDataFromB3();
		isOk = cvmdbman.fillCiaDatabases();
		isOk = cvmdbman.calculateCIAIndicators();
		*/	
		isOk = downloadDataFromCVM();
		isOk = cvmdbman.fillDatabases();
		isOk = cvmdbman.calculateDirectIndicators();
		isOk = cvmdbman.calculateMarketIndicators();
		isOk = cvmdbman.calculateIndicatorsBasedOnMarket();

		recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.END_UPDATE.toString(), null, null, null, !isOk, null, new Date(), !isOk));
		reportLastUpdate();
	}

//		public void ReportErrorProcessingFile(String filepath, Exception ex, String datetime, String methodOrDescription) {
//			converter.ReportErrorProcessingFile(filepath, ex, datetime, methodOrDescription);
//		}
	private boolean downloadDataFromCVM() {
		boolean result = downloader.downloadDir(remoteCVMDir, localCVMDir, true);
		return result;
	}

	private boolean downloadDataFromB3() {
		boolean result = downloader.downloadB3Prices(localCVMDir, true);
		return result;
	}

	//private boolean createAndFillDatabases() throws SQLException, ClassNotFoundException, IOException, FileNotFoundException, ParseException, Exception {
	/*
        CVMDatabaseManager cvmdb = new CVMDatabaseManager(databasename, databaseUsername, databasePassword);
        cvmdb.createInvestFundsDatabaseTables();
        cvmdb.fillInvestFundsTablesData(localCVMDir);
	 */
	//converter.CVMRunsOverDirsAndFindMetaAndData();
	// aguarda todas as threads terminarem
	//	return true;
	//}
	/**
	 *
	 * @param ar
	 */
	@Override
	public void recordAction(ActionResult ar) {
		if (ar.HasErrors) {
			this.numReportedErrors++;
		}
		try {
			Statement st = (Statement) connection.createStatement();
			String sql, campos = "", valores = "";
			if (ar.action != null) {
				campos += "action,";
				valores += "\"" + ar.action + "\",";
			}
			if (ar.localURI != null) {
				campos += "localURI,";
				valores += "\"" + ar.localURI + "\",";
			}
			if (ar.remoteURI != null) {
				campos += "remoteURI,";
				valores += "\"" + ar.remoteURI + "\",";
			}
			if (ar.result != null) {
				campos += "result,";
				valores += ar.result.toString() + ",";
			}
			if (ar.HasErrors != null) {
				campos += "HasErrors,";
				valores += ar.HasErrors.toString() + ",";
			}
			if (ar.needToRedo != null) {
				campos += "NeedToRedo,";
				valores += ar.needToRedo.toString() + ",";
			}
			if (ar.message != null) {
				campos += "message,";
				int min = Math.min(399, ar.message.length());
				valores += "\"" + ar.message.replace("\"", "'").substring(0, min) + "\",";
			}
			if (ar.date != null) {
				campos += "date,";
				//YYYY-MM-DD hh:mm:ss
				SimpleDateFormat dtf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
				valores += "\"" + dtf.format(ar.date) + "\",";
			}
			if (campos.length() > 0) {
				campos = campos.substring(0, campos.length() - 1);
				valores = valores.substring(0, valores.length() - 1);
			}
			sql = "insert into logs_atividades (" + campos + ") values(" + valores + ")";
			st.execute(sql);
			connection.commit();
			if (ar.HasErrors != null && ar.message != null) {
				if (ar.HasErrors) {
					System.out.println("");
					System.out.println("ERROR: " + ar.message);
				}
			}
			System.out.println("recd: " + ar.toString());
		} catch (SQLException ex) {
			Logger.getLogger(InvestFundsManager.class
					.getName()).log(Level.SEVERE, null, ex);
			System.out.println();
			System.out.println("ERRO AO REGISTRAR AÇÃO NO BANCO DE DADOS: " + ex.getMessage() + " ; registro: " + ar.toString());
		}
	}

	/**
	 *
	 * @param infos
	 * @return
	 */
	@Override
	public ActionResult getActionResult(ActionResult infos) {
		ActionResult result = null;
		try {
			Statement st = (Statement) connection.createStatement();
			String sql, criterios = "";
			if (infos.action != null) {
				criterios += "action=" + "\"" + infos.action + "\" AND ";
			}
			if (infos.remoteURI != null) {
				criterios += "remoteURI=" + "\"" + infos.remoteURI + "\" AND ";
			}
			if (infos.localURI != null) {
				criterios += "localURI=" + "\"" + infos.localURI.replace("//", "/") + "\" AND ";
			}
			if (infos.result != null) {
				criterios += "result=" + infos.result.toString() + " AND ";
			}
			if (infos.HasErrors != null) {
				criterios += "hasErrors=" + infos.HasErrors.toString() + " AND ";
			}
			if (infos.needToRedo != null) {
				criterios += "needToRedo=" + infos.needToRedo.toString() + " AND ";
			}
			if (infos.message != null) {
				criterios += "message=" + "\"" + infos.message.replace("\"", "'") + "\" AND ";
			}
			if (infos.date != null) {
				criterios += "date=" + "\"" + infos.date.toString() + "\" AND ";
			}
			if (criterios.length() > 0) {
				criterios = criterios.substring(0, criterios.length() - 4);
				sql = "select * from logs_atividades where " + criterios + " order by date desc";
				java.sql.ResultSet rs = st.executeQuery(sql);
				if (rs.next()) {
					result = new ActionResult(rs.getString("action"), rs.getString("remoteURI"), rs.getString("localURI"), rs.getLong("result"), rs.getBoolean("HasErrors"), rs.getString("message"), rs.getDate("date"), rs.getBoolean("needToRedo"));
				}
			}
		} catch (SQLException ex) {
			Logger.getLogger(InvestFundsManager.class
					.getName()).log(Level.SEVERE, null, ex);
		}
		return result;
	}
}
