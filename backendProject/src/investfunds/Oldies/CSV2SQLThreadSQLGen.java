/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package investfunds.Oldies;

import investfunds.ActionResultLogger;
import investfunds.NotifyingThread;
import investfunds.Oldies.CVS2SQLConverter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rlcancian
 */
public class CSV2SQLThreadSQLGen extends NotifyingThread {

	private final String datafilepath;
	private final long datafilelength;
	private final String datafilename;
	private final CSVMetadata csvMetadata;
	private final String DB_URL;
	private final String databasename;
	private final String databaseUsername;
	private final String databasePassword;
	private final String JDBC_DRIVER;
	private final int csvThreadID;
	private ActionResultLogger actionResultLogger;
	private int countErrosRegistros;
	private final int CTE_COUNT_COMMIT = (int) 1e2;
	private final int CTE_COUNT_SHOW_PROGRESS = (int) 1e4;
	private final int CTE_COUNT_PARTIT_TABLE = (int) 1e5;

	/**
	 * @return the datafilelength
	 */
	public long getDatafilelength() {
		return datafilelength;
	}

	/**
	 * @return the datafilename
	 */
	public String getDatafilename() {
		return datafilename;
	}

	CSV2SQLThreadSQLGen(int threadID, String datafilepath, String datafilename, CSVMetadata csvMetadata, String JDBC_DRIVER, String DB_URL, String databasename, String databaseUsername, String databasePassword) {
		this.csvThreadID = threadID;
		this.datafilepath = datafilepath;
		File file = new File(datafilepath + "/" + datafilename);
		this.datafilelength = file.length();
		this.datafilename = datafilename;
		file = null; // allow garbage collection
		this.csvMetadata = csvMetadata;
		this.JDBC_DRIVER = JDBC_DRIVER;
		this.DB_URL = DB_URL;
		this.databasename = databasename;
		this.databaseUsername = databaseUsername;
		this.databasePassword = databasePassword;
		this.countErrosRegistros = 0;
	}

	private String fileSizeStr(long size) {
		int unit = 0;
		float floatsize = size;
		while (floatsize > 1000) {
			floatsize /= 1000;
			unit++;
		}
		floatsize = ((int) (floatsize * 1e3)) / (float) 1e3;
		String res = Float.toString(floatsize);
		switch (unit) {
			case 0:
				res += "bytes";
				break;
			case 1:
				res += "KB";
				break;
			case 2:
				res += "MB";
				break;
			case 3:
				res += "GB";
				break;
			default:
				res += "TB";
				break;
		}
		return res;
	}

	/*
	private void readRecordsAndFillTable(Connection connection, Statement st, BufferedReader lineReader, int[] metadataFieldIndex, DateTimeFormatter dtf, Integer countRecords) throws IOException, SQLException {
		String thisfileLengthStr = fileSizeStr(this.getDatafilelength());
		String tableName = new File(this.datafilepath).getName();
		tableName = tableName.substring(0, tableName.length() - 4);
		long bytesReadedSoFar = 0;
		countRecords = 0;
		int myCsvThreadID = this.csvThreadID;
		int MAX_INTERNAL_RUNNING_THREADS = 16;
		List<Thread> internalThreadPool = Collections.synchronizedList(new ArrayList<>());
		String lineText = "nao é null";
		while (lineText != null) {
			int numInternalThreads = 0;
			while (numInternalThreads < MAX_INTERNAL_RUNNING_THREADS && ((lineText = lineReader.readLine()) != null)) {
				numInternalThreads++;
				bytesReadedSoFar += lineText.length();
				lineText = lineText.replace('"', '\'');
				CSVRecordThread processLineThread = new CSVRecordThread(lineText, csvMetadata, metadataFieldIndex, databasename, tableName, connection, myCsvThreadID, countRecords);
				internalThreadPool.add(processLineThread);
				processLineThread.start();
			}

			while (internalThreadPool.size()>0) {
				while (internalThreadPool.get(0).isAlive()) {
					Thread.yield(); // libera CPU até a thread índice 0 ter terminado
				}
				internalThreadPool.remove(0);
				countRecords++;
				if (countRecords % 100 == 0) {
					connection.commit();
					if (countRecords % 1e3 == 0) {
						LocalDateTime now = LocalDateTime.now();
						String dataProcessado = dtf.format(now);
						System.out.println("      [" + myCsvThreadID + "." + this.getId() + "] " + countRecords / 1e3 + "K registros (" + fileSizeStr(bytesReadedSoFar) + "/" + thisfileLengthStr + "=" + 100 * bytesReadedSoFar / this.getDatafilelength() + "%) inseridos até " + dataProcessado + " no arquivo '" + this.getDatafilename() + "'");
						System.gc(); // GARBAGE COLLECTOR
					}
				}
			}
		}
	}
	 */
	private Object[] readRecordsAndFillTable(Connection connection, Statement st, BufferedReader lineReader, int[] indexInMetadataFields, String createTableSql) throws IOException, SQLException {
		String thisfileLengthStr = fileSizeStr(this.getDatafilelength());
		String tableNameBase = this.datafilename;//new File(getDatafilepath()).getName();
		tableNameBase = "CVMRAW__" + tableNameBase.substring(0, tableNameBase.length() - 4);
		String tableName = tableNameBase + "__part00";
		int tableBaseNumber = 0;
		int countErrosTabela = 0;
		int countErrosArquivo = 0;
		String nomesCampos, valores, lineText, sql;
		CSVField csvField;
		long bytesReadedSoFar = 0;
		String bytesReadedSoFarStr;
		ActionResultLogger.ActionResult result = this.actionResultLogger.getActionResult(new ActionResultLogger.ActionResult(ActionResultLogger.Action.TABLE_PROCESSED.toString(), null, tableName, null, Boolean.FALSE, null, null, null));
		boolean tableProcessed = (result != null);
		long countRecords = 0;
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
		while ((lineText = lineReader.readLine()) != null) {
			bytesReadedSoFar += lineText.length();
			if (!tableProcessed) {
				lineText = lineText.replace('"', '\'');
				String[] record = lineText.split(";");
				nomesCampos = "";
				valores = "";
				for (int i = 0; i < record.length; i++) {
					if (!record[i].equals("")) {
						csvField = csvMetadata.getCSVFields().get(indexInMetadataFields[i]);
						nomesCampos += csvField.getCampo() + ", ";
						if (csvField.getTipo().equals("varchar") || csvField.getTipo().equals("char") || csvField.getTipo().equals("date")) {
							valores += "\"" + record[i] + "\", ";
							//System.out.println("Campo:" + csvField.getCampo() + " Tipo:" + csvField.getTipo() + " sql:" + "\"" + record[i] + "\", ");
						} else {
							if (record[i].contains(" ")) {
								// provavelmente é um erro no registro no arquivo da CVM. Que porcaria.
								//actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.FILE_PROCESSED.toString(), null, tableName, countRecords, Boolean.TRUE, "Arquivo possui erro no registro "+countRecords+": " + lineText.replace("\"", "'"), new Date()));
								//////System.out.println("ERRO:     ["+this.getId()+"] Arquivo "+datafilename+" possui erro no registro "+countRecords+": " + lineText);
								record[i] = record[i].substring(0, record[i].indexOf(" ") - 1);
								// verifica se a primeira palavra é mesmo um número ou se tudo estava errado
								try {
									Double.parseDouble(record[i]);
								} catch (Exception ex) {
									record[i] = "0.0";
								}
							}
							valores += record[i] + ", ";
							//System.out.println("Campo:" + csvField.getCampo() + " Tipo:" + csvField.getTipo() + " sql:" + record[i] + ", ");

						}

					}
				}
				nomesCampos = nomesCampos.substring(0, nomesCampos.length() - 2);
				valores = valores.substring(0, valores.length() - 2);
				sql = "insert into `" + databasename + "`.`" + tableName + "` (" + nomesCampos + ") values (" + valores + ");";
				st = (Statement) connection.createStatement();
				try {
					st.execute(sql);
				} catch (SQLException ex) {
					countErrosTabela++;
					countErrosArquivo++;
					String message = "Arquivo "+datafilename+" possui erro não recuperado no registro "+countRecords+": '" + ex.getMessage() + "'; Query='" + sql + "'; Line='" + lineText + "'";
					message = message.replace("\"", "'");
					System.out.println("ERRO: [" + this.csvThreadID + "] ERRO no registro " + countRecords + "; " + message);
					//System.out.println("ERRO: [" + this.csvThreadID + "] Query: " + sql);
					//System.out.println("ERRO: [" + this.csvThreadID + "] linetext: " + lineText);
					actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.FILE_PROCESSED.toString(), null, tableName, countRecords, Boolean.TRUE, message, new Date(), Boolean.FALSE));
//				Scanner console = new Scanner(System.in);
//				int length = console.nextInt();

				}
			}
			countRecords++;
			if (countRecords % CTE_COUNT_COMMIT == 0) {
				if (!tableProcessed) {
					connection.commit();
				}
				if (countRecords % CTE_COUNT_SHOW_PROGRESS == 0) {
					if (!tableProcessed) {
						LocalDateTime now = LocalDateTime.now();
						String dataProcessado = dtf.format(now);
						System.out.println("info:      [" + this.csvThreadID + "] " + (int) (countRecords / 1e3) + "K registros (" + fileSizeStr(bytesReadedSoFar) + "/" + thisfileLengthStr + "=" + 100 * bytesReadedSoFar / this.getDatafilelength() + "%) do arquivo '" + this.getDatafilename() + "' na tabela '" + tableName + "' inseridos até " + dataProcessado);
					}
				}
				if (countRecords % CTE_COUNT_PARTIT_TABLE == 0) {
					if (!tableProcessed) {
						//marca tabela como processada
						this.actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.TABLE_PROCESSED.toString(), null, tableName, countRecords, (countErrosTabela > 0), null, new Date(), Boolean.FALSE));
					}
					// particiona em nova tabela
					countErrosTabela = 0;
					tableBaseNumber++;
					tableName = "0" + String.valueOf(tableBaseNumber);
					tableName = tableNameBase + "__part" + tableName.substring(tableName.length() - 2, tableName.length());
					result = this.actionResultLogger.getActionResult(new ActionResultLogger.ActionResult(ActionResultLogger.Action.TABLE_PROCESSED.toString(), null, tableName, null, Boolean.FALSE, null, null, Boolean.FALSE));
					tableProcessed = (result != null);
					if (!tableProcessed) {
						// apaga se existia
						sql = "drop table if exists `" + databasename + "`.`" + tableName + "`;";
						st.execute(sql);
						connection.commit();
						// cria
						sql = createTableSql.replace(tableNameBase + "__part00", tableName);
						sql = sql.replace("PK_" + tableNameBase, "PK_" + tableName);
						st.execute(sql);
						connection.commit();
						System.out.println("info:    [" + this.csvThreadID + "] Tabela " + tableName + " criada.");
					}

					System.gc(); // GARBAGE COLLECTOR
				}
				Thread.yield();
			}
		}
		//marca tabela como processada
		this.actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.TABLE_PROCESSED.toString(), null, tableName, countRecords, (countErrosTabela > 0), null, new Date(), Boolean.FALSE));
		Object[] returns = {countRecords, countErrosArquivo};
		return returns;
	}

	private Object[] readHeaderAndCreateTable(Connection connection, Statement st, BufferedReader lineReader) throws IOException, SQLException {//throws IOException, SQLException {
		//String datafileName = new File(getDatafilepath()).getName();
		String tableName = "CVMRAW__" + datafilename;
		tableName = tableName.substring(0, tableName.length() - 4) + "__part00";
		//String tableName = tableNameBase;// + "__000";
		// header
		String lineText = lineReader.readLine(); // le primeira linha
		String[] header = lineText.split(";"); // que é o header
		int numFields = header.length;
		if (numFields != csvMetadata.getCSVFields().size()) {
			System.out.println("WARN: [" + this.csvThreadID + "] O arquivo tem quantidade de campos (" + numFields + ") diferente que seus metadados (" + csvMetadata.getCSVFields().size() + ")");
		}
		int indexInFieldsList;
		CSVField field;
		String createTableSql = "create table `" + databasename + "`.`" + tableName + "` (\n"
				+ "`PKID_" + tableName + "` int not null auto_increment,\n";
		for (int i = 0; i < numFields; i++) {
			indexInFieldsList = csvMetadata.indexOfField(header[i]);
			if (indexInFieldsList == -1) {
				System.out.println("[" + this.csvThreadID + "] WARN: O campo '" + header[i] + "' parece não fazer parte dos metadados do arquivo");
			} else {
				field = csvMetadata.getCSVFields().get(indexInFieldsList);
				field.setIndexInHeaderArray(i);
				createTableSql += "`" + field.getCampo() + "` " + field.getTipo();
				if (field.getTipo().toLowerCase().equals("float") || field.getTipo().toLowerCase().equals("decimal") || field.getTipo().toLowerCase().equals("numeric")) {
					createTableSql += "(" + field.getPrecisao();
					if (field.getEscala() >= 0) {
						createTableSql += "," + field.getEscala();
					}
					createTableSql += ")";
				} else if (field.getTipo().toLowerCase().equals("real")) {
					if (field.getEscala() == -1) {
						field.setEscala(0);
					}
					createTableSql += "(" + field.getPrecisao() + "," + field.getEscala() + ")";
				} else if (field.getTipo().toLowerCase().contains("int")) {
					createTableSql += "(" + field.getPrecisao() + ")";
				} else if (!field.getTipo().toLowerCase().equals("date")) {
					createTableSql += "(" + field.getTamanho() + ")";
				}
			}
			//System.out.println("Field '"+field.getCampo()+"' type '"+field.getTipo()+"' index in Meta="+index+" index in header="+i);
			createTableSql += " null,\n";
		}
		createTableSql += "PRIMARY KEY(`PKID_" + tableName + "`)) ENGINE=InnoDB;";
		if (createTableSql.contains("-1")) {
			System.out.println("ERRO: " + createTableSql);
		}

		ActionResultLogger.ActionResult result = this.actionResultLogger.getActionResult(new ActionResultLogger.ActionResult(ActionResultLogger.Action.TABLE_PROCESSED.toString(), null, tableName, null, Boolean.FALSE, null, null, Boolean.FALSE));
		if (result == null) {
			try {
				String sql = "drop table if exists `" + databasename + "`.`" + tableName + "`;";
				st.execute(sql);
				connection.commit();
				st.execute(createTableSql);
			} catch (SQLException ex) {
				Logger.getLogger(CSV2SQLThreadSQLGen.class.getName()).log(Level.SEVERE, null, ex);
				actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.FILE_PROCESSED.toString(), null, tableName, null, Boolean.TRUE, "Query:" + createTableSql + " ; Message:" + ex.getMessage(), new Date(), Boolean.FALSE));
			}
			connection.commit();
			System.out.println("info:    [" + this.csvThreadID + "] Tabela " + tableName + " criada.");
		} else {
			System.out.println("done:    [" + this.csvThreadID + "] Tabela previamente processada " + tableName);
		}
		Object[] returns = {numFields, createTableSql};
		return returns;
	}

	/**
	 *
	 */
	@Override
	public void doRun() {
		String sql = "";
		long countRecords = 0;
		try {
			System.out.println("ação: [" + this.csvThreadID + "] Thread começou a executar o processamento do arquivo \"" + datafilename + "\"");
			//System.out.println("  [" + this.csvThreadID + "] Conectando ao bando de dados");
			//boolean fileAlreadyProcessed = false;
			Class.forName(JDBC_DRIVER);
			Connection connection;
			connection = DriverManager.getConnection(DB_URL + databasename, databaseUsername, databasePassword);
			connection.setAutoCommit(false);
			Statement st = (Statement) connection.createStatement();
			BufferedReader lineReader = new BufferedReader(new FileReader(this.datafilepath + "/" + this.datafilename));
			//header
			//------------------
			Object[] returns = readHeaderAndCreateTable(connection, st, lineReader);//, createTableSql, numFields);
			int numFields = (int) returns[0];
			String createTableSql = (String) returns[1];
			//------------------				
			int[] indexInMetadataFields = new int[numFields];
			int j = 0;
			int indexInHeaderArray;
			for (CSVField csvField : csvMetadata.getCSVFields()) {
				indexInHeaderArray = csvField.getIndexInHeaderArray();
				if (indexInHeaderArray < numFields && indexInHeaderArray >= 0) {
					indexInMetadataFields[indexInHeaderArray] = j;
				} else {
					//TODO: AVALIAR MELHOR
					System.out.println("WARN:      [" + this.csvThreadID + "] O campo '" + csvField.getCampo() + "' existe no metadata mas não no cabeçalho do arquivo " + this.datafilename);
				}
				j++;
			}
			Object[] result = readRecordsAndFillTable(connection, st, lineReader, indexInMetadataFields, createTableSql);
			countRecords = (long) result[0];
			countErrosRegistros = (int) result[1];
			st.close();
			connection.close();
			System.out.println("info:      [" + this.csvThreadID + "] " + countRecords + " registros inseridos. Processamento do arquivo " + datafilename + " concluído.");// em " + dataProcessado);
			//String dir =  new File(datafilepath).getAbsolutePath();
			//dir = new File(datafilepath).getCanonicalPath();
			actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.FILE_PROCESSED.toString(), datafilepath, datafilename, countRecords, Boolean.FALSE, null, new Date(), Boolean.FALSE));
			Thread.yield();
		} catch (SQLException | IOException | ClassNotFoundException ex) {
			System.out.println("ERRO: [" + this.csvThreadID + "] WARN: Erro no processamento do arquivo " + datafilename + ": " + ex.getMessage());
			System.out.println("ERRO: [" + this.csvThreadID + "] Query: " + sql);
			Logger.getLogger(CVS2SQLConverter.class.getName()).log(Level.SEVERE, null, ex);
			actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.FILE_PROCESSED.toString(), datafilepath, datafilename, countRecords, Boolean.TRUE, "Query:" + sql + " ; Message:" + ex.getMessage(), new Date(), Boolean.FALSE));
		}
	}

	/**
	 * @return the threadID
	 */
	public int getCSVThreadID() {
		return csvThreadID;
	}

	/**
	 * @param actionResultLogger the actionResultLogger to set
	 */
	public void setActionResultLogger(ActionResultLogger actionResultLogger) {
		this.actionResultLogger = actionResultLogger;
	}

	/**
	 *
	 * @return
	 */
	public int getCountErrosRegistros() {
		return countErrosRegistros;
	}
}
