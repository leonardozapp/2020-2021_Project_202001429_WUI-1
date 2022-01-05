/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package investfunds;

import investfunds.Oldies.CVS2SQLConverter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rlcancian
 */
public class CVMDatabaseThread extends NotifyingThread {

	/**
	 * @return the tempoTotalThreads
	 */
	public static double GetTempoTotalThreads() {
		return tempoTotalThreads;
	}

	/**
	 *
	 * @return
	 */
	public static Set<String> Get_cad_cnpj_fundo_nao_encontrado() {
		Set<String> sysSet = Collections.synchronizedSet(CVMDatabaseThread.tb_cnpj_fundos_nao_cadastrados.keySet());
		return sysSet;
	}

	/**
	 * @return the numTotalThreads
	 */
	public static int GetNumTotalThreads() {
		return numTotalThreads;
	}

	/**
	 *
	 */
	public static void ResetTempoTotalThreads() {
		if (numTotalThreads > 0) {
			tempoTotalThreads = ((double) tempoTotalThreads / numTotalThreads);
		} else {
			tempoTotalThreads = 0;
		}
		numTotalThreads = 1;
	}

	private final String datafilepath;
	private final String localCVMSubdir;
	private final long datafilelength;
	private final String datafilename;
	// DB
	private final String DB_URL;
	private final String databasename;
	private final String databaseUsername;
	private final String databasePassword;
	private final String JDBC_DRIVER;
	// force
	private final boolean forceProcessFiles;
	private final boolean forceCalculate;
	//
	private final int csvThreadID;
	private ActionResultLogger actionResultLogger;
	private int countErrosRegistros;
	// acesso rápido às tabelas
	private static final Map<String, Integer> tb_cnpj_fundos_nao_cadastrados = Collections.synchronizedMap(new HashMap<>());
	private static final Map<String, Integer> tb_cad_cnpj_fundo = Collections.synchronizedMap(new HashMap<>());
	private static final Map<String, Integer> tb_administrador_fundos = Collections.synchronizedMap(new HashMap<>());
	private static final Map<String, Integer> tb_gestor_fundos = Collections.synchronizedMap(new HashMap<>());
	private static final Map<String, Integer> tb_tipo_classe_fundos = Collections.synchronizedMap(new HashMap<>());
	private static final Map<String, Integer> tb_tipo_situacao_fundos = Collections.synchronizedMap(new HashMap<>());
	private static final Map<String, Integer> tb_tipo_rentabilidade_fundos = Collections.synchronizedMap(new HashMap<>());
	private static final Map<String, Integer> tb_tipo_anbima_classes = Collections.synchronizedMap(new HashMap<>());

	private static final Map<String, Integer> tb_doc_ = Collections.synchronizedMap(new HashMap<>());
	//private static final Map<String, Integer> tb_cnpj_pessoa = Collections.synchronizedMap(new HashMap<>());
	// statistics
	private static double tempoTotalThreads = 0;
	private static int numTotalThreads = 0;

	CVMDatabaseThread(int threadID, String localDVMSubdir, String datafilepath, String datafilename, String JDBC_DRIVER, String DB_URL, String databasename, String databaseUsername, String databasePassword, boolean forceProcessFiles, boolean forceCalculate) {
		this.csvThreadID = threadID;
		this.localCVMSubdir = localDVMSubdir;
		this.datafilepath = datafilepath;
		File file = new File(datafilepath + "/" + datafilename);
		this.datafilelength = file.length();
		this.datafilename = datafilename;
		file = null; // allow garbage collection
		this.JDBC_DRIVER = JDBC_DRIVER;
		this.DB_URL = DB_URL;
		this.databasename = databasename;
		this.databaseUsername = databaseUsername;
		this.databasePassword = databasePassword;
		this.countErrosRegistros = 0;
		//
		this.forceProcessFiles = forceProcessFiles;
		this.forceCalculate = forceCalculate;
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

	/**
	 *
	 * @return
	 */
	public long getDatafilelength() {
		return datafilelength;
	}

	/**
	 *
	 * @return
	 */
	public String getDatafilename() {
		return datafilename;
	}

	private Object[] processa_DOC_EXTRATO(Connection connection) throws SQLException {
		long inicioProcessamento = System.currentTimeMillis();
		long tempoProcessamentoParte, tempoProcessamentoTotal, fimProcessamento = inicioProcessamento;
		BufferedReader lineReader = null;
		Long countRecords = 0L;
		Long countRecordsComErro = 0L;
		Long countRecordsRepetidos = 0L;
		try {
			// Processa arquivos de doc / extrato
			// le arquivo
			//lineReader = new BufferedReader(new FileReader(this.datafilepath + "/" + this.datafilename));
			//lineReader = new BufferedReader(new InputStreamReader(new FileInputStream(this.datafilepath + "/" + this.datafilename), "UTF-8"));
			lineReader = Files.newBufferedReader(Paths.get(this.datafilepath + "/" + this.datafilename), Charset.forName("ISO-8859-1"));
			String lineText = lineReader.readLine(); // le primeira linha
			String[] header = lineText.split(";"); // que é o header
			int numFields = header.length;
			HashMap<String, Integer> fields = new HashMap<>();
			for (int i = 0; i < numFields; i++) {
				fields.put(header[i], i);
			}
			String dataArquivo = this.datafilename.replace("extrato_fi_", "").replace(".csv", ""); // pega apenas os numeros num nome de arquivo como: inf_cadastral_fi_20200626.csv
			dataArquivo = dataArquivo.substring(0, 4);
			String sql;
			Statement st = (Statement) connection.createStatement();
			java.sql.ResultSet rs;
			long bytesReadedSoFar = 0;
			String CNPJ_FUNDO, DT_COMPTC, classe_anbima, APLIC_MIN, QT_DIA_PAGTO_COTA, QT_DIA_RESGATE_COTAS, QT_DIA_PAGTO_RESGATE, PR_COTA_ETF_MAX;//, DENOM_SOCIAL, situacao, CLASSE, rentabilidade, DT_CANCEL, CNPJ_ADMIN, CNPJ_GESTOR, NOME;
			Integer ID_FUNDO, ID_CLASSE_ANBIMA;//, ID_SIT, ID_CLASSE, ID_RENTAB, CONDOM_ABERTO, FUNDO_COTAS, FUNDO_EXCLUSIVO, INVEST_QUALIF, ID_ADMIN = null, ID_GESTOR = null;
			while ((lineText = lineReader.readLine()) != null) {
				bytesReadedSoFar += lineText.length();
				lineText = lineText.replace("\"", "'") + " ";
				lineText = lineText.replace(";;", "; ;");
				String[] record = lineText.split(";");
				if (record.length < numFields) {
					System.out.println("erro: O registro " + countRecords + " possui apenas " + record.length + " campos, enquanto o cabeçalho especifica " + numFields + " campos");
				}
				CNPJ_FUNDO = record[fields.get("CNPJ_FUNDO")].strip();
				ID_FUNDO = CVMDatabaseThread.tb_cad_cnpj_fundo.get(CNPJ_FUNDO); // procura no map em memória
				if (ID_FUNDO == null) {
					// nova informação cadastral de fundo. Busca infos do fundo no BD
					if (CVMDatabaseThread.tb_cnpj_fundos_nao_cadastrados.get(CNPJ_FUNDO) == null) {
						sql = "select id from cnpj_fundos USE INDEX(idx_cnpj) where cnpj=\"" + CNPJ_FUNDO + "\"";
						rs = st.executeQuery(sql);
						if (rs.next()) {
							ID_FUNDO = rs.getInt("id");
							CVMDatabaseThread.tb_cad_cnpj_fundo.put(CNPJ_FUNDO, ID_FUNDO); // adiciona ao map
						} else {
							/// MUITO IMPROVÁVEL, MAS HÁ INFORMAÇÕES DE UM FUNDO QUE AINDA NÃO FOI CADASTRADO
							System.out.println("erro:      [" + this.csvThreadID + "]  Fundo cnpj=" + CNPJ_FUNDO + " não foi cadastrado, e há extratos sobre ele. Inserindo o CNPJ do fundo, mesmo sem informações de cadastro.");
							sql = "select id from cnpj_fundos_nao_cadastrados USE INDEX(idx_cnpj) where cnpj=\"" + CNPJ_FUNDO + "\"";
							rs = st.executeQuery(sql);
							if (!rs.next()) {
								sql = "insert into cnpj_fundos_nao_cadastrados (cnpj, DENOM_SOCIAL) values (\"" + CNPJ_FUNDO + "\", \"--DESCONHECIDO--\")";
								try {
									st.execute(sql);
									connection.commit();
								} catch (Exception ex) {
								}
								sql = "select id from cnpj_fundos_nao_cadastrados USE INDEX(idx_cnpj) where cnpj=\"" + CNPJ_FUNDO + "\"";
								rs = st.executeQuery(sql);
								rs.next();
							}
							ID_FUNDO = rs.getInt("id");
							CVMDatabaseThread.tb_cnpj_fundos_nao_cadastrados.put(CNPJ_FUNDO, ID_FUNDO);
							ID_FUNDO = null; // aqui é apenas o id do cnpj não cadastrado. Nunca achou o id do FUNDO
							// eu decidi que vou "cadastrar" o fundo; ao menos registrar seu CNPJ na tabela "cnpj_fundos"
							// fundo não existe nem no BD. Inclui e depois pega id
							sql = "insert into cnpj_fundos (cnpj,  DENOM_SOCIAL, DT_REG_CVM) values(\"" + CNPJ_FUNDO + "\",\"--DESCONHECIDO--\",\"" + dataArquivo + "\")";
							try {
								st.execute(sql);
								connection.commit();
							} catch (SQLException e) {
								System.out.println("Erro ao incluir fundo não cadastrado " + e.getMessage());
							}
							sql = "select id from cnpj_fundos USE INDEX(idx_cnpj) where cnpj=\"" + CNPJ_FUNDO + "\"";
							rs = st.executeQuery(sql);
							if (rs.next()) {
								ID_FUNDO = rs.getInt("id");
								CVMDatabaseThread.tb_cad_cnpj_fundo.put(CNPJ_FUNDO, ID_FUNDO); // adiciona ao map
							} else {
								countRecordsComErro++;
							}
						}
					} else {
						countRecordsComErro++;
					}
				}
				// aqui o id do fundo é conhecido (???) e está no map e no BD

				if (ID_FUNDO != null) {
					DT_COMPTC = record[fields.get("DT_COMPTC")].strip();
					classe_anbima = record[fields.get("CLASSE_ANBIMA")].strip();
					ID_CLASSE_ANBIMA = CVMDatabaseThread.tb_tipo_anbima_classes.get(classe_anbima);
					if (ID_CLASSE_ANBIMA == null) {
						// nova informação cadastral de fundo. Busca infos do fundo no BD
						sql = "select id from tipo_anbima_classes  where classe_anbima=\"" + classe_anbima + "\"";
						rs = st.executeQuery(sql);
						if (!rs.next()) {
							// situação não existe nem no BD. Inclui e depois pega id
							sql = "insert into tipo_anbima_classes (classe_anbima) values(\"" + classe_anbima + "\")";
							try {
								st.execute(sql);
							} catch (SQLException ex) {
							}
							connection.commit();
							sql = "select id from tipo_anbima_classes  where classe_anbima=\"" + classe_anbima + "\"";
							rs = st.executeQuery(sql);
							rs.next(); // deve haver um registro retornado
						}
						ID_CLASSE_ANBIMA = rs.getInt("id");
						CVMDatabaseThread.tb_tipo_anbima_classes.put(classe_anbima, ID_CLASSE_ANBIMA); // adiciona ao map
					}
					// aqui o ID_CLASSE_ANBIMA é conhecido e está no map e no BD
					// = "";
					APLIC_MIN = record[fields.get("APLIC_MIN")].strip();
					QT_DIA_PAGTO_COTA = record[fields.get("QT_DIA_PAGTO_COTA")].strip();
					//QT_DIA_RESGATE_COTAS = record[fields.get("QT_DIA_RESGATE_COTAS")].strip();
					QT_DIA_PAGTO_RESGATE = record[fields.get("QT_DIA_PAGTO_RESGATE")].strip();
					PR_COTA_ETF_MAX = record[fields.get("PR_COTA_ETF_MAX")].strip();

					sql = "insert into doc_extratos_fundos "
							+ "(cnpj_fundo_id, DT_COMPTC, tipo_anbima_classe_id, APLIC_MIN, QT_DIA_PAGTO_COTA, QT_DIA_PAGTO_RESGATE, PR_COTA_ETF_MAX)"
							+ "values (" + ID_FUNDO + ", \"" + DT_COMPTC + "\", " + ID_CLASSE_ANBIMA + ", " + APLIC_MIN + ", " + QT_DIA_PAGTO_COTA + ", " + QT_DIA_PAGTO_RESGATE + ", " + PR_COTA_ETF_MAX + ")";
					try {
						st.execute(sql);
					} catch (SQLException ex) {// sql pode falhar por haver linhas com informações duplicadas no CSV.
						if (ex.getMessage().contains("Duplicate")) {
							countRecordsRepetidos++;
						} else {
							countRecordsComErro++;
						}
					}
				}

				// fim do processamento do registro
				countRecords++;
				if (countRecords % 1e3 == 0) {
					connection.commit();
					if (countRecords % 1e4 == 0) {
						tempoProcessamentoParte = (System.currentTimeMillis() - fimProcessamento);
						tempoProcessamentoTotal = (System.currentTimeMillis() - inicioProcessamento);
						System.out.println("info:      [" + this.csvThreadID + "] " + countRecords / 1e3 + "K registros processados até o momento, sendo " + countRecordsComErro + " com erro e " + countRecordsRepetidos + " preexistentes (" + (int) (100 * bytesReadedSoFar / datafilelength) + "% em " + (int) (tempoProcessamentoParte / 1e3) + "s, totalizando " + (int) (tempoProcessamentoTotal / 1e3) + " s)");
						fimProcessamento = System.currentTimeMillis();
					}
				}
			}
			st.close();
		} catch (IOException | SQLException ex) {
			Logger.getLogger(CVMDatabaseThread.class.getName()).log(Level.SEVERE, null, ex);
		} finally {
			try {
				lineReader.close();
			} catch (IOException ex) {
				Logger.getLogger(CVMDatabaseThread.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		Object[] result = {countRecords, countRecordsComErro, countRecordsRepetidos};
		return result;
	}

	private Object[] processa_DOC_COMPL(Connection connection) throws SQLException {
		long inicioProcessamento = System.currentTimeMillis();
		long tempoProcessamentoParte, tempoProcessamentoTotal, fimProcessamento = inicioProcessamento;
		BufferedReader lineReader = null;
		Long countRecords = 0L;
		Long countRecordsComErro = 0L;
		Long countRecordsRepetidos = 0L;

		// TODO
		countRecordsComErro = 0L;
		Object[] result = {countRecords, countRecordsComErro, countRecordsRepetidos};
		return result;
	}

	private Object[] processa_DOC_INF_DIARIO(Connection connection) throws SQLException {
		long inicioProcessamento = System.currentTimeMillis();
		long tempoProcessamentoParte, tempoProcessamentoTotal, fimProcessamento = inicioProcessamento;
		BufferedReader lineReader = null;
		Long countRecords = 0L;
		Long countRecordsComErro = 0L;
		Long countRecordsRepetidos = 0L;
		long bytesReadedSoFar = 0;
		try {
			// Processa arquivos de informação cadastral
			// le arquivo
			//lineReader = new BufferedReader(new FileReader(this.datafilepath + "/" + this.datafilename));
			//lineReader = new BufferedReader(new InputStreamReader(new FileInputStream(this.datafilepath + "/" + this.datafilename), "UTF-8"));
			lineReader = Files.newBufferedReader(Paths.get(this.datafilepath + "/" + this.datafilename), Charset.forName("ISO-8859-1"));
			String lineText = lineReader.readLine(); // le primeira linha
			String[] header = lineText.split(";"); // que é o header
			int numFields = header.length;
			HashMap<String, Integer> fields = new HashMap<>();
			for (int i = 0; i < numFields; i++) {
				fields.put(header[i], i);
			}
			String dataArquivo = this.datafilename.replace("inf_diario_fi_", "").replace(".csv", ""); // pega apenas os numeros num nome de arquivo como: inf_diario_fi_201704.csv
			dataArquivo = dataArquivo.substring(0, 4) + "-" + dataArquivo.substring(4, 6);
			String sql;
			Statement st = connection.createStatement();
			java.sql.ResultSet rs;
			String CNPJ_FUNDO, DT_COMPTC, VL_TOTAL, VL_QUOTA, VL_PATRIM_LIQ, CAPTC_DIA, RESG_DIA, NR_COTST;
			Integer ID_FUNDO;
			while ((lineText = lineReader.readLine()) != null) {
				bytesReadedSoFar += lineText.length();
				lineText = lineText.replace("\"", "'") + " ";
				lineText = lineText.replace(";;", "; ;");
				String[] record = lineText.split(";");
				if (record.length < numFields) {
					System.out.println("erro:       [" + this.csvThreadID + "] O registro " + countRecords + " possui apenas " + record.length + " campos, enquanto o cabeçalho especifica " + numFields + " campos");
				}
				CNPJ_FUNDO = record[fields.get("CNPJ_FUNDO")].strip();
				ID_FUNDO = CVMDatabaseThread.tb_cad_cnpj_fundo.get(CNPJ_FUNDO); // procura no map em memória
				if (ID_FUNDO == null) {
					// nova informação cadastral de fundo. Busca infos do fundo no BD
					if (CVMDatabaseThread.tb_cnpj_fundos_nao_cadastrados.get(CNPJ_FUNDO) == null) {
						sql = "select id from cnpj_fundos USE INDEX(idx_cnpj) where cnpj=\"" + CNPJ_FUNDO + "\"";
						rs = st.executeQuery(sql);
						if (rs.next()) {
							ID_FUNDO = rs.getInt("id");
							CVMDatabaseThread.tb_cad_cnpj_fundo.put(CNPJ_FUNDO, ID_FUNDO); // adiciona ao map
						} else {
							/// MUITO IMPROVÁVEL, MAS HÁ INFORMAÇÕES DE UM FUNDO QUE AINDA NÃO FOI CADASTRADO
							System.out.println("erro:      [" + this.csvThreadID + "]  Fundo cnpj=" + CNPJ_FUNDO + " não foi cadastrado, e há informes diários sobre ele. Inserindo o CNPJ do fundo, mesmo sem informações de cadastro.");
							sql = "select id from cnpj_fundos_nao_cadastrados USE INDEX(idx_cnpj) where cnpj=\"" + CNPJ_FUNDO + "\"";
							rs = st.executeQuery(sql);
							if (!rs.next()) {
								sql = "insert into cnpj_fundos_nao_cadastrados (cnpj, DENOM_SOCIAL) values (\"" + CNPJ_FUNDO + "\", \"--DESCONHECIDO--\")";
								try {
									st.execute(sql);
									connection.commit();
								} catch (Exception ex) {
								}
								sql = "select id from cnpj_fundos_nao_cadastrados USE INDEX(idx_cnpj) where cnpj=\"" + CNPJ_FUNDO + "\"";
								rs = st.executeQuery(sql);
								rs.next();
							}
							ID_FUNDO = rs.getInt("id");
							CVMDatabaseThread.tb_cnpj_fundos_nao_cadastrados.put(CNPJ_FUNDO, ID_FUNDO);
							ID_FUNDO = null; // aqui é apenas o id do cnpj não cadastrado. Nunca achou o id do FUNDO
							// eu decidi que vou "cadastrar" o fundo; ao menos registrar seu CNPJ na tabela "cnpj_fundos"
							// fundo não existe nem no BD. Inclui e depois pega id
							sql = "insert into cnpj_fundos (cnpj,  DENOM_SOCIAL, DT_REG_CVM) values(\"" + CNPJ_FUNDO + "\",\"--DESCONHECIDO--\",\"" + dataArquivo + "-01\")";
							try {
								st.execute(sql);
								connection.commit();
							} catch (SQLException e) {
								System.out.println("Erro ao incluir fundo não cadastrado " + e.getMessage());
							}
							sql = "select id from cnpj_fundos USE INDEX(idx_cnpj) where cnpj=\"" + CNPJ_FUNDO + "\"";
							rs = st.executeQuery(sql);
							if (rs.next()) {
								ID_FUNDO = rs.getInt("id");
								CVMDatabaseThread.tb_cad_cnpj_fundo.put(CNPJ_FUNDO, ID_FUNDO); // adiciona ao map
							} else {
								countRecordsComErro++;
							}
						}
					} else {
						countRecordsComErro++;
					}
				}
				// aqui o id do fundo é conhecido (???) e está no map e no BD

				if (ID_FUNDO != null) {
					// cadastra informações básicas
					DT_COMPTC = record[fields.get("DT_COMPTC")].strip();
					VL_TOTAL = record[fields.get("VL_TOTAL")].strip();
					VL_QUOTA = record[fields.get("VL_QUOTA")].strip();
					VL_PATRIM_LIQ = record[fields.get("VL_PATRIM_LIQ")].strip();
					CAPTC_DIA = record[fields.get("CAPTC_DIA")].strip();
					RESG_DIA = record[fields.get("RESG_DIA")].strip();
					NR_COTST = record[fields.get("NR_COTST")].strip();

					sql = "INSERT INTO doc_inf_diario_fundos "
							+ "(cnpj_fundo_id, DT_COMPTC, VL_TOTAL, VL_QUOTA, VL_PATRIM_LIQ, CAPTC_DIA, RESG_DIA, NR_COTST) "
							+ " VALUES (" + ID_FUNDO + ", \"" + DT_COMPTC + "\", " + VL_TOTAL + ", " + VL_QUOTA + ", " + VL_PATRIM_LIQ + ", " + CAPTC_DIA + ", " + RESG_DIA + ", " + NR_COTST + "); ";
					try {
						st.execute(sql);
					} catch (SQLException ex) {// sql pode falhar por haver linhas com informações duplicadas no CSV.
						if (ex.getMessage().contains("Duplicate")) {
							countRecordsRepetidos++;
						} else {
							//actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.FILE_PROCESSED.toString(), datafilepath, datafilename, countRecords, Boolean.TRUE, null, new Date()));
							countRecordsComErro++;
						}
					}
				}

				// fim do processamento do registro
				countRecords++;
				if (countRecords % 1e3 == 0) {
					connection.commit();
					if (countRecords % 1e4 == 0) {
						tempoProcessamentoParte = (System.currentTimeMillis() - fimProcessamento);
						tempoProcessamentoTotal = (System.currentTimeMillis() - inicioProcessamento);
						System.out.println("info:      [" + this.csvThreadID + "] " + countRecords / 1e3 + "K registros processados até o momento, sendo " + countRecordsComErro + " com erro e " + countRecordsRepetidos + " preexistentes (" + (int) (100 * bytesReadedSoFar / datafilelength) + "% em " + (int) (tempoProcessamentoParte / 1e3) + "s, totalizando " + (int) (tempoProcessamentoTotal / 1e3) + " s)");
						fimProcessamento = System.currentTimeMillis();
					}
				}
			}
			st.close();
		} catch (IOException | SQLException ex) {
			Logger.getLogger(CVMDatabaseThread.class.getName()).log(Level.SEVERE, null, ex);
		} finally {
			try {
				lineReader.close();
			} catch (IOException ex) {
				Logger.getLogger(CVMDatabaseThread.class.getName()).log(Level.SEVERE, null, ex);
			}
		}
		Object[] result = {countRecords, countRecordsComErro, countRecordsRepetidos};
		return result;
	}

	private Object[] processa_INTERMEDIARIO_CAD(Connection connection) throws SQLException {
		long inicioProcessamento = System.currentTimeMillis();
		long tempoProcessamentoParte, tempoProcessamentoTotal, fimProcessamento = inicioProcessamento;
		BufferedReader lineReader = null;
		Long countRecords = 0L;
		Long countRecordsComErro = 0L;
		Long countRecordsRepetidos = 0L;
		boolean fundoComErrosComDados = false;
		try {

		} catch (Exception ex) {
			Logger.getLogger(CVMDatabaseThread.class.getName()).log(Level.SEVERE, null, ex);
		}

		Object[] result = {countRecords, countRecordsComErro, countRecordsRepetidos};
		return result;
	}

	private Object[] insere_cias_meta(Connection connection) throws SQLException {
		// long inicioProcessamento = System.currentTimeMillis();
		// long tempoProcessamentoParte, tempoProcessamentoTotal, fimProcessamento = inicioProcessamento;
		String sql;
		// System.out.println("name "+this.datafilename);
		// System.out.println("path "+this.datafilepath);
		connection.setAutoCommit(false);
		Statement st = connection.createStatement();

		try {
			sql = "SET GLOBAL local_infile=1;";
			st.execute(sql);
			connection.commit();
		} catch (SQLException e) {
			System.out.println("erro: Não pude mudar a variável local_infile: " + e.getMessage() + " / " + e.getSQLState());
		}

		// CNPJ_CIA	DENOM_SOCIAL	DENOM_COMERC	DT_REG	DT_CONST	DT_CANCEL	MOTIVO_CANCEL	SIT	DT_INI_SIT	CD_CVM	SETOR_ATIV	TP_MERC	CATEG_REG	DT_INI_CATEG	SIT_EMISSOR	DT_INI_SIT_EMISSOR	CONTROLE_ACIONARIO	TP_ENDER	LOGRADOURO	COMPL	BAIRRO	MUN	UF	PAIS	CEP	DDD_TEL	TEL	DDD_FAX	FAX	EMAIL	TP_RESP	RESP	DT_INI_RESP	LOGRADOURO_RESP	COMPL_RESP	BAIRRO_RESP	MUN_RESP	UF_RESP	PAIS_RESP	CEP_RESP	DDD_TEL_RESP	TEL_RESP	DDD_FAX_RESP	FAX_RESP	EMAIL_RESP	CNPJ_AUDITOR	AUDITOR
		sql = "LOAD DATA LOCAL INFILE './CVM/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv' "
				+ " IGNORE INTO TABLE tipo_setor_ativ_cias_abertas CHARACTER SET latin1 "
				+ " FIELDS TERMINATED BY ';' "
				+ " LINES TERMINATED BY '\r\n' "
				+ " IGNORE 1 LINES "
				+ " (@DUMMY, @DUMMY,@DUMMY,@DUMMY,@DUMMY,@DUMMY,@DUMMY,@DUMMY,@DUMMY,@DUMMY,SETOR_ATIV);";
		try {
			st.execute(sql);
			connection.commit();
		} catch (SQLException e) {
			System.out.println("erro: " + e.getMessage() + " / " + e.getSQLState());
		}
		sql = "LOAD DATA LOCAL INFILE \'" + datafilepath + this.datafilename + "\'"
				+ " IGNORE INTO TABLE cnpj_cias_abertas CHARACTER SET latin1"
				+ " FIELDS TERMINATED BY ';'"
				+ " LINES TERMINATED BY '\r\n'"
				+ " IGNORE 1 LINES"
				+ " (CNPJ_CIA, DENOM_SOCIAL, DENOM_COMERC, DT_REG, DT_CONST, DT_CANCEL, @dummy, SIT, DT_INI_SIT, CD_CVM, SETOR_ATIV);";
		try {
			st.execute(sql);
			connection.commit();
		} catch (SQLException e) {
			System.out.println("erro: " + e.getMessage() + " / " + e.getSQLState());
		}

		Long countRecords = 0L;
		Long countRecordsComErro = 0L;
		Long countRecordsRepetidos = 0L;
		Object[] result = {countRecords, countRecordsComErro, countRecordsRepetidos};
		return result;
	}

	private Object[] insere_BP(Connection connection, String bpType) throws SQLException {
		// long inicioProcessamento = System.currentTimeMillis();
		// long tempoProcessamentoParte, tempoProcessamentoTotal, fimProcessamento = inicioProcessamento;
		String sql = "";
		// System.out.println("name "+this.datafilename);
		// System.out.println("path "+this.datafilepath);
		connection.setAutoCommit(false);
		Statement st = connection.createStatement();
		try {
			sql = "LOAD DATA LOCAL INFILE \'" + datafilepath + this.datafilename + "\'"
					+ "\nIGNORE"
					+ "\nINTO TABLE cias_" + bpType + "s"
					+ "\nCHARACTER SET latin1"
					+ "\nFIELDS TERMINATED BY \';\'"
					+ "\nLINES TERMINATED BY \'\\r\\n\'"
					+ "\nIGNORE 1 LINES"
					+ "\n(CNPJ_CIA, DT_REFER, VERSAO, @dummy, CD_CVM, @dummy, MOEDA, @ESCALA_MOEDA, ORDEM_EXERC, DT_FIM_EXERC, CD_CONTA, DS_CONTA, @VL_CONTA, ST_CONTA_FIXA)"
					+ "\nSET ESCALA_MOEDA = \'UNIDADE\', VL_CONTA = IF(@ESCALA_MOEDA = \'MIL\', @VL_CONTA * 1000, @VL_CONTA)";
			st.execute(sql);
			connection.commit();
		} catch (Exception e) {
			e.printStackTrace();
		}
		Long countRecords = 0L;
		Long countRecordsComErro = 0L;
		Long countRecordsRepetidos = 0L;
		Object[] result = {countRecords, countRecordsComErro, countRecordsRepetidos};
		return result;
	}

	private Object[] insere_DRE(Connection connection) throws SQLException {
		// long inicioProcessamento = System.currentTimeMillis();
		// long tempoProcessamentoParte, tempoProcessamentoTotal, fimProcessamento = inicioProcessamento;
		String sql = "";
		// System.out.println("name "+this.datafilename);
		// System.out.println("path "+this.datafilepath);
		connection.setAutoCommit(false);
		Statement st = connection.createStatement();

		sql = "LOAD DATA LOCAL INFILE \'" + datafilepath + this.datafilename + "\'"
				+ "\nIGNORE"
				+ "\nINTO TABLE cias_dres"
				+ "\nCHARACTER SET latin1"
				+ "\nFIELDS TERMINATED BY \';\'"
				+ "\nLINES TERMINATED BY \'\\r\\n\'"
				+ "\nIGNORE 1 LINES"
				+ "\n(CNPJ_CIA, DT_REFER, VERSAO, @dummy, CD_CVM, @dummy, MOEDA, @ESCALA_MOEDA, ORDEM_EXERC, DT_INI_EXERC, DT_FIM_EXERC, CD_CONTA, DS_CONTA, @VL_CONTA, ST_CONTA_FIXA)"
				+ "\nSET ESCALA_MOEDA = \'UNIDADE\', VL_CONTA = IF(@ESCALA_MOEDA = \'MIL\', @VL_CONTA * 1000, @VL_CONTA)";
		st.execute(sql);
		connection.commit();
		Long countRecords = 0L;
		Long countRecordsComErro = 0L;
		Long countRecordsRepetidos = 0L;
		Object[] result = {countRecords, countRecordsComErro, countRecordsRepetidos};
		return result;
	}

	private Object[] insere_DMPL(Connection connection) throws SQLException {
		// long inicioProcessamento = System.currentTimeMillis();
		// long tempoProcessamentoParte, tempoProcessamentoTotal, fimProcessamento = inicioProcessamento;
		String sql = "";
		// System.out.println("name "+this.datafilename);
		// System.out.println("path "+this.datafilepath);
		connection.setAutoCommit(false);
		Statement st = connection.createStatement();

		sql = "LOAD DATA LOCAL INFILE \'" + datafilepath + this.datafilename + "\'"
				+ "\nIGNORE"
				+ "\nINTO TABLE cias_dmpls"
				+ "\nCHARACTER SET latin1"
				+ "\nFIELDS TERMINATED BY \';\'"
				+ "\nLINES TERMINATED BY \'\\r\\n\'"
				+ "\nIGNORE 1 LINES"
				+ "\n(CNPJ_CIA, DT_REFER, VERSAO, @dummy, CD_CVM, @dummy, MOEDA, @ESCALA_MOEDA, ORDEM_EXERC, DT_INI_EXERC, DT_FIM_EXERC, COLUNA_DF, CD_CONTA, DS_CONTA, @VL_CONTA, ST_CONTA_FIXA)"
				+ "\nSET ESCALA_MOEDA = \'UNIDADE\', VL_CONTA = IF(@ESCALA_MOEDA = \'MIL\', @VL_CONTA * 1000, @VL_CONTA)";
		st.execute(sql);
		connection.commit();
		Long countRecords = 0L;
		Long countRecordsComErro = 0L;
		Long countRecordsRepetidos = 0L;
		Object[] result = {countRecords, countRecordsComErro, countRecordsRepetidos};
		return result;
	}

	private Object[] insere_DFC_MD(Connection connection) throws SQLException {
		// long inicioProcessamento = System.currentTimeMillis();
		// long tempoProcessamentoParte, tempoProcessamentoTotal, fimProcessamento = inicioProcessamento;
		String sql = "";
		// System.out.println("name "+this.datafilename);
		// System.out.println("path "+this.datafilepath);
		connection.setAutoCommit(false);
		Statement st = connection.createStatement();

		sql = "LOAD DATA LOCAL INFILE \'" + datafilepath + this.datafilename + "\'"
				+ "\nIGNORE"
				+ "\nINTO TABLE cias_dfcs_mds"
				+ "\nCHARACTER SET latin1"
				+ "\nFIELDS TERMINATED BY \';\'"
				+ "\nLINES TERMINATED BY \'\\r\\n\'"
				+ "\nIGNORE 1 LINES"
				+ "\n(CNPJ_CIA, DT_REFER, VERSAO, @dummy, CD_CVM, @dummy, MOEDA, @ESCALA_MOEDA, ORDEM_EXERC, DT_INI_EXERC, DT_FIM_EXERC, CD_CONTA, DS_CONTA, @VL_CONTA, ST_CONTA_FIXA)"
				+ "\nSET ESCALA_MOEDA = \'UNIDADE\', VL_CONTA = IF(@ESCALA_MOEDA = \'MIL\', @VL_CONTA * 1000, @VL_CONTA)";
		st.execute(sql);
		connection.commit();
		Long countRecords = 0L;
		Long countRecordsComErro = 0L;
		Long countRecordsRepetidos = 0L;
		Object[] result = {countRecords, countRecordsComErro, countRecordsRepetidos};
		return result;
	}

	private Object[] insere_cod_B3(Connection connection) throws SQLException {

		String sql = "";
		// System.out.println("name "+this.datafilename);
		// System.out.println("path "+this.datafilepath);
		connection.setAutoCommit(false);
		Statement st = connection.createStatement();

		try {
			sql = "SET GLOBAL local_infile=1;";
			st.execute(sql);
			connection.commit();
		} catch (SQLException e) {

		}

		sql = "LOAD DATA LOCAL INFILE \'" + datafilepath + this.datafilename + "\'"
				+ "\nIGNORE"
				+ "\nINTO TABLE cias_cods_b3s"
				+ "\nCHARACTER SET latin1"
				+ "\nFIELDS TERMINATED BY \'\\t\'"
				+ "\nLINES TERMINATED BY \'\\n\'"
				+ "\nIGNORE 0 LINES"
				+ "\n(CD_B3, CNPJ_CIA, DENOM_CIA)";
		// System.out.println(sql);
		st.execute(sql);
		connection.commit();

		Long countRecords = 0L;
		Long countRecordsComErro = 0L;
		Long countRecordsRepetidos = 0L;
		Object[] result = {countRecords, countRecordsComErro, countRecordsRepetidos};
		return result;
	}

	private Object[] insere_precos_B3(Connection connection) throws SQLException {

		String sql = "";
		// System.out.println("name "+this.datafilename);
		// System.out.println("path "+this.datafilepath);
		connection.setAutoCommit(false);

		// open file
		File file = new File(this.datafilepath + this.datafilename);
		try {
			FileReader reader = new FileReader(file);
			BufferedReader bReader = new BufferedReader(reader);
			// throw first line away
			String line = bReader.readLine();
			var i = 0;
			while ((line = bReader.readLine()) != null) {
				if (line.substring(0, 2).equals("99")) {
					System.out.format("FINISH! Read %s stocks out of %s lines\n", i, Integer.parseInt(line.substring(31, 42)));
					break;
				}
				// first is Market Code
				String marketCode = line.substring(24, 27);
				// what we want is 10 code which is stock
				if (!marketCode.equals("010")) {
					continue;
				}
				i++;
				// next is the date
				String date = line.substring(2, 6) + "-" + line.substring(6, 8) + "-" + line.substring(8, 10);
				// System.out.format("date %s\n", date);
				// next BDI code
				String bdiCode = line.substring(10, 12);
				// System.out.format("bdiCode %s\n", bdiCode);
				// next Ticker code
				String tickerCode = line.substring(12, 24);
				// company name
				String companyName = line.substring(27, 39);
				// stock specification code (ON, PN...)
				String specCode = line.substring(39, 48);
				// reference currency
				// String currency = line.substring(52, 55);
				// if (!currency.equals(" R$"))
				//     System.out.format("currency %s\n", currency);
				// get price
				Double closePrice = Double.parseDouble(line.substring(108, 119)) + Double.parseDouble("0." + line.substring(119, 121));
				// System.out.format("closePrice %s\n", closePrice);
				sql = String.format("INSERT IGNORE INTO cias_precos_diarios\n"
						+ "(CD_B3, DT_REFER, COD_BDI, DENOM_CIA, SPEC_CODE, CLOSE_PRICE)\n"
						+ "VALUES (\'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%f\')", tickerCode.trim(), date.trim(), bdiCode, companyName.trim(), specCode.trim(), closePrice);
				Statement st = connection.createStatement();
				st.execute(sql);
				if (i % 1e3 == 0) {
					connection.commit();
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		connection.commit();

		Long countRecords = 0L;
		Long countRecordsComErro = 0L;
		Long countRecordsRepetidos = 0L;
		Object[] result = {countRecords, countRecordsComErro, countRecordsRepetidos};
		return result;
	}

	private Object[] processa_CAD(Connection connection) throws SQLException {
		long inicioProcessamento = System.currentTimeMillis();
		long tempoProcessamentoParte, tempoProcessamentoTotal, fimProcessamento = inicioProcessamento;
		BufferedReader lineReader = null;
		Long countRecords = 0L;
		Long countRecordsComErro = 0L;
		Long countRecordsRepetidos = 0L;
		boolean fundoComErrosComDados = false;
		try {
			// Processa arquivos de informação cadastral
			// le arquivo
			//lineReader = new BufferedReader(new FileReader(this.datafilepath + "/" + this.datafilename));
			//lineReader = new BufferedReader(new InputStreamReader(new FileInputStream(this.datafilepath + "/" + this.datafilename), "UTF-8"));
			//System.out.println(Charset.defaultCharset().name() + " => " + Charset.availableCharsets().toString());
			lineReader = Files.newBufferedReader(Paths.get(this.datafilepath + "/" + this.datafilename), Charset.forName("ISO-8859-1"));
			String lineText = lineReader.readLine(); // le primeira linha
			String[] header = lineText.split(";"); // que é o header
			int numFields = header.length;
			HashMap<String, Integer> fields = new HashMap<>();
			for (int i = 0; i < numFields; i++) {
				fields.put(header[i], i);
			}
			String dataArquivo = this.datafilename.replace("inf_cadastral_fi_", "").replace(".csv", ""); // pega apenas os numeros num nome de arquivo como: inf_cadastral_fi_20200626.csv
			dataArquivo = dataArquivo.substring(0, 4) + "-" + dataArquivo.substring(4, 6) + "-" + dataArquivo.substring(6, 8);
			String sql;
			Statement st = (Statement) connection.createStatement();
			java.sql.ResultSet rs;
			long bytesReadedSoFar = 0;
			String CNPJ_FUNDO, DENOM_SOCIAL, situacao, CLASSE, rentabilidade, DT_CANCEL, CNPJ_ADMIN, CNPJ_GESTOR, NOME, DT_INI_CLASSE, DT_INI_ATIV;
			Integer ID_FUNDO, ID_SIT, ID_CLASSE, ID_RENTAB, CONDOM_ABERTO, FUNDO_COTAS, FUNDO_EXCLUSIVO, INVEST_QUALIF, ID_ADMIN = null, ID_GESTOR = null;
			while ((lineText = lineReader.readLine()) != null) {
				bytesReadedSoFar += lineText.length();
				lineText = lineText.replace("\"", "'") + " ";
				lineText = lineText.replace(";;", "; ;");
				String[] record = lineText.split(";");
				if (record.length < numFields) {
					System.out.println("erro: O registro " + countRecords + " possui apenas " + record.length + " campos, enquanto o cabeçalho especifica " + numFields + " campos");
				}
				CNPJ_FUNDO = record[fields.get("CNPJ_FUNDO")].strip();
				ID_FUNDO = CVMDatabaseThread.tb_cad_cnpj_fundo.get(CNPJ_FUNDO); // procura no map em memória
				if (ID_FUNDO == null) {
					// nova informação cadastral de fundo. Busca infos do fundo no BD
					sql = "select id from cnpj_fundos USE INDEX(idx_cnpj) where cnpj=\"" + CNPJ_FUNDO + "\"";
					rs = st.executeQuery(sql);
					if (!rs.next()) {
						// fundo não existe nem no BD. Inclui e depois pega id
						DENOM_SOCIAL = record[fields.get("DENOM_SOCIAL")].strip();
						sql = "insert into cnpj_fundos (cnpj,  DENOM_SOCIAL, DT_REG_CVM) values(\"" + CNPJ_FUNDO + "\",\"" + DENOM_SOCIAL + "\",\"" + dataArquivo + "\")";
						try {
							st.execute(sql);
						} catch (Exception ex) {
							if (ex.getMessage().contains("Duplicate")) {
								countRecordsRepetidos++;
							} else {
								countRecordsComErro++;
								// verifica se por acaso esse fundo com erro não é um fundo que posteriormente possui docs e que não foi cadastrado (por causa do erro que acabou de ocorrer
								sql = "select id from cnpj_fundos_nao_cadastrados USE INDEX(idx_cnpj) where cnpj=\"" + CNPJ_FUNDO + "\"";
								rs = st.executeQuery(sql);
								if (rs.next()) {
									// é um fundo que possui dados mas que não foi cadastrado por aqlgum problema neste cadastro
									fundoComErrosComDados = true;
								}
							}
						}
						connection.commit();
						sql = "select id from cnpj_fundos USE INDEX(idx_cnpj) where cnpj=\"" + CNPJ_FUNDO + "\"";
						rs = st.executeQuery(sql);
						rs.next(); // deve haver um registro retornado
					}
					ID_FUNDO = rs.getInt("id");
					CVMDatabaseThread.tb_cad_cnpj_fundo.put(CNPJ_FUNDO, ID_FUNDO); // adiciona ao map
				}
				// aqui o id do fundo é conhecido e está no map e no BD

				situacao = record[fields.get("SIT")].strip();
				ID_SIT = CVMDatabaseThread.tb_tipo_situacao_fundos.get(situacao);
				if (ID_SIT == null) {
					// nova informação cadastral de fundo. Busca infos do fundo no BD
					sql = "select id from tipo_situacao_fundos  where situacao=\"" + situacao + "\"";
					rs = st.executeQuery(sql);
					if (!rs.next()) {
						// situação não existe nem no BD. Inclui e depois pega id
						sql = "insert into tipo_situacao_fundos (situacao) values(\"" + situacao + "\")";
						try {
							st.execute(sql);
						} catch (Exception ex) {
						}
						connection.commit();
						sql = "select id from tipo_situacao_fundos where situacao=\"" + situacao + "\"";
						rs = st.executeQuery(sql);
						rs.next(); // deve haver um registro retornado
					}
					ID_SIT = rs.getInt("id");
					CVMDatabaseThread.tb_tipo_situacao_fundos.put(situacao, ID_SIT); // adiciona ao map
				}
				// aqui o ID_SIT é conhecido e está no map e no BD

				CLASSE = record[fields.get("CLASSE")].strip();
				ID_CLASSE = CVMDatabaseThread.tb_tipo_classe_fundos.get(CLASSE);
				if (ID_CLASSE == null) {
					// nova informação cadastral de fundo. Busca infos do fundo no BD
					sql = "select id from tipo_classe_fundos where CLASSE=\"" + CLASSE + "\"";
					rs = st.executeQuery(sql);
					if (!rs.next()) {
						// situação não existe nem no BD. Inclui e depois pega id
						sql = "insert into tipo_classe_fundos (CLASSE) values(\"" + CLASSE + "\")";
						try {
							st.execute(sql);
						} catch (Exception ex) {
						}
						connection.commit();
						sql = "select id from tipo_classe_fundos where CLASSE=\"" + CLASSE + "\"";
						rs = st.executeQuery(sql);
						rs.next(); // deve haver um registro retornado
					}
					ID_CLASSE = rs.getInt("id");
					CVMDatabaseThread.tb_tipo_classe_fundos.put(CLASSE, ID_CLASSE); // adiciona ao map
				}
				// aqui o ID_CLASSE é conhecido e está no map e no BD

				rentabilidade = record[fields.get("RENTAB_FUNDO")].strip();
				ID_RENTAB = CVMDatabaseThread.tb_tipo_rentabilidade_fundos.get(rentabilidade);
				if (ID_RENTAB == null) {
					// nova informação cadastral de fundo. Busca infos do fundo no BD
					sql = "select id from tipo_rentabilidade_fundos where rentabilidade=\"" + rentabilidade + "\"";
					rs = st.executeQuery(sql);
					if (!rs.next()) {
						// situação não existe nem no BD. Inclui e depois pega id
						sql = "insert into tipo_rentabilidade_fundos (rentabilidade) values(\"" + rentabilidade + "\")";
						try {
							st.execute(sql);
						} catch (Exception ex) {
						}
						connection.commit();
						sql = "select id from tipo_rentabilidade_fundos where rentabilidade=\"" + rentabilidade + "\"";
						rs = st.executeQuery(sql);
						rs.next(); // deve haver um registro retornado
					}
					ID_RENTAB = rs.getInt("id");
					CVMDatabaseThread.tb_tipo_rentabilidade_fundos.put(rentabilidade, ID_RENTAB); // adiciona ao map
				}
				// aqui o ID_RENTAB é conhecido e está no map e no BD

				// cadastra situação do fundo
				sql = "insert into situacao_fundos (cnpj_fundo_id, tipo_situacao_fundo_id, DT_INI_SIT, DT_REG_CVM) values (" + ID_FUNDO + ", " + ID_SIT + ", \"" + record[fields.get("DT_INI_SIT")] + "\", \"" + dataArquivo + "\")";
				try {
					st.execute(sql);
				} catch (Exception ex) { // sql pode falhar por haver linhas com informações duplicadas no CSV, e falha no unique da tabela. Realmente não deve inserir novamente
					//countRecordsRepetidos++;
				}
				// cadastra data de cancelamento, se existir //TODO: VERIFICAR ISSO
				DT_CANCEL = record[fields.get("DT_CANCEL")].strip();
				if (!DT_CANCEL.isEmpty()) {
					sql = "insert into cancelamento_fundos (cnpj_fundo_id, DT_CANCEL, DT_REG_CVM) values (" + ID_FUNDO + ", \"" + DT_CANCEL + "\", \"" + dataArquivo + "\")";
					try {
						st.execute(sql);
					} catch (Exception ex) {
					}
				}

				// cadastra informações básicas
				CONDOM_ABERTO = record[fields.get("CONDOM")].equals("Aberto") ? 1 : 0;
				FUNDO_COTAS = record[fields.get("FUNDO_COTAS")].equals("S") ? 1 : 0;
				FUNDO_EXCLUSIVO = record[fields.get("FUNDO_EXCLUSIVO")].equals("S") ? 1 : 0;
				INVEST_QUALIF = record[fields.get("INVEST_QUALIF")].equals("S") ? 1 : 0;

				CNPJ_ADMIN = record[fields.get("CNPJ_ADMIN")].strip();
				if (!CNPJ_ADMIN.isEmpty()) {
					ID_ADMIN = CVMDatabaseThread.tb_administrador_fundos.get(CNPJ_ADMIN); // procura no map em memória
					if (ID_ADMIN == null) {
						// nova informação cadastral de fundo. Busca infos do fundo no BD
						sql = "select id from administrador_fundos USE INDEX(idx_cnpj) where cnpj=\"" + CNPJ_ADMIN + "\"";
						rs = st.executeQuery(sql);
						if (!rs.next()) {
							// fundo não existe nem no BD. Inclui e depois pega id
							NOME = record[fields.get("ADMIN")].strip();
							sql = "insert into administrador_fundos (cnpj, nome, DT_REG_CVM) values(\"" + CNPJ_ADMIN + "\",\"" + NOME + "\",\"" + dataArquivo + "\")";
							try {
								st.execute(sql);
							} catch (Exception ex) {
								if (!ex.getMessage().contains("Duplicate")) {
									System.out.println("erro:   Erro '" + ex.getMessage() + "' ao inserir administrador. Query: " + sql);
								}
							}
							connection.commit();
							sql = "select id from administrador_fundos USE INDEX(idx_cnpj) where cnpj=\"" + CNPJ_ADMIN + "\"";
							rs = st.executeQuery(sql);
							rs.next(); // deve haver um registro retornado
						}
						ID_ADMIN = rs.getInt("id");
						CVMDatabaseThread.tb_administrador_fundos.put(CNPJ_ADMIN, ID_ADMIN); // adiciona ao map
					}
					// aqui o id do admin é conhecido e está no map e no BD
					/* sql = "insert INTO FUNDO_ADMIN (cnpj_fundo_id, FK_ID_ADMIN, DATA_REG_CVM) values (" + ID_FUNDO + ", " + ID_ADMIN + ",\"" + dataArquivo + "\")";
					try {
					st.execute(sql);
					} catch (Exception ex) {// sql pode falhar por haver linhas com informações duplicadas no CSV.
					//System.out.println("Erro " + ex.getMessage());
					} */
				}
				// aqui o id do admin é conhecido e está no map e no BD

				CNPJ_GESTOR = record[fields.get("CPF_CNPJ_GESTOR")].strip();
				if (!CNPJ_GESTOR.isEmpty()) {
					ID_GESTOR = CVMDatabaseThread.tb_gestor_fundos.get(CNPJ_GESTOR); // procura no map em memória
					if (ID_GESTOR == null) {
						// nova informação cadastral de fundo. Busca infos do fundo no BD
						sql = "select id from gestor_fundos USE INDEX(idx_cnpj) where cnpj=\"" + CNPJ_GESTOR + "\"";
						rs = st.executeQuery(sql);
						if (!rs.next()) {
							// fundo não existe nem no BD. Inclui e depois pega id
							NOME = record[fields.get("GESTOR")].strip();
							sql = "insert into gestor_fundos (cnpj, nome, DT_REG_CVM) values(\"" + CNPJ_GESTOR + "\",\"" + NOME + "\",\"" + dataArquivo + "\")";
							try {
								st.execute(sql);
							} catch (SQLException ex) {
								if (!ex.getMessage().contains("Duplicate")) {
									System.out.println("erro:   Erro '" + ex.getMessage() + "' ao inserir gestor. Query: " + sql);
								}
							}
							connection.commit();
							sql = "select id from gestor_fundos USE INDEX(idx_cnpj) where cnpj=\"" + CNPJ_GESTOR + "\"";
							rs = st.executeQuery(sql);
							rs.next(); // deve haver um registro retornado
						}
						ID_GESTOR = rs.getInt("id");
						CVMDatabaseThread.tb_gestor_fundos.put(CNPJ_GESTOR, ID_GESTOR); // adiciona ao map
					}
					// aqui o id do gestor é conhecido e está no map e no BD
					/* sql = "insert INTO FUNDO_GESTOR (cnpj_fundo_id, FK_ID_GESTOR, DATA_REG_CVM) values (" + ID_FUNDO + ", " + ID_GESTOR + ",\"" + dataArquivo + "\")";
					try {
					st.execute(sql);
					} catch (Exception ex) {// sql pode falhar por haver linhas com informações duplicadas no CSV.
					//System.out.println("Erro " + ex.getMessage());
					}*/
				}

				DT_INI_CLASSE = record[fields.get("DT_INI_CLASSE")].strip();
				if (DT_INI_CLASSE.isEmpty()) {
					DT_INI_CLASSE = record[fields.get("DT_INI_ATIV")].strip();
					if (DT_INI_CLASSE.isEmpty()) {
						DT_INI_CLASSE = record[fields.get("DT_CANCEL")].strip();
					}
				}

				sql = "INSERT INTO cadastro_fundos "
						+ "(cnpj_fundo_id, DT_CONST, tipo_classe_fundo_id, DT_INI_CLASSE, tipo_rentabilidade_fundo_id, CONDOM_ABERTO, FUNDO_COTAS, FUNDO_EXCLUSIVO, INVEST_QUALIF, administrador_fundo_id, gestor_fundo_id, DT_REG_CVM) "
						+ " VALUES (" + ID_FUNDO + ", \"" + record[fields.get("DT_CONST")].strip() + "\", " + ID_CLASSE + ", \"" + DT_INI_CLASSE + "\", " + ID_RENTAB + ", " + CONDOM_ABERTO + ", " + FUNDO_COTAS + ", " + FUNDO_EXCLUSIVO + ", " + INVEST_QUALIF + ", " + ID_ADMIN + ", " + ID_GESTOR + ", \"" + dataArquivo + "\"); ";
				try {
					st.execute(sql);
				} catch (Exception ex) {// sql pode falhar por haver linhas com informações duplicadas no CSV.
					if (ex.getMessage().contains("Duplicate")) {
						countRecordsRepetidos++;
						// TODO RETIRAR ISSO DEPOIS DE PROCESSAR NOVAMENTE OS ARQUIVOS DE INF_CADASTRAL
						//sql = "UPDATE cadastro_fundos set administrador_fundo_id=" + ID_ADMIN + ", gestor_fundo_id=" + ID_GESTOR + " WHERE cnpj_fundo_id=" + ID_FUNDO;
						//try {
						//	st.execute(sql);
						//} catch (SQLException e) {
						//	System.out.println("Não pude atualizar o registro de admin e gestor: " + e.getMessage());
						//}
					} else {
						//countRecordsComErro++;
						System.out.println("erro: Registro " + countRecords + " possui o erro: " + ex.getMessage() + " ; Query: " + sql);
						actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.FILE_PROCESSED.toString(), datafilepath, datafilename, countRecords, Boolean.TRUE, null, new Date(), Boolean.FALSE));
					}
				}

				// fim do processamento do registro
				countRecords++;
				if (countRecords % 1e3 == 0) {
					connection.commit();
					if (countRecords % 1e4 == 0) {
						tempoProcessamentoParte = (System.currentTimeMillis() - fimProcessamento);
						tempoProcessamentoTotal = (System.currentTimeMillis() - inicioProcessamento);
						System.out.println("info:      [" + this.csvThreadID + "] " + countRecords / 1e3 + "K registros processados até o momento, sendo " + countRecordsComErro + " com erro e " + countRecordsRepetidos + " preexistentes (" + (int) (100 * bytesReadedSoFar / datafilelength) + "% em " + (int) (tempoProcessamentoParte / 1e3) + "s, totalizando " + (int) (tempoProcessamentoTotal / 1e3) + " s)");
						fimProcessamento = System.currentTimeMillis();
					}
				}
			}
			st.close();
		} catch (FileNotFoundException ex) {
			Logger.getLogger(CVMDatabaseThread.class
					.getName()).log(Level.SEVERE, null, ex);
		} catch (IOException ex) {
			Logger.getLogger(CVMDatabaseThread.class
					.getName()).log(Level.SEVERE, null, ex);
		} finally {
			try {
				lineReader.close();
			} catch (IOException ex) {
				Logger.getLogger(CVMDatabaseThread.class
						.getName()).log(Level.SEVERE, null, ex);
			}
		}
		Object[] result = {countRecords, countRecordsComErro, countRecordsRepetidos};
		return result;

	}

	private Object[] calcula_indicadores_DOC_INF_DIARIO(Connection connection) {
		long inicioProcessamento = System.currentTimeMillis();
		long tempoProcessamentoParte, tempoProcessamentoTotal=0, fimProcessamento = inicioProcessamento;
		Long countRecords = 0L;
		Long countRecordsComErro = 0L;
		Long countRecordsRepetidos = 0L;
		try {
			//
			int idFundo = Integer.valueOf(this.datafilename);
			String sql;
			Statement st = connection.createStatement();
			Statement st2 = connection.createStatement();
			java.sql.ResultSet rs;
			java.sql.ResultSet rs2;
			//if (idFundo == 561) {
			//	System.err.println("AQUI");
			//}

			while (countRecordsRepetidos == 0L) { // calcula indicadores diários para todos os intervalos de datas em que eles estiverem vazios
				//
				// completa indicadores diários para qualquer período que eles ainda estejam vazios
				sql = "select VL_QUOTA, DT_COMPTC from doc_inf_diario_fundos where cnpj_fundo_id=" + idFundo + " and (rentab_diaria is null or volat_diaria is null) order by DT_COMPTC ASC"; // registros sem rentabilidade, para calcular
				rs = st.executeQuery(sql);
				if (rs.next()) {
					Double valorQuotaAnterior, rentabAcumulAnterior, valorQuotaAtual, rentabilidade, volatilidade, rentabilidadeAcumulada, maxValorQuota, drawdown, temp;//, somaRent;
					Double rent1 = Double.NaN, rent2 = Double.NaN, rent3 = Double.NaN;
					String dataAtual;
					String firstEmptyDate = rs.getDate("DT_COMPTC").toString(); // primeira data sem rentabilidade calculada. Tenta buscar a última rentabilidade calculada antes dessa
					sql = "select VL_QUOTA, rentab_acumulada from doc_inf_diario_fundos where cnpj_fundo_id=" + idFundo + " and (rentab_diaria is not null and volat_diaria is not null) and DT_COMPTC<'" + firstEmptyDate + "' order by DT_COMPTC DESC";
					rs2 = st2.executeQuery(sql);
					if (rs2.next()) { // há valores anteriores à faixa de valores vazios. Pega o último valor de cota conhecido antes disso
						valorQuotaAnterior = rs2.getDouble("VL_QUOTA");
						rentabAcumulAnterior = rs2.getDouble("rentab_acumulada");
						sql = "select Max(VL_QUOTA) as MAX_VL_QUOTA from doc_inf_diario_fundos where cnpj_fundo_id=" + idFundo + " and DT_COMPTC<'" + firstEmptyDate + "' order by DT_COMPTC DESC";
						rs2 = st2.executeQuery(sql);
						if (rs2.next()) {
							maxValorQuota = rs2.getDouble("MAX_VL_QUOTA");
						} else {
							// nunc deveria acontecer
							maxValorQuota = 0.0; //?????
							System.out.println("ERRO: MAX_VL_QUOTA não encontrado. Maior valor de quota = 0");
						}
					} else {
						// não há valores anteriores. Então o valor anterior deve ser o primeiro valor da quota no período sem valores calculados
						valorQuotaAnterior = rs.getDouble("VL_QUOTA");
						rentabAcumulAnterior = 0.0;
						maxValorQuota = valorQuotaAnterior;
					}
					// tendo um valor anterior, varre todo o período calculando a rentabilidade
					do {
						// calcula rentabilidades
						valorQuotaAtual = rs.getDouble("VL_QUOTA");
						dataAtual = rs.getDate("DT_COMPTC").toString();
						if (valorQuotaAnterior != 0.0) {
							rentabilidade = (valorQuotaAtual - valorQuotaAnterior) / valorQuotaAnterior;
							if (rentabilidade.isNaN() || rentabilidade.isInfinite()) {
								rentabilidade = 0.0;
							}
						} else {
							rentabilidade = 0.0;
						}
						rentabilidadeAcumulada = rentabAcumulAnterior + rentabilidade;
						// calcula volatilidade
						rent1 = rent2;
						rent2 = rent3;
						rent3 = rentabilidade;
						if (!rent1.isNaN()) {
							temp = (rent1 + rent2 + rent3) / 3; // TODO: Conferir: a volatilidade diária é calculada com base numa média dos últimos 3 dias.
							volatilidade = Math.sqrt((Math.pow(rent1 - temp, 2) + Math.pow(rent2 - temp, 2) + Math.pow(rent3 - temp, 2)) / 2);//2=n-1
						} else {
							volatilidade = 0.0;
						}
						//
						// calcula drawdown
						if (valorQuotaAtual > maxValorQuota) {
							maxValorQuota = valorQuotaAtual;
						}
						if (valorQuotaAtual < maxValorQuota) {
							drawdown = valorQuotaAtual / maxValorQuota - 1.0;
						} else {
							drawdown = 0.0;
						}
						// salva no BD
						sql = "update doc_inf_diario_fundos set rentab_diaria=" + rentabilidade + ", rentab_acumulada=" + rentabilidadeAcumulada + ", volat_diaria=" + volatilidade + ", drawdown=" + drawdown + " where cnpj_fundo_id=" + idFundo + " and DT_COMPTC='" + dataAtual + "'";
						st2.execute(sql);
						// TODO check por substituir as linhas anteriores por
						//rs.updateDouble("rentab_diaria", rentabilidade);
						//
						countRecords++;
						if (countRecords % 1e2 == 0) {
							connection.commit();
							if (countRecords % 1e3 == 0) {
								tempoProcessamentoParte = (System.currentTimeMillis() - fimProcessamento);
								tempoProcessamentoTotal = (System.currentTimeMillis() - inicioProcessamento);
								System.out.println("info:      [" + this.csvThreadID + "] " + countRecords / 1e3 + "K registros processados até o momento (totalizando " + (int) (tempoProcessamentoTotal) + " ms)");//, sendo " + countRecordsComErro + " com erro e " + countRecordsRepetidos + " preexistentes");
								fimProcessamento = System.currentTimeMillis();
							}
						}
						// atualiza vars
						valorQuotaAnterior = valorQuotaAtual;
						rentabAcumulAnterior = rentabilidadeAcumulada;
					} while (rs.next());
					connection.commit();
					//
				} else {// não há valores vazios a serem calculados nos indicadores diários. Estão todos calculados e prontos. Não há o que fazer (ou simplesmente não há qualquer registro nos informativos diários
					countRecordsRepetidos++;
				}
			}

			//
			// Calcula rentabilidades e outros indicadores em períodos mensais
			//
			sql = "select Max(DT_COMPTC) as UltimaDataComDados, Count(cnpj_fundo_id) as numRegistros from doc_inf_diario_fundos where cnpj_fundo_id=" + idFundo;
			rs = st.executeQuery(sql);
			if (rs.next()) {
				if (rs.getLong("numRegistros") == 0L) {
					// há um fundo cadastrado sem qualquer informação diária
					// TODO: Registrar isso!!!
				} else {
					LocalDate ultimaData = rs.getDate("UltimaDataComDados").toLocalDate(); // com dados diários obtidos da CVM
					long mesesNoPassadoParaUltimaData = 0;
					//
					// Calcula indicadores para 1, 2, 3, 6, 12, 24, ... meses no passado, todos terminando no mês passado (em relação a hoje), ou a 2 meses no passado, 3, 4, até terminando 12*5 meses no passado
					while (mesesNoPassadoParaUltimaData < 12 * 2) {
						ultimaData = ultimaData.minusMonths(1L);
						String primeiroDiaAposUltimoMesCompleto = "0" + String.valueOf(ultimaData.getMonthValue());
						primeiroDiaAposUltimoMesCompleto = String.valueOf(ultimaData.getYear()) + "-" + primeiroDiaAposUltimoMesCompleto.substring(primeiroDiaAposUltimoMesCompleto.length() - 2) + "-01";
						sql = "select VL_QUOTA, DT_COMPTC, rentab_diaria from doc_inf_diario_fundos where cnpj_fundo_id=" + idFundo + " and DT_COMPTC<'" + primeiroDiaAposUltimoMesCompleto + "' order by DT_COMPTC DESC"; //calcula indicadores do último dia para trás, até o limite máximo de meses definido abaixo
						rs = st.executeQuery(sql);
						if (rs.next()) {
							String dataFinal = rs.getDate("DT_COMPTC").toString();
							// verifica se já existe indicadores do fundo no último mês
							sql = "select cnpj_fundo_id from indicadores_fundos where cnpj_fundo_id=" + idFundo + " and periodo_meses=1 and data_final='" + dataFinal + "'";
							rs2 = st2.executeQuery(sql);
							if (!rs2.next()) { // se não há nada de um mês, não deve haver os períodos mais longos também (terminando do último mês); então calcula tudo. Se há, não faz nada
								LocalDate primeiraDataPeriodo, ultimaDataPeriodo;
								ultimaDataPeriodo = rs.getDate("DT_COMPTC").toLocalDate();
								//System.out.println("Indicadores de 1 mês terminando em " + ultimaDataPeriodo);
								primeiraDataPeriodo = ultimaDataPeriodo;
								Month mesAnterior = ultimaDataPeriodo.getMonth();
								Month mesAtual;
								Double rentabilidade, sumRentabilidade = 0.0, minRentabilidade = 1e9, maxRentabilidade = -1e9;
								ArrayList<Double> rentabilidades = new ArrayList<>();
								ArrayList<Double> quotas = new ArrayList<>();
								Integer numValores = 0, numMeses = 0;
								Double rentabilidadeMedia = 0.0;
								Double desvioPadrao = 0.0;
								Double maxDrawdown = 0.0;
								do {
									mesAtual = rs.getDate("DT_COMPTC").toLocalDate().getMonth();
									if (mesAtual.equals(mesAnterior)) {
										// acumula
										primeiraDataPeriodo = rs.getDate("DT_COMPTC").toLocalDate();
										numValores++;
										quotas.add(0, rs.getDouble("VL_QUOTA")); // inclui em ordem cronológica para posterior cálculo do MaxDrawdown
										rentabilidade = rs.getDouble("rentab_diaria");
										rentabilidades.add(rentabilidade);
										sumRentabilidade += rentabilidade;
										if (rentabilidade < minRentabilidade) {
											minRentabilidade = rentabilidade;
										}
										if (rentabilidade > maxRentabilidade) {
											maxRentabilidade = rentabilidade;
										}
									} else {
										// calcula indicadores do mês que terminou (e de todos os outros meses que já passou, de forma acumulada)
										mesAnterior = mesAtual;
										numMeses++;
										rentabilidadeMedia = sumRentabilidade / numValores;
										Double sumDesvioPadrao = 0.0;
										for (Double rent : rentabilidades) {
											sumDesvioPadrao += Math.pow(rent - rentabilidadeMedia, 2);
										}
										if (numValores > 1) {//pois n-1 == 0
											desvioPadrao = Math.sqrt(sumDesvioPadrao / (numValores - 1));
										} else {
											desvioPadrao = 0.0;
										}
										Double maxQuota = -1e9, drawDown;
										maxDrawdown = 0.0;  // TODO: MaxQuota é muito negativa pois pode haver quotas negativas (check this out)
										for (Double quota : quotas) {
											if (quota > maxQuota) {
												maxQuota = quota;
											} else {
												if (maxQuota == 0.0) {
													drawDown = 0.0;
												} else {
													drawDown = quota / maxQuota - 1.0;
												}
												if (drawDown < maxDrawdown) {
													maxDrawdown = drawDown;
												}
											}
										}
										if (numMeses == 1 || numMeses == 2 || numMeses == 3 || numMeses == 6 || numMeses % 12 == 0) {
											// salva no BD
											sql = "INSERT INTO indicadores_fundos (cnpj_fundo_id, periodo_meses, data_inicial, data_final, rentabilidade, desvio_padrao, num_valores, rentab_min, rentab_max, max_drawdown, meses_acima_bench, sharpe, sharpe_geral_bench, sharpe_geral_classe, beta) VALUES (" + idFundo + "," + numMeses + ",'" + primeiraDataPeriodo + "', '" + ultimaDataPeriodo + "'," + rentabilidadeMedia + "," + desvioPadrao + "," + numValores + "," + minRentabilidade + "," + maxRentabilidade + "," + maxDrawdown + ", null, null, null, null, null)";
											try {
												st2.execute(sql);
												countRecords++;
												if (numMeses % 12 == 0) {
													connection.commit();
													tempoProcessamentoParte = (System.currentTimeMillis() - fimProcessamento);
													tempoProcessamentoTotal = (System.currentTimeMillis() - inicioProcessamento);
													//System.out.println("info:      [" + this.csvThreadID + "] " + numMeses + " meses processados até o momento (totalizando " + (int) (tempoProcessamentoTotal) + " ms)");//, sendo " + countRecordsComErro + " com erro e " + countRecordsRepetidos + " preexistentes");
													fimProcessamento = System.currentTimeMillis();
												}
											} catch (SQLException ex) {
												Logger.getLogger(CVMDatabaseThread.class.getName()).log(Level.SEVERE, null, ex);
												System.out.println("ERRO: " + ex.getMessage());
												countRecordsRepetidos++;
											}
										}
									}
								} while (rs.next()); // && numMeses <= 60);
								// foi até o começo do funcionamento do fundo, mas os primeiros meses que não formam um ano completo a partir do mês atual, não foram gravados no BD
								// TODO: // deve gravar os primeiros meses para ter um registro do período máximo do fundo (desde que haja pelo menos 1 mes de dados)
								// salva no BD
								if (numMeses > 0) {
									sql = "INSERT INTO indicadores_fundos (cnpj_fundo_id, periodo_meses, data_inicial, data_final, rentabilidade, desvio_padrao, num_valores, rentab_min, rentab_max, max_drawdown, meses_acima_bench, sharpe, sharpe_geral_bench, sharpe_geral_classe, beta) VALUES (" + idFundo + "," + numMeses + ",'" + primeiraDataPeriodo + "', '" + ultimaDataPeriodo + "'," + rentabilidadeMedia + "," + desvioPadrao + "," + numValores + "," + minRentabilidade + "," + maxRentabilidade + "," + maxDrawdown + ", null, null, null, null, null)";
									try {
										st2.execute(sql);
										countRecords++;
										if (numMeses % 12 == 0) {
											connection.commit();
											tempoProcessamentoParte = (System.currentTimeMillis() - fimProcessamento);
											tempoProcessamentoTotal = (System.currentTimeMillis() - inicioProcessamento);
											//System.out.println("info:      [" + this.csvThreadID + "] " + numMeses + " meses processados até o momento (totalizando " + (int) (tempoProcessamentoTotal) + " ms)");//, sendo " + countRecordsComErro + " com erro e " + countRecordsRepetidos + " preexistentes");
											fimProcessamento = System.currentTimeMillis();
										}
									} catch (SQLException ex) {
										//Logger.getLogger(CVMDatabaseThread.class.getName()).log(Level.SEVERE, null, ex);
										countRecordsRepetidos++;
									}
									connection.commit();
								}
								System.out.println("info:      [" + this.csvThreadID + "] " + numMeses + " meses processados (totalizando " + (int) (tempoProcessamentoTotal) + " ms)");//, sendo " + countRecordsComErro + " com erro e " + countRecordsRepetidos + " preexistentes");

							}
						}
						mesesNoPassadoParaUltimaData++;
					}

				}
			}
		} catch (SQLException ex) {
			Logger.getLogger(CVMDatabaseThread.class.getName()).log(Level.SEVERE, null, ex);
			countRecordsComErro++;
		}
		Object[] result = {countRecords, countRecordsComErro, countRecordsRepetidos};
		return result;
	}

	/**
	 *
	 */
	@Override
	public void doRun() {
		String sql = "";
		countErrosRegistros = 0;
		long inicioProcessamento = System.currentTimeMillis();
		long tempoProcessamentoTotal, fimProcessamento;
		System.out.println("ação: [" + this.csvThreadID + "] Thread começou a executar o processamento do arquivo \"" + datafilename + "\"");
		Connection connection = null;
		long countRecords = 0L;
		long countRecordsComErro = 0L;
		long countRecordsRepetidos = 0L;
		try {
			// init BD
			Class.forName(JDBC_DRIVER);
			connection = DriverManager.getConnection(DB_URL + databasename, databaseUsername, databasePassword);
			connection.setAutoCommit(false);

			Object[] result = null;
			String action = ActionResultLogger.Action.FILE_PROCESSED.toString();
			boolean saveAction = true;
			switch (this.localCVMSubdir) {
				case "FI/CAD/DADOS/":
					// processa informações de cadastro (
					result = processa_CAD(connection);
					break;
				case "FI/DOC/EXTRATO/DADOS/":
					// processa informações de cadastro (
					result = processa_DOC_EXTRATO(connection);
					break;
				case "FI/DOC/COMPL/DADOS/":
					// processa informações de cadastro (
					result = processa_DOC_COMPL(connection);
					break;
				case "FI/DOC/INF_DIARIO/DADOS/HIST/":
					// processa informações de cadastro (
					result = processa_DOC_INF_DIARIO(connection);
					break;
				case "FI/DOC/INF_DIARIO/DADOS/":
					// processa informações de cadastro (
					result = processa_DOC_INF_DIARIO(connection);
					break;
				case "INTERMEDIARIO/CAD/DADOS":
					// processa informações de cadastro (
					result = processa_INTERMEDIARIO_CAD(connection);
					break;
				case "table:doc_inf_diario_fundos":
					// processa informações de cadastro (
					result = calcula_indicadores_DOC_INF_DIARIO(connection);
					action = ActionResultLogger.Action.INDICADORS_CALCULATED.toString();
					saveAction = false;
					break;
				// processa info de ações
				case "CIA_ABERTA/CAD/DADOS/":
					result = insere_cias_meta(connection);
					action = ActionResultLogger.Action.INSERT_META_CIA_INTO_DB.toString();
					break;
				// dealing with data from ITR, which can be DMPL and DRE
				case "CIA_ABERTA/DOC/ITR/DADOS/":
					if (this.datafilename.contains("BPA")) {
						result = this.insere_BP(connection, "bpa");
						action = ActionResultLogger.Action.INSERT_BPA_INTO_DB.toString();
					} else if (this.datafilename.contains("DRE")) {
						result = this.insere_DRE(connection);
						action = ActionResultLogger.Action.INSERT_DRE_INTO_DB.toString();
					} else if (this.datafilename.contains("DMPL")) {
						result = this.insere_DMPL(connection);
						action = ActionResultLogger.Action.INSERT_DMPL_INTO_DB.toString();
					} else if (this.datafilename.contains("DFC_MD")) {
						result = this.insere_DFC_MD(connection);
						action = ActionResultLogger.Action.INSERT_DFC_MD_INTO_DB.toString();
					} else if (this.datafilename.contains("BPP")) {
						result = this.insere_BP(connection, "bpp");
						action = ActionResultLogger.Action.INSERT_BPP_INTO_DB.toString();
					} else {
						System.out.println("DONT KNOW WHAT TO DO WITH: " + this.datafilename);
					}
					break;
				case "B3/":
					result = insere_cod_B3(connection);
					action = ActionResultLogger.Action.INSERT_B3_CODES_INTO_DB.toString();
					break;
				case "CIA_ABERTA/DOC/DFP/B3/PRICES/":
					result = insere_precos_B3(connection);
					action = ActionResultLogger.Action.INSERT_B3_PRICES_INTO_DB.toString();
					break;
				default:
					System.out.println("info: [" + this.csvThreadID + "] Sem especificção de como processar o arquivo \"" + datafilename + "\"");
					break;
			}
			// get results
			countRecords = (long) result[0];
			countRecordsComErro = (long) result[1];
			countRecordsRepetidos = (long) result[2];

			connection.close();
			fimProcessamento = System.currentTimeMillis();
			tempoProcessamentoTotal = (fimProcessamento - inicioProcessamento);
			CVMDatabaseThread.numTotalThreads++;
			CVMDatabaseThread.tempoTotalThreads += tempoProcessamentoTotal / 1e3;
			System.out.println("info:      [" + this.csvThreadID + "] " + countRecords + " registros inseridos com sucesso, " + countRecordsComErro + " com erro e " + countRecordsRepetidos + " preexistentes. Processamento do arquivo " + datafilename + " concluído em " + (int) (tempoProcessamentoTotal / 1e3) + " s");
			if (saveAction) {
				actionResultLogger.recordAction(new ActionResultLogger.ActionResult(action, datafilepath, datafilename, countRecords, countRecordsComErro > 0, null, new Date(), Boolean.FALSE));
			}
			Thread.yield();
		} catch (ClassNotFoundException | SQLException ex) {
			System.out.println("ERRO: [" + this.csvThreadID + "] " + ex.getMessage() + " ; Query: " + sql);
			try {
				connection.commit();
			} catch (SQLException ex1) {
				Logger.getLogger(CVMDatabaseThread.class
						.getName()).log(Level.SEVERE, null, ex1);
			}
			Logger.getLogger(CVS2SQLConverter.class
					.getName()).log(Level.SEVERE, null, ex);
			actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.FILE_PROCESSED.toString(), datafilepath, datafilename, countRecords, Boolean.TRUE, "Query:" + sql + " ; Message:" + ex.getMessage(), new Date(), Boolean.FALSE));
		}
	}

}
