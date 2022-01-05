/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package investfunds;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.Month;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rlcancian
 */
public class CVMDatabaseManager implements ThreadCompleteListener {

	private final String JDBC_DRIVER;
	private final String DB_URL;
	private final String databasename;
	private final String databaseUsername;
	private final String databasePassword;
	private final String localCVMDir;
	private final boolean forceProcessFiles;
	private final boolean forceCalculate;
	private ActionResultLogger actionResultLogger;
	// threads
	private final static List<CVMDatabaseThread> threadPool = Collections.synchronizedList(new ArrayList<>());
	private static int MAX_RUNNING_THREADS = 4;
	private int total_num_threads;
	private int newCSVThreadID;
	private static final Semaphore semaphoreAllDone = new Semaphore(0);

	/**
	 *
	 * @param databasename
	 * @param databaseUsername
	 * @param databasePassword
	 * @param localCVMDir
	 * @param JDBC_DRIVER
	 * @param DB_URL
	 * @param forceProcessFiles
	 * @param forceCalculate
	 */
	public CVMDatabaseManager(String databasename, String databaseUsername, String databasePassword, String localCVMDir, String JDBC_DRIVER, String DB_URL, boolean forceProcessFiles, boolean forceCalculate) {
		//db
		this.databasename = databasename;
		this.databaseUsername = databaseUsername;
		this.databasePassword = databasePassword;
		this.localCVMDir = localCVMDir;
		this.JDBC_DRIVER = JDBC_DRIVER;
		this.DB_URL = DB_URL;
		// force
		this.forceCalculate = forceCalculate;
		this.forceProcessFiles = forceProcessFiles;
		// threads
		newCSVThreadID = 1;
	}

	private static void AddThread(CVMDatabaseThread thread) {
		CVMDatabaseManager.AddThread(thread, true);
	}

	private static void AddThread(CVMDatabaseThread thread, boolean startRunning) {
		synchronized (threadPool) {
			threadPool.add(thread);
		}
		/*
		 * synchronized (threadPool) { CustomComparator comparator = new CustomComparator();
		 * Collections.sort(threadPool, comparator); comparator = null; }
		 */
		int pos = threadPool.indexOf(thread);
		System.out.println("info: thread " + thread.getCSVThreadID() + " incluída na posição " + pos + " da lista para processamento do arquivo " + thread.getDatafilename() + " de tamanho " + thread.getDatafilelength() + " bytes");
		if (startRunning) {
			CVMDatabaseManager.CheckDispatchNewThread();
		}
	}

	/**
	 *
	 * @param thread
	 */
	@Override
	public void notifyOfThreadComplete(Thread thread) {
		int size;
		synchronized (threadPool) {
			int registrosComErro = ((CVMDatabaseThread) (thread)).getCountErrosRegistros();
			threadPool.remove(thread);
			size = threadPool.size();
		}
		if (total_num_threads > 0) {
			System.out.println("info: [" + ((CVMDatabaseThread) thread).getCSVThreadID() + "] thread enviada para execução. Há " + size + " threads restantes (" + size + "/" + total_num_threads + " = " + 100 * size / total_num_threads + "% restantes). Tempo estimado: " + GetTempoEstimadoStr());
		}
		if (size > 0) {
			CVMDatabaseManager.CheckDispatchNewThread();
		} else {
			System.out.println("info: TODAS AS THREADS TERMINARAM. FIM DO PROCESSAMENTO.");
			semaphoreAllDone.release();
		}
	}

	private static String GetTempoEstimadoStr() {
		double tempoEstimado;
		tempoEstimado = ((CVMDatabaseThread.GetTempoTotalThreads() / CVMDatabaseThread.GetNumTotalThreads()) * threadPool.size()) / MAX_RUNNING_THREADS;
		String tempoEstimadoStr = tempoEstimado + " s";
		if (tempoEstimado > 60) {
			tempoEstimado /= 60;
			tempoEstimado = (1.0 * ((long) (tempoEstimado * 1e2))) / 1e2;
			tempoEstimadoStr = tempoEstimado + " min";
		}
		if (tempoEstimado > 60) {
			tempoEstimado /= 60;
			tempoEstimado = (1.0 * ((long) (tempoEstimado * 1e2))) / 1e2;
			tempoEstimadoStr = tempoEstimado + " h";
		}
		return tempoEstimadoStr;
	}

	private static void CheckDispatchNewThread() {
		try {
			int temp = MAX_RUNNING_THREADS;
			MAX_RUNNING_THREADS = Integer.valueOf(new Scanner(new File("./MAX_RUNNING_THREADS.txt")).useDelimiter("\\Z").next());
			if (MAX_RUNNING_THREADS != temp) {
				CVMDatabaseThread.ResetTempoTotalThreads();
			}
		} catch (FileNotFoundException | NumberFormatException ex) {
			Logger.getLogger(CVMDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
			MAX_RUNNING_THREADS = 1;
		}
		synchronized (threadPool) {
			// must be in synchronized block 
			Iterator it = threadPool.iterator();
			int countRunning = 0;
			long totalFileSizes = 0;
			CVMDatabaseThread t;
			while (it.hasNext()) {
				//System.out.println(it.next());
				t = (CVMDatabaseThread) it.next();
				totalFileSizes += t.getDatafilelength();
				if (t.isAlive()) {
					countRunning++;
				}
			}
			if (countRunning >= MAX_RUNNING_THREADS) {
				System.out.println("info: Quantidade de " + countRunning + " threads executando já atingiu o limite máximo de " + MAX_RUNNING_THREADS + " (" + totalFileSizes / 1e6 + "MB para processar)" + ". Há " + threadPool.size() + " threads na fila");
			}
			it = threadPool.iterator();
			while (it.hasNext() && countRunning < MAX_RUNNING_THREADS) {
				t = (CVMDatabaseThread) it.next();
				if (!t.isAlive()) {
					System.out.println("info: Thread " + ((CVMDatabaseThread) t).getCSVThreadID() + " pode iniciar a executar (" + countRunning + "/" + MAX_RUNNING_THREADS + ") " + " [" + totalFileSizes / 1e6 + "MB para processar]" + ". Há " + threadPool.size() + " threads na fila. Tempo estimado: " + GetTempoEstimadoStr());
					t.start();
					countRunning++;
				}
			}
			if (countRunning == 0) {
				System.out.println("info: NENHUMA THREAD PODE EXECUTAR. FIM DO PROCESSAMENTO.");
				semaphoreAllDone.release();
			}
		}
	}

	private void processaDir(String[] dataFiles, String localDVMSubdir, String currentDirStr) {
		Arrays.sort(dataFiles, (a, b) -> a.compareTo(b));
		CVMDatabaseThread thread;
		boolean deveProcessarArquivo;
		for (String dataFile : dataFiles) {
			ActionResultLogger.ActionResult result = this.actionResultLogger.getActionResult(new ActionResultLogger.ActionResult(ActionResultLogger.Action.FILE_PROCESSED.toString(), currentDirStr, dataFile, null, null, null, null, Boolean.FALSE));
			///// TODO: MUDAR ABAIXO DEPOIS DOS TESTES
			deveProcessarArquivo = (result == null || this.forceProcessFiles || this.forceCalculate);
			if (deveProcessarArquivo) {
				System.out.println("info:  Processando dados do arquivo " + dataFile);
				newCSVThreadID++;
				thread = new CVMDatabaseThread(newCSVThreadID, localDVMSubdir, currentDirStr, dataFile, JDBC_DRIVER, DB_URL, databasename, databaseUsername, databasePassword, this.forceProcessFiles, this.forceCalculate);
				thread.addListener(this);
				thread.setActionResultLogger(actionResultLogger);
				CVMDatabaseManager.AddThread(thread, false);
			} else {
				System.out.println("done:  Arquivo previamente processado " + dataFile);
			}
		}
	}

	/**
	 *
	 * @return
	 */
	public boolean fillDatabases() {
		String localDVMSubdir;
		String currentDirStr;
		String[] dataFiles;

		System.out.println("\ninfo: Processando cadastro de fundos");
		localDVMSubdir = "FI/CAD/DADOS/";
		currentDirStr = this.localCVMDir + localDVMSubdir;
		dataFiles = new File(currentDirStr).list(new FilenameFilter() {
			@Override
			public boolean accept(File f, String name) {
				return name.startsWith("inf_cad") && name.endsWith(".csv");
			}
		});
		processaDir(dataFiles, localDVMSubdir, currentDirStr);
		CVMDatabaseManager.CheckDispatchNewThread();
		System.out.println("info: Aguardando " + total_num_threads + " threads terminarem");
		try {
			semaphoreAllDone.acquire();
		} catch (InterruptedException ex) {
		}

		//
		System.out.println("\ninfo: Processando documentos de extratos");
		localDVMSubdir = "FI/DOC/EXTRATO/DADOS/";
		currentDirStr = this.localCVMDir + localDVMSubdir;
		dataFiles = new File(currentDirStr).list(new FilenameFilter() {
			@Override
			public boolean accept(File f, String name) {
				return name.startsWith("extrato_fi_") && name.endsWith(".csv");
			}
		});
		processaDir(dataFiles, localDVMSubdir, currentDirStr);
		CVMDatabaseManager.CheckDispatchNewThread();
		System.out.println("info: Aguardando " + total_num_threads + " threads terminarem");
		try {
			semaphoreAllDone.acquire();
		} catch (InterruptedException ex) {
		}
		CVMDatabaseThread.ResetTempoTotalThreads();
		//

		System.out.println("\ninfo: Processando documentos complementares");
		localDVMSubdir = "FI/DOC/COMPL/DADOS/";
		currentDirStr = this.localCVMDir + localDVMSubdir;
		dataFiles = new File(currentDirStr).list(new FilenameFilter() {
			@Override
			public boolean accept(File f, String name) {
				return name.startsWith("compl_fi_") && name.endsWith(".csv");
			}
		});
		processaDir(dataFiles, localDVMSubdir, currentDirStr);
		CVMDatabaseManager.CheckDispatchNewThread();
		System.out.println("info: Aguardando " + total_num_threads + " threads terminarem");
		try {
			semaphoreAllDone.acquire();
		} catch (InterruptedException ex) {
		}
		CVMDatabaseThread.ResetTempoTotalThreads();
		//

		System.out.println("\ninfo: Processando cadastro de intermediários");
		localDVMSubdir = "INTERMEDIARIO/DADOS/";
		currentDirStr = this.localCVMDir + localDVMSubdir;
		dataFiles = new File(currentDirStr).list(new FilenameFilter() {
			@Override
			public boolean accept(File f, String name) {
				return name.startsWith("inf_cadastral_intermediario.csv");// && name.endsWith(".csv");
			}
		});
		/////////processaDir(dataFiles, localDVMSubdir, currentDirStr);
		CVMDatabaseManager.CheckDispatchNewThread();
		System.out.println("info: Aguardando " + total_num_threads + " threads terminarem");
		try {
			semaphoreAllDone.acquire();
		} catch (InterruptedException ex) {
		}
		CVMDatabaseThread.ResetTempoTotalThreads();
		//

		System.out.println("\ninfo: Processando documentos de informes diários históricos");
		localDVMSubdir = "FI/DOC/INF_DIARIO/DADOS/HIST/";
		currentDirStr = this.localCVMDir + localDVMSubdir;
		dataFiles = new File(currentDirStr).list(new FilenameFilter() {
			@Override
			public boolean accept(File f, String name) {
				return name.startsWith("inf_diario_fi_") && name.endsWith(".csv");
			}
		});
		processaDir(dataFiles, localDVMSubdir, currentDirStr);
		CVMDatabaseManager.CheckDispatchNewThread();
		System.out.println("info: Aguardando " + total_num_threads + " threads terminarem");
		try {
			semaphoreAllDone.acquire();
		} catch (InterruptedException ex) {
		}
		CVMDatabaseThread.ResetTempoTotalThreads();

		System.out.println("\ninfo: Processando documentos de informes diários");
		localDVMSubdir = "FI/DOC/INF_DIARIO/DADOS/";
		currentDirStr = this.localCVMDir + localDVMSubdir;
		dataFiles = new File(currentDirStr).list(new FilenameFilter() {
			@Override
			public boolean accept(File f, String name) {
				return name.startsWith("inf_diario_fi_") && name.endsWith(".csv");
			}
		});
		processaDir(dataFiles, localDVMSubdir, currentDirStr);
		CVMDatabaseManager.CheckDispatchNewThread();
		System.out.println("info: Aguardando " + total_num_threads + " threads terminarem");
		try {
			semaphoreAllDone.acquire();
		} catch (InterruptedException ex) {
		}
		CVMDatabaseThread.ResetTempoTotalThreads();
		// depois de processar os informes diários, plota os fundos não encontrados no cadastro
		Set<String> fundosNaoEncontrados = CVMDatabaseThread.Get_cad_cnpj_fundo_nao_encontrado();
		fundosNaoEncontrados.forEach((fundo) -> {
			System.out.println("erro: fundo não encontrado no cadastro: " + fundo);
		});

		return true;
	}

	/**
	 *
	 * @return
	 */
	public boolean calculateDirectIndicators() {
		try {
			int id;
			String strId;
			String sql;
			System.out.println("\ninfo: Calculando indicadores financeiros diretos de cada fundo");
			java.sql.Connection connection;
			try {
				Class.forName(this.JDBC_DRIVER);
			} catch (ClassNotFoundException ex) {
				Logger.getLogger(CVMDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
			}
			connection = DriverManager.getConnection(this.DB_URL + this.databasename, this.databaseUsername, this.databasePassword);
			connection.setAutoCommit(false);
			Statement st = connection.createStatement();
			Statement st2 = connection.createStatement();
			sql = "select id from cnpj_fundos order by id"; //////
			java.sql.ResultSet rs = st.executeQuery(sql);
			java.sql.ResultSet rs2;
			CVMDatabaseThread thread;
			int count = 0;
			while (rs.next()) {
				id = rs.getInt("id");
				sql = "select * from cancelamento_fundos use index (idx_id_fundo) where cnpj_fundo_id=" + id;
				rs2 = st2.executeQuery(sql);
				if (!rs2.next()) { // exclui fundos cancelados
					strId = String.valueOf(id);
					System.out.println("info:  Criando thread para calcular indicadores do fundo " + id);
					newCSVThreadID++;
					thread = new CVMDatabaseThread(newCSVThreadID, "diretorio_calcula_indicadores_diretos_fundos", strId, strId, JDBC_DRIVER, DB_URL, databasename, databaseUsername, databasePassword, this.forceProcessFiles, this.forceCalculate);
					thread.addListener(this);
					thread.setActionResultLogger(actionResultLogger);
					CVMDatabaseManager.AddThread(thread, false); //, (count++ % 10 == 0));
					if (count++ % 10 == 0) { // a cada 10 threads criadas, libera para executar
						CVMDatabaseManager.CheckDispatchNewThread();
						//try {
						//	semaphoreAllDone.acquire();
						//} catch (InterruptedException ex) {
						//}
					}
				} else {
					////System.out.println("done:  Fundo " + id + " foi cancelado");
				}
			}
			CVMDatabaseManager.CheckDispatchNewThread();
			System.out.println("info: Aguardando " + total_num_threads + " threads terminarem");
			try {
				semaphoreAllDone.acquire();
			} catch (InterruptedException ex) {
				return false;
			}
			return true;
		} catch (SQLException ex) {
			Logger.getLogger(CVMDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
			return false;
		}
	}

	/**
	 *
	 * @return
	 */
	public boolean calculateMarketIndicators() {
		try {
			System.out.println("\ninfo: Calculando indicadores do mercado");
			java.sql.Connection connection;
			try {
				Class.forName(this.JDBC_DRIVER);
			} catch (ClassNotFoundException ex) {
				Logger.getLogger(CVMDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
			}
			connection = DriverManager.getConnection(this.DB_URL + this.databasename, this.databaseUsername, this.databasePassword);
			connection.setAutoCommit(false);
			Statement st = connection.createStatement();
			Statement st2 = connection.createStatement();
			Statement st3 = connection.createStatement();
			java.sql.ResultSet rs;
			java.sql.ResultSet rs2;
			String sql;
			//
			// Calcula rentabilidades e volatilidade em períodos mensais
			//
			int tipo_id, periodoMeses;
			Double rent;
			Double rentabilidadeMercado, volatilidadeMercado, sumRentabilidadeMercado, sumRentabilidadeMercadoSquare;
			Double rentabilidadeClasse, volatilidadeClasse, sumRentabilidadeClasse, sumRentabilidadeClasseSquare;
			long numValoresMercado, numValoresClasse;
			LocalDate ultimaData, primeiraData = LocalDate.now();
			// ASSUME QUE A ÚLTIMA DATA FOI ONTEM (evita uma query sobre todos os fundos para pegar a data mais recente)
			ultimaData = LocalDate.now().minusDays(1); // rs.getDate("UltimaDataComDados").toLocalDate(); // com dados diários obtidos da CVM
			long mesesNoPassadoParaUltimaData = 0;
			//
			// Calcula indicadores para 1, 2, 3, 6, 12, 24, ... 96 meses no passado, todos terminando no mês passado (em relação a hoje), ou a 2 meses no passado, 3, 4, até terminando 12*2 meses no passado
			while (mesesNoPassadoParaUltimaData < 12 * 2) {
				ultimaData = ultimaData.minusMonths(1L);
				System.out.println("  Procurando fundos com indicadores mensais encerrando em " + ultimaData.getMonthValue() + "/" + ultimaData.getYear());
				periodoMeses = 1;
				while (periodoMeses <= 96) {
					sumRentabilidadeMercado = 0.0;
					sumRentabilidadeMercadoSquare = 0.0;
					numValoresMercado = 0;
					//
					// varre as classes dos fundos
					sql = "select * from tipo_classe_fundos order by id";
					rs = st.executeQuery(sql);
					while (rs.next()) {
						tipo_id = rs.getInt("id");
						//System.out.println("Procurando '" + rs.getString("classe") + "' com indicadores de " + periodoMeses + " meses encerrando em " + ultimaData.getMonthValue() + "/" + ultimaData.getYear());
						// busca todos os fundos dessa classe com certa data final (mes e ano) e certa quantidade de meses nos indicadores
						sql = "select indicadores_fundos.cnpj_fundo_id, rentabilidade, data_inicial, data_final from indicadores_fundos "
								+ " inner join cadastro_fundos on cadastro_fundos.cnpj_fundo_id = indicadores_fundos.cnpj_fundo_id and cadastro_fundos.tipo_classe_fundo_id = " + tipo_id
								+ " where month(data_final)=" + ultimaData.getMonthValue() + " and year(data_final)=" + ultimaData.getYear() + " and periodo_meses= " + periodoMeses;//"+ " order by ";
						rs2 = st2.executeQuery(sql);
						sumRentabilidadeClasse = 0.0;
						sumRentabilidadeClasseSquare = 0.0;
						numValoresClasse = 0;
						while (rs2.next()) {
							if (sumRentabilidadeClasse == 0.0) {
								primeiraData = rs2.getDate("data_inicial").toLocalDate();
								ultimaData = rs2.getDate("data_final").toLocalDate();
							}
							rent = rs2.getDouble("rentabilidade");
							sumRentabilidadeMercado += rent;
							sumRentabilidadeMercadoSquare += rent * rent;
							numValoresMercado++;
							sumRentabilidadeClasse += rent;
							sumRentabilidadeClasseSquare += rent * rent;
							numValoresClasse++;
						}
						if (numValoresClasse > 0) {
							rentabilidadeClasse = sumRentabilidadeClasse / numValoresClasse;
							if (numValoresClasse > 1) {
								volatilidadeClasse = Math.sqrt(sumRentabilidadeClasseSquare - (sumRentabilidadeClasse * sumRentabilidadeClasse) / numValoresClasse);
							} else {
								volatilidadeClasse = 0.0;
							}
							// atualiza BD com indicadores dessa classe nesse período de tempo
							//System.out.println("Atualizando " + numValoresClasse + " '" + rs.getString("classe") + "'  com indicadores de " + periodoMeses + "  meses encerrando em " + ultimaData.getMonthValue() + "/" + ultimaData.getYear());

							try {
								sql = "insert into indicadores_mercados (tipo_classe_fundos_id, data_inicial, data_final, periodo_meses, num_valores, rentabilidade, desvio_padrao) "
										+ " values(" + tipo_id + ", '" + primeiraData.toString() + "', '" + ultimaData.toString() + "', " + periodoMeses + ", " + numValoresClasse + ", " + rentabilidadeClasse + ", " + volatilidadeClasse + ")";
								st3.execute(sql);
							} catch (SQLException ex) {
								//System.out.println(ex.getMessage());
								try {
									sql = "update indicadores_mercados set data_inicial='" + primeiraData.toString() + "', num_valores=" + numValoresClasse + ", rentabilidade=" + rentabilidadeClasse + ", desvio_padrao=" + volatilidadeClasse + " where tipo_classe_fundos_id=" + tipo_id + " and data_final='" + ultimaData.toString() + "' and periodo_meses=" + periodoMeses;
									st3.execute(sql);
								} catch (SQLException ex2) {
								}
							}
						}
					}
					if (numValoresMercado > 0) {
						rentabilidadeMercado = sumRentabilidadeMercado / numValoresMercado;
						if (numValoresMercado > 1) {
							volatilidadeMercado = Math.sqrt(sumRentabilidadeMercadoSquare - (sumRentabilidadeMercado * sumRentabilidadeMercado) / numValoresMercado);
						} else {
							volatilidadeMercado = 0.0;
						}
						// atualiza BD com indicadores de todo o mercado nesse período de tempo
						System.out.println("    Atualizando " + numValoresMercado + " valores de mercado com indicadores de " + periodoMeses + "  meses encerrando em " + ultimaData.getMonthValue() + "/" + ultimaData.getYear());
						int tipo_id_mercado = 999;
						try {
							sql = "insert into indicadores_mercados (tipo_classe_fundos_id, data_inicial, data_final, periodo_meses, num_valores, rentabilidade, desvio_padrao) "
									+ " values(" + tipo_id_mercado + ", '" + primeiraData.toString() + "', '" + ultimaData.toString() + "', " + periodoMeses + ", " + numValoresMercado + ", " + rentabilidadeMercado + ", " + volatilidadeMercado + ")";
							st3.execute(sql);
						} catch (SQLException ex) {
							//System.out.println(ex.getMessage());
							try {
								sql = "update indicadores_mercados set data_inicial='" + primeiraData.toString() + "', num_valores=" + numValoresMercado + ", rentabilidade=" + rentabilidadeMercado + ", desvio_padrao=" + volatilidadeMercado + " where tipo_classe_fundos_id=" + tipo_id_mercado + " and data_final='" + ultimaData.toString() + "' and periodo_meses=" + periodoMeses;
								st3.execute(sql);
							} catch (SQLException ex2) {
							}
						}
						connection.commit();
					}
					// atualiza quantidade de meses para a estatistica
					switch (periodoMeses) {
						case 1:
							periodoMeses = 2;
							break;
						case 2:
							periodoMeses = 3;
							break;
						case 3:
							periodoMeses = 6;
							break;
						case 6:
							periodoMeses = 12;
							break;
						default:
							periodoMeses += 12;
					}
				}
				mesesNoPassadoParaUltimaData++;
			}
			connection.commit();
			return true;
			//}
		} catch (SQLException ex) {
			Logger.getLogger(CVMDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
			return false;
		}
	}

	public boolean calculateIndicatorsBasedOnMarket() {
		try {
			System.out.println("\ninfo: Calculando indicadores de fundos baseados no mercado");
			java.sql.Connection connection;
			try {
				Class.forName(this.JDBC_DRIVER);
			} catch (ClassNotFoundException ex) {
				Logger.getLogger(CVMDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
			}
			connection = DriverManager.getConnection(this.DB_URL + this.databasename, this.databaseUsername, this.databasePassword);
			connection.setAutoCommit(false);
			Statement st = connection.createStatement();
			Statement st2 = connection.createStatement();
			java.sql.ResultSet rs;
			java.sql.ResultSet rs2;
			String sql;

			//
			// fim dos cálculos do mercardo.
			// Vai disparar threads para cálculo dos indicadores indiretos (aqueles que dependem do mercado) de cada fundo
			//
			System.out.println("\ninfo: Calculando indicadores (de mercado) de cada fundo");
			sql = "select id from cnpj_fundos order by id";
			rs = st.executeQuery(sql);
			CVMDatabaseThread thread;
			int tipo_id, count = 0;
			String strId;
			while (rs.next()) {
				tipo_id = rs.getInt("id");
				sql = "select * from cancelamento_fundos use index (idx_id_fundo) where cnpj_fundo_id=" + tipo_id;
				rs2 = st2.executeQuery(sql);
				if (!rs2.next()) { // exclui fundos cancelados
					strId = String.valueOf(tipo_id);
					System.out.println("info:  Criando thread para calcular indicadores (de mercado) do fundo " + tipo_id);
					newCSVThreadID++;
					thread = new CVMDatabaseThread(newCSVThreadID, "diretorio_calcula_indicadores_fundos_baseados_mercado", strId, strId, JDBC_DRIVER, DB_URL, databasename, databaseUsername, databasePassword, this.forceProcessFiles, this.forceCalculate);
					thread.addListener(this);
					thread.setActionResultLogger(actionResultLogger);
					CVMDatabaseManager.AddThread(thread, false); //, (count++ % 10 == 0));
					if (count++ % 10 == 0) { // a cada 10 threads criadas, libera para executar
						CVMDatabaseManager.CheckDispatchNewThread();
						try {
							semaphoreAllDone.acquire();
						} catch (InterruptedException ex) {
						}
					}
				} else {
					////System.out.println("done:  Fundo " + id + " foi cancelado");
				}
			}
			CVMDatabaseManager.CheckDispatchNewThread();
			System.out.println("info: Aguardando " + total_num_threads + " threads terminarem");
			try {
				semaphoreAllDone.acquire();
			} catch (InterruptedException ex) {
				return false;
			}
			return true;
		} catch (SQLException ex) {
			Logger.getLogger(CVMDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
			return false;
		}
	}

	/**
	 *
	 * @return
	 */
	public boolean createDBInfra() {

		try {
			System.out.println("Creating database infra...");
			java.sql.Connection connection;
			Class.forName(this.JDBC_DRIVER);
			connection = DriverManager.getConnection(this.DB_URL + this.databasename, this.databaseUsername, this.databasePassword);
			connection.setAutoCommit(false);
			Statement st = connection.createStatement();
			String sql = "";
			try {
				sql = new String(Files.readAllBytes(Paths.get("./create_investfunds.sql")), StandardCharsets.UTF_8);
			} catch (IOException ex) {
				Logger.getLogger(CVMDatabaseManager.class
						.getName()).log(Level.SEVERE, null, ex);
			}
			st.execute(sql);
			connection.commit();
			connection.close();
			return true;
		} catch (SQLException | ClassNotFoundException ex) {
			Logger.getLogger(CVMDatabaseManager.class
					.getName()).log(Level.SEVERE, null, ex);
		}
		return false;
	}

	/**
	 * @param actionResultLogger the actionResultLogger to set
	 */
	public void setActionResultLogger(ActionResultLogger actionResultLogger) {
		this.actionResultLogger = actionResultLogger;
	}

	/**
	 * Set the buffer to the appropriate size, which, for the moment is 12G. This seriously affects performance.
	 *
	 * @return true
	 */
	public boolean setBufferPoolSize() {
		try {
			java.sql.Connection connection;
			connection = DriverManager.getConnection(this.DB_URL + this.databasename, this.databaseUsername, this.databasePassword);
			connection.setAutoCommit(false);
			Statement st = connection.createStatement();
			String sql = "SET GLOBAL innodb_buffer_pool_size=2000000000;";
			st.execute(sql);
			connection.commit();
		} catch (SQLException ex) {
			//ex.printStackTrace();
			System.out.println("ERRO ao executar SQL");
		}
		return true;
	}

	/**
	 * Creates the relevant tables for calculating indicators, calls setbufferPoolSize to ensure performance
	 *
	 * @return true
	 */
	public boolean createCiaDbs() {
		System.out.println("Creating CIA TABLES");
		this.setBufferPoolSize();
		try {
			java.sql.Connection connection;
			connection = DriverManager.getConnection(this.DB_URL + this.databasename, this.databaseUsername, this.databasePassword);
			connection.setAutoCommit(false);
			Statement st = connection.createStatement();
			String sql = ""
					// "-- cnpj_cias_abertas definition\n"
					+ "				CREATE TABLE IF NOT EXISTS `cnpj_cias_abertas` (\n"
					+ "				  `CNPJ_CIA` varchar(20) NOT NULL,\n"
					+ "				  `DENOM_SOCIAL` varchar(100) NOT NULL,\n"
					+ "				  `DENOM_COMERC` varchar(100) NOT NULL,\n"
					+ "				  `SIT` varchar(40) NOT NULL,\n"
					+ "				  `CD_CVM` decimal(7,0) NOT NULL,\n"
					+ "				  PRIMARY KEY (`CNPJ_CIA`)\n"
					+ "				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
					+ "				\n"
					+ "				";
			st.execute(sql);
			connection.commit();
			// cias_bpas definition
			sql = "				\n"
					+ "				CREATE TABLE IF NOT EXISTS `cias_bpas` (\n"
					+ "				  `CNPJ_CIA` varchar(20) NOT NULL,\n"
					+ "				  `DT_REFER` date NOT NULL,\n"
					+ "				  `VERSAO` int(5) DEFAULT NULL,\n"
					+ "				  `CD_CVM` int(7) DEFAULT NULL,\n"
					+ "				  `MOEDA` varchar(4) DEFAULT NULL,\n"
					+ "				  `ESCALA_MOEDA` varchar(7) DEFAULT NULL,\n"
					+ "				  `ORDEM_EXERC` varchar(9) DEFAULT NULL,\n"
					+ "				  `DT_FIM_EXERC` date DEFAULT NULL,\n"
					+ "				  `CD_CONTA` varchar(18) NOT NULL,\n"
					+ "				  `DS_CONTA` varchar(100) DEFAULT NULL,\n"
					+ "				  `VL_CONTA` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `ST_CONTA_FIXA` varchar(1) DEFAULT NULL,\n"
					+ "				  PRIMARY KEY (`CNPJ_CIA`,`DT_REFER`,`CD_CONTA`, `ORDEM_EXERC`)\n"
					//+"				  CONSTRAINT `bpa_cia_aberta_con_FK` FOREIGN KEY (`CNPJ_CIA`) REFERENCES `cnpj_cias_abertas` (`CNPJ_CIA`)\n"
					+ "				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
					+ "				\n"
					+ "				";
			st.execute(sql);
			connection.commit();

			// cias_bpps definition
			sql = "				\n"
					+ "				CREATE TABLE IF NOT EXISTS `cias_bpps` (\n"
					+ "				  `CNPJ_CIA` varchar(20) NOT NULL,\n"
					+ "				  `DT_REFER` date NOT NULL,\n"
					+ "				  `VERSAO` int(5) DEFAULT NULL,\n"
					+ "				  `CD_CVM` int(7) DEFAULT NULL,\n"
					+ "				  `MOEDA` varchar(4) DEFAULT NULL,\n"
					+ "				  `ESCALA_MOEDA` varchar(7) DEFAULT NULL,\n"
					+ "				  `ORDEM_EXERC` varchar(9) DEFAULT NULL,\n"
					+ "				  `DT_FIM_EXERC` date DEFAULT NULL,\n"
					+ "				  `CD_CONTA` varchar(18) NOT NULL,\n"
					+ "				  `DS_CONTA` varchar(100) DEFAULT NULL,\n"
					+ "				  `VL_CONTA` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `ST_CONTA_FIXA` varchar(1) DEFAULT NULL,\n"
					+ "				  PRIMARY KEY (`CNPJ_CIA`,`DT_REFER`,`CD_CONTA`, `ORDEM_EXERC`)\n"
					//+"				  CONSTRAINT `bpp_cia_aberta_con_FK` FOREIGN KEY (`CNPJ_CIA`) REFERENCES `cnpj_cias_abertas` (`CNPJ_CIA`)\n"
					+ "				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
					+ "				\n"
					+ "				";
			st.execute(sql);
			connection.commit();
			// cias_cods_b3s definition
			sql = "				\n"
					+ "				CREATE TABLE IF NOT EXISTS `cias_cods_b3s` (\n"
					+ "				  `CD_B3` varchar(20) NOT NULL,\n"
					+ "				  `CNPJ_CIA` varchar(20) DEFAULT NULL,\n"
					+ "				  `DENOM_CIA` varchar(100) DEFAULT NULL,\n"
					+ "				  PRIMARY KEY (`CD_B3`),\n"
					+ "				  KEY `cias_cods_b3s_FK` (`CNPJ_CIA`)\n"
					//+"				  CONSTRAINT `cias_cods_b3s_FK` FOREIGN KEY (`CNPJ_CIA`) REFERENCES `cnpj_cias_abertas` (`CNPJ_CIA`)\n"
					+ "				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
					+ "				\n"
					+ "				";
			st.execute(sql);
			connection.commit();
			// cias_precos_diarios definition
			sql = "				\n"
					+ "				CREATE TABLE IF NOT EXISTS `cias_precos_diarios` (\n"
					+ "				  `CD_B3` varchar(20) NOT NULL,\n"
					+ "				  `DT_REFER` date NOT NULL,\n"
					+ "				  `COD_BDI` varchar(20) DEFAULT NULL,\n"
					+ "				  `DENOM_CIA` varchar(100) DEFAULT NULL,\n"
					+ "				  `SPEC_CODE` varchar(11) DEFAULT NULL,\n"
					+ "				  `CLOSE_PRICE` decimal(29,10) DEFAULT NULL,\n"
					+ "				  PRIMARY KEY (`CD_B3`,`DT_REFER`)\n"
					//+"				  CONSTRAINT `cod_b3_cias_abertas_preco_diario_FK` FOREIGN KEY (`CD_B3`) REFERENCES `cias_cods_b3s` (`CD_B3`)"
					+ "				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
					+ "				\n"
					+ "				";
			st.execute(sql);
			connection.commit();
			// cias_dfcs_mds definition
			sql = "				\n"
					+ "				CREATE TABLE IF NOT EXISTS `cias_dfcs_mds` (\n"
					+ "				  `CNPJ_CIA` varchar(20) NOT NULL,\n"
					+ "				  `DT_REFER` date NOT NULL,\n"
					+ "				  `VERSAO` int(5) DEFAULT NULL,\n"
					+ "				  `CD_CVM` decimal(7,0) DEFAULT NULL,\n"
					+ "				  `MOEDA` varchar(4) DEFAULT NULL,\n"
					+ "				  `ESCALA_MOEDA` varchar(7) DEFAULT NULL,\n"
					+ "				  `ORDEM_EXERC` varchar(9) DEFAULT NULL,\n"
					+ "				  `DT_INI_EXERC` date NOT NULL,\n"
					+ "				  `DT_FIM_EXERC` date NOT NULL,\n"
					+ "				  `CD_CONTA` varchar(18) NOT NULL,\n"
					+ "				  `DS_CONTA` varchar(100) DEFAULT NULL,\n"
					+ "				  `VL_CONTA` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `ST_CONTA_FIXA` varchar(1) DEFAULT NULL,\n"
					+ "				  PRIMARY KEY (`CNPJ_CIA`,`DT_REFER`,`DT_INI_EXERC`,`DT_FIM_EXERC`,`CD_CONTA`)\n"
					//+"				  CONSTRAINT `dfc_md_cia_aberta_con_FK` FOREIGN KEY (`CNPJ_CIA`) REFERENCES `cnpj_cias_abertas` (`CNPJ_CIA`)\n"
					+ "				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
					+ "				\n"
					+ "				";
			st.execute(sql);
			connection.commit();
			// cias_dmpls definition
			sql = "				\n"
					+ "				CREATE TABLE IF NOT EXISTS `cias_dmpls` (\n"
					+ "				  `CNPJ_CIA` varchar(20) NOT NULL,\n"
					+ "				  `DT_REFER` date NOT NULL,\n"
					+ "				  `VERSAO` int(5) DEFAULT NULL,\n"
					+ "				  `CD_CVM` decimal(7,0) DEFAULT NULL,\n"
					+ "				  `MOEDA` varchar(4) DEFAULT NULL,\n"
					+ "				  `ESCALA_MOEDA` varchar(7) DEFAULT NULL,\n"
					+ "				  `ORDEM_EXERC` varchar(9) DEFAULT NULL,\n"
					+ "				  `DT_INI_EXERC` date NOT NULL,\n"
					+ "				  `DT_FIM_EXERC` date NOT NULL,\n"
					+ "				  `COLUNA_DF` varchar(60) DEFAULT NULL,\n"
					+ "				  `CD_CONTA` varchar(18) NOT NULL,\n"
					+ "				  `DS_CONTA` varchar(100) DEFAULT NULL,\n"
					+ "				  `VL_CONTA` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `ST_CONTA_FIXA` varchar(1) DEFAULT NULL,\n"
					+ "				  PRIMARY KEY (`CNPJ_CIA`,`DT_REFER`,`DT_INI_EXERC`,`DT_FIM_EXERC`,`CD_CONTA`),\n"
					+ "				  KEY `dmpl_cia_aberta_con_CNPJ_CIA_IDX` (`CNPJ_CIA`,`DT_INI_EXERC`,`DT_FIM_EXERC`,`DS_CONTA`) USING BTREE"
					//+"				  CONSTRAINT `dmpl_cia_aberta_con_FK` FOREIGN KEY (`CNPJ_CIA`) REFERENCES `cnpj_cias_abertas` (`CNPJ_CIA`)\n"
					+ "				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
					+ "				\n"
					+ "				";
			st.execute(sql);
			connection.commit();
			// cias_dres definition
			sql = "				\n"
					+ "				CREATE TABLE IF NOT EXISTS `cias_dres` (\n"
					+ "				  `CNPJ_CIA` varchar(20) NOT NULL,\n"
					+ "				  `DT_REFER` date NOT NULL,\n"
					+ "				  `VERSAO` int(5) DEFAULT NULL,\n"
					+ "				  `CD_CVM` decimal(7,0) DEFAULT NULL,\n"
					+ "				  `MOEDA` varchar(4) DEFAULT NULL,\n"
					+ "				  `ESCALA_MOEDA` varchar(7) DEFAULT NULL,\n"
					+ "				  `ORDEM_EXERC` varchar(9) DEFAULT NULL,\n"
					+ "				  `DT_INI_EXERC` date NOT NULL,\n"
					+ "				  `DT_FIM_EXERC` date NOT NULL,\n"
					+ "				  `CD_CONTA` varchar(18) NOT NULL,\n"
					+ "				  `DS_CONTA` varchar(100) DEFAULT NULL,\n"
					+ "				  `VL_CONTA` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `ST_CONTA_FIXA` varchar(1) DEFAULT NULL,\n"
					+ "				  PRIMARY KEY (`CNPJ_CIA`,`DT_REFER`,`CD_CONTA`,`DT_INI_EXERC`,`DT_FIM_EXERC`),\n"
					+ "				  KEY `dre_cia_aberta_con_DT_FIM_EXERC_IDX` (`DT_FIM_EXERC`) USING BTREE,\n"
					+ "				  KEY `dre_cia_aberta_con_DT_INI_EXERC_IDX` (`DT_INI_EXERC`) USING BTREE,\n"
					+ "				  KEY `dre_cia_aberta_con_DS_CONTA_IDX` (`DS_CONTA`) USING BTREE\n"
					//+"				  CONSTRAINT `dre_cia_aberta_con_FK` FOREIGN KEY (`CNPJ_CIA`) REFERENCES `cnpj_cias_abertas` (`CNPJ_CIA`)\n"
					+ "				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n"
					+ "				\n"
					+ "				";
			st.execute(sql);
			connection.commit();
			// cias_indicadores definition
			sql = "				\n"
					+ "				CREATE TABLE IF NOT EXISTS `cias_indicadores` (\n"
					+ "				  `CNPJ_CIA` varchar(20) NOT NULL,\n"
					+ "				  `DT_REFER` date NOT NULL,\n"
					+ "				  `CD_B3` varchar(20) NOT NULL,\n"
					+ "				  `CLOSE_PRICE` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `EBIT` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `MARGEM_EBIT%` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `EBITDA` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `MARGEM_EBITDA%` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `P/L` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `P/VP` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `P/EBIT` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `P/EBITDA` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `EV/EBIT` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `EV/EBITDA` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `EV` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `VPA` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `ROE%` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `ROIC%` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `LPA` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `EBIT/ATIVO%` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `GIRO_ATIVO` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `MARGEM_LIQUIDA%` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `LIQUIDEZ_CORRENTE` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `LUCRO_LIQUIDO` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `RECEITA_LIQUIDA` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `PATRIMONIO_LIQUIDO` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `PASSIVO` decimal(29,10) DEFAULT NULL,\n"
					+ "				  `ATIVO` decimal(29,10) DEFAULT NULL,\n"
					+ "				  PRIMARY KEY (`CNPJ_CIA`,`DT_REFER`,`CD_B3`)\n"
					//+"				  CONSTRAINT `indicadores_cia_aberta_FK` FOREIGN KEY (`CNPJ_CIA`) REFERENCES `cnpj_cias_abertas` (`CNPJ_CIA`)\n"
					+ "				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";
			st.execute(sql);
			connection.commit();

			sql = "CREATE TABLE IF NOT EXISTS `logs_atividades` (\n"
					+ "  `id` int\n"
					+ "			(11) NOT NULL AUTO_INCREMENT\n"
					+ "			,\n"
					+ "  `action` varchar(100) NOT NULL,\n"
					+ "			`remoteURI` varchar(400) DEFAULT NULL,\n"
					+ "			`localURI` varchar(400) DEFAULT NULL,\n"
					+ "			`result` bigint(20) DEFAULT NULL,\n"
					+ "			`hasErrors` tinyint(1) DEFAULT NULL,\n"
					+ "			`message` varchar(400) DEFAULT NULL,\n"
					+ "			`date` datetime NOT NULL\n"
					+ "			,\n"
					+ "  `needToRedo` tinyint(1) NOT NULL DEFAULT 0,\n"
					+ "  PRIMARY KEY (`id`)\n"
					+ ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;\n"
					+ "";
			st.execute(sql);
			connection.commit();

		} catch (SQLException ex) {
			ex.printStackTrace();
			// Logger.getLogger(CVMDatabaseManager.class.getName()).log(Level.SEVERE, null, ex);
		}
		return true;

	}

	/**
	 * Calculates the indicators and inserts them into the indicators table. This is very slow, so be patient!
	 *
	 * @return true
	 */
	public boolean calculateCIAIndicators() {
		try {
			Connection connection = DriverManager.getConnection(DB_URL + databasename + "?&allowLoadLocalInfile=true", databaseUsername, databasePassword);
			connection.setAutoCommit(false);
			// INSERT INDICATORS (if it doesn't work, append ignore after insert
			// because the data from CVM is broken.)
			String sql = "INSERT INTO cias_indicadores\n"
					+ "-- explain\n"
					+ "WITH DRE AS (\n"
					+ "    SELECT DISTINCT\n"
					+ "            CNPJ_CIA,\n"
					+ "            DT_INI_EXERC,\n"
					+ "            DT_FIM_EXERC,\n"
					+ "            MAX(IF(CD_CONTA = '3.03', VL_CONTA, NULL)) AS RESULTADO_BRUTO,\n"
					+ "            MAX(IF(CD_CONTA = '3.04.01', ABS(VL_CONTA), NULL)) AS DESPESAS_COM_VENDAS,\n"
					+ "            MAX(IF(CD_CONTA = '3.04.02', ABS(VL_CONTA), NULL)) AS DESPESAS_GERAIS_ADM,\n"
					+ "            SUM(IF(CD_CONTA LIKE '3.04%' AND (LOWER(DS_CONTA) LIKE '%amor%' OR LOWER(DS_CONTA) LIKE '%depre%'), VL_CONTA, 0)) AS DEPAMO,\n"
					+ "            MAX(IF(CD_CONTA = '3.11.01', VL_CONTA, NULL)) AS LUCROLIQUIDO,\n"
					+ "            MAX(IF(CD_CONTA = '3.01', VL_CONTA, NULL)) AS RECEITALIQUIDA,\n"
					+ "            MAX(IF(DS_CONTA LIKE 'PNA' AND CD_CONTA NOT LIKE '3.99.02%', VL_CONTA, NULL)) AS LPAPN,\n"
					+ "            MAX(IF(DS_CONTA LIKE 'ON' AND CD_CONTA NOT LIKE '3.99.02%', VL_CONTA, NULL)) AS LPAON\n"
					+ "    FROM cias_dres\n"
					+ "        WHERE VL_CONTA <> 0\n"
					+ "--         WHERE CD_CONTA IN ('3.99', \"3.04\", \"3.05\", \"3.11\")\n"
					+ "--         OR DS_CONTA IN ('PNA', 'ON')\n"
					+ "    GROUP BY CNPJ_CIA, DT_INI_EXERC, DT_FIM_EXERC\n"
					+ "    ),\n"
					+ "    BPP AS (\n"
					+ "    SELECT DISTINCT\n"
					+ "            CNPJ_CIA,\n"
					+ "            DT_FIM_EXERC,\n"
					+ "            MAX(IF(CD_CONTA = '2', VL_CONTA, NULL)) AS PASSIVO,\n"
					+ "            MAX(IF(CD_CONTA = '2.01.02', VL_CONTA, NULL)) AS FORNECEDORES,\n"
					+ "            MAX(IF(CD_CONTA = '2.01', VL_CONTA, NULL)) AS PASSIVO_CIRCULANTE,\n"
					+ "            MAX(IF(CD_CONTA = '2.01.04', VL_CONTA, NULL)) AS EMPRESTIMOS_FINANCIAMENTOS_CIRCULANTE,\n"
					+ "            MAX(IF(CD_CONTA = '2.02.01', VL_CONTA, NULL)) AS EMPRESTIMOS_FINANCIAMENTOS_NAO_CIRCULANTE,\n"
					+ "            MAX(IF(CD_CONTA = '2.03', VL_CONTA, NULL)) AS PL_CONSOLIDADO,\n"
					+ "            MAX(IF(CD_CONTA = '2.03.09', VL_CONTA, NULL)) AS PARTICIPACAO_ACIONISTAS_NAO_CONTROLADORES\n"
					+ "    FROM cias_bpps\n"
					+ "        WHERE VL_CONTA <> 0\n"
					+ "--         WHERE CD_CONTA IN ('3.99', \"3.04\", \"3.05\", \"3.11\")\n"
					+ "--         OR DS_CONTA IN ('PNA', 'ON')\n"
					+ "    GROUP BY CNPJ_CIA, DT_FIM_EXERC\n"
					+ "    ),\n"
					+ "    BPA AS (\n"
					+ "    SELECT DISTINCT\n"
					+ "            CNPJ_CIA,\n"
					+ "            DT_FIM_EXERC,\n"
					+ "            MAX(IF(CD_CONTA = '1', VL_CONTA, NULL)) AS ATIVO,\n"
					+ "            MAX(IF(CD_CONTA = '1.01', VL_CONTA, NULL)) AS ATIVO_CIRCULANTE,\n"
					+ "            MAX(IF(CD_CONTA = '1.02', VL_CONTA, NULL)) AS ATIVO_NAO_CIRCULANTE,\n"
					+ "            MAX(IF(CD_CONTA = '1.01.01', VL_CONTA, NULL)) AS CAIXA\n"
					+ "    FROM cias_bpas\n"
					+ "        WHERE VL_CONTA <> 0\n"
					+ "--         WHERE CD_CONTA IN ('3.99', \"3.04\", \"3.05\", \"3.11\")\n"
					+ "--         OR DS_CONTA IN ('PNA', 'ON')\n"
					+ "    GROUP BY CNPJ_CIA, DT_FIM_EXERC\n"
					+ "    )\n"
					+ "SELECT DISTINCT CNPJTOB3.CNPJ_CIA,\n"
					+ "        PRECOS.DT_REFER, \n"
					+ "        PRECOS.CD_B3, \n"
					+ "        PRECOS.CLOSE_PRICE, \n"
					+ "        DRE.RESULTADO_BRUTO - DRE.DESPESAS_COM_VENDAS - DRE.DESPESAS_GERAIS_ADM AS 'EBIT', -- ok\n"
					+ "        100*DRE.RESULTADO_BRUTO - DRE.DESPESAS_COM_VENDAS - DRE.DESPESAS_GERAIS_ADM/DRE.RECEITALIQUIDA AS 'MARGEM_EBIT%', -- ok\n"
					+ "        DRE.RESULTADO_BRUTO - DRE.DESPESAS_COM_VENDAS - DRE.DESPESAS_GERAIS_ADM + ABS(DRE.DEPAMO) AS 'EBITDA',\n"
					+ "        100*(DRE.RESULTADO_BRUTO - DRE.DESPESAS_COM_VENDAS - DRE.DESPESAS_GERAIS_ADM + ABS(DRE.DEPAMO))/DRE.RECEITALIQUIDA AS 'MARGEM_EBITDA%',\n"
					+ "        IF (CNPJTOB3.CD_B3 LIKE '%3',\n"
					+ "            PRECOS.CLOSE_PRICE/DRE.LPAON,\n"
					+ "            PRECOS.CLOSE_PRICE/DRE.LPAPN\n"
					+ "            ) AS 'P/L', -- ok\n"
					+ "        IF (CNPJTOB3.CD_B3 LIKE '%3', \n"
					+ "            (PRECOS.CLOSE_PRICE/((BPP.PL_CONSOLIDADO - BPP.PARTICIPACAO_ACIONISTAS_NAO_CONTROLADORES)/(DRE.LUCROLIQUIDO/DRE.LPAON))), \n"
					+ "            (PRECOS.CLOSE_PRICE/((BPP.PL_CONSOLIDADO - BPP.PARTICIPACAO_ACIONISTAS_NAO_CONTROLADORES)/(DRE.LUCROLIQUIDO/DRE.LPAPN)))\n"
					+ "        ) AS 'P/VP', -- ok\n"
					+ "--         usando o preço por ação x número de ações / ebit\n"
					+ "        IF(CNPJTOB3.CD_B3 LIKE '%3', \n"
					+ "            PRECOS.CLOSE_PRICE*(DRE.LUCROLIQUIDO/DRE.LPAON)/(DRE.RESULTADO_BRUTO - DRE.DESPESAS_COM_VENDAS - DRE.DESPESAS_GERAIS_ADM), \n"
					+ "            PRECOS.CLOSE_PRICE*(DRE.LUCROLIQUIDO/DRE.LPAPN)/(DRE.RESULTADO_BRUTO - DRE.DESPESAS_COM_VENDAS - DRE.DESPESAS_GERAIS_ADM))\n"
					+ "        AS 'P/EBIT', -- ok \n"
					+ "        IF(CNPJTOB3.CD_B3 LIKE '%3', \n"
					+ "            PRECOS.CLOSE_PRICE*(DRE.LUCROLIQUIDO/DRE.LPAON)/(DRE.RESULTADO_BRUTO - DRE.DESPESAS_COM_VENDAS - DRE.DESPESAS_GERAIS_ADM + ABS(DRE.DEPAMO)), \n"
					+ "            PRECOS.CLOSE_PRICE*(DRE.LUCROLIQUIDO/DRE.LPAPN)/(DRE.RESULTADO_BRUTO - DRE.DESPESAS_COM_VENDAS - DRE.DESPESAS_GERAIS_ADM + ABS(DRE.DEPAMO)))\n"
					+ "        AS 'P/EBITDA', -- ok \n"
					+ "        IF(CNPJTOB3.CD_B3 LIKE '%3', \n"
					+ "            (((BPP.EMPRESTIMOS_FINANCIAMENTOS_CIRCULANTE + BPP.EMPRESTIMOS_FINANCIAMENTOS_NAO_CIRCULANTE) + PRECOS.CLOSE_PRICE*(DRE.LUCROLIQUIDO/DRE.LPAON)) - BPA.CAIXA)/(DRE.RESULTADO_BRUTO - DRE.DESPESAS_COM_VENDAS - DRE.DESPESAS_GERAIS_ADM), \n"
					+ "            (((BPP.EMPRESTIMOS_FINANCIAMENTOS_CIRCULANTE + BPP.EMPRESTIMOS_FINANCIAMENTOS_NAO_CIRCULANTE) + PRECOS.CLOSE_PRICE*(DRE.LUCROLIQUIDO/DRE.LPAPN)) - BPA.CAIXA)/(DRE.RESULTADO_BRUTO - DRE.DESPESAS_COM_VENDAS - DRE.DESPESAS_GERAIS_ADM)\n"
					+ "        ) AS 'EV/EBIT', -- ok\n"
					+ "        IF(CNPJTOB3.CD_B3 LIKE '%3', \n"
					+ "            (((BPP.EMPRESTIMOS_FINANCIAMENTOS_CIRCULANTE + BPP.EMPRESTIMOS_FINANCIAMENTOS_NAO_CIRCULANTE) + PRECOS.CLOSE_PRICE*(DRE.LUCROLIQUIDO/DRE.LPAON)) - BPA.CAIXA)/(DRE.RESULTADO_BRUTO - DRE.DESPESAS_COM_VENDAS - DRE.DESPESAS_GERAIS_ADM + ABS(DRE.DEPAMO)),\n"
					+ "            (((BPP.EMPRESTIMOS_FINANCIAMENTOS_CIRCULANTE + BPP.EMPRESTIMOS_FINANCIAMENTOS_NAO_CIRCULANTE) + PRECOS.CLOSE_PRICE*(DRE.LUCROLIQUIDO/DRE.LPAPN)) - BPA.CAIXA)/(DRE.RESULTADO_BRUTO - DRE.DESPESAS_COM_VENDAS - DRE.DESPESAS_GERAIS_ADM + ABS(DRE.DEPAMO))\n"
					+ "        ) AS 'EV/EBITDA', -- ok\n"
					+ "        IF(CNPJTOB3.CD_B3 LIKE '%3',\n"
					+ "            ((BPP.EMPRESTIMOS_FINANCIAMENTOS_CIRCULANTE + BPP.EMPRESTIMOS_FINANCIAMENTOS_NAO_CIRCULANTE) + PRECOS.CLOSE_PRICE*(DRE.LUCROLIQUIDO/DRE.LPAON)) - BPA.CAIXA,\n"
					+ "            ((BPP.EMPRESTIMOS_FINANCIAMENTOS_CIRCULANTE + BPP.EMPRESTIMOS_FINANCIAMENTOS_NAO_CIRCULANTE) + PRECOS.CLOSE_PRICE*(DRE.LUCROLIQUIDO/DRE.LPAPN)) - BPA.CAIXA\n"
					+ "        ) AS 'EV', -- ok\n"
					+ "        IF(CNPJTOB3.CD_B3 LIKE '%3',\n"
					+ "            ((BPP.PL_CONSOLIDADO - BPP.PARTICIPACAO_ACIONISTAS_NAO_CONTROLADORES)/(DRE.LUCROLIQUIDO/DRE.LPAON)),\n"
					+ "            ((BPP.PL_CONSOLIDADO - BPP.PARTICIPACAO_ACIONISTAS_NAO_CONTROLADORES)/(DRE.LUCROLIQUIDO/DRE.LPAPN))\n"
					+ "        ) AS 'VPA', -- ok\n"
					+ "        100*DRE.LUCROLIQUIDO/(BPP.PL_CONSOLIDADO - BPP.PARTICIPACAO_ACIONISTAS_NAO_CONTROLADORES) AS 'ROE%',\n"
					+ "        100*(DRE.RESULTADO_BRUTO - DRE.DESPESAS_COM_VENDAS - DRE.DESPESAS_GERAIS_ADM)/(BPA.ATIVO - BPP.FORNECEDORES - BPA.CAIXA) AS 'ROIC%', \n"
					+ "        IF (CNPJTOB3.CD_B3 LIKE '%3', DRE.LPAON, DRE.LPAPN) AS 'LPA',\n"
					+ "        100*(DRE.RESULTADO_BRUTO - DRE.DESPESAS_COM_VENDAS - DRE.DESPESAS_GERAIS_ADM)/BPA.ATIVO AS 'EBIT/ATIVO%',\n"
					+ "        DRE.RECEITALIQUIDA/BPA.ATIVO AS 'GIRO_ATIVO',\n"
					+ "        100*(DRE.LUCROLIQUIDO/DRE.RECEITALIQUIDA) AS 'MARGEM_LIQUIDA%',\n"
					+ "        (BPA.ATIVO_CIRCULANTE/BPP.PASSIVO_CIRCULANTE) AS 'LIQUIDEZ_CORRENTE',\n"
					+ "        DRE.LUCROLIQUIDO as 'LUCRO_LIQUIDO',\n"
					+ "        DRE.RECEITALIQUIDA as 'RECEITA_LIQUIDA',\n"
					+ "        (BPP.PL_CONSOLIDADO - BPP.PARTICIPACAO_ACIONISTAS_NAO_CONTROLADORES) as 'PATRIMONIO_LIQUIDO',\n"
					+ "        BPP.PASSIVO - (BPP.PL_CONSOLIDADO - BPP.PARTICIPACAO_ACIONISTAS_NAO_CONTROLADORES) AS 'PASSIVO', -- o passivo ja inclui o PL\n"
					+ "        BPA.ATIVO AS 'ATIVO' -- O passivo é igual ao Ativo, A = P + PL, não é necessário mostrar o passivo\n"
					+ "FROM cias_precos_diarios PRECOS\n"
					+ "    JOIN cias_cods_b3s CNPJTOB3\n"
					+ "        ON PRECOS.CD_B3 = CNPJTOB3.CD_B3 \n"
					+ "    LEFT JOIN DRE\n"
					+ "        ON DRE.CNPJ_CIA = CNPJTOB3.CNPJ_CIA AND\n"
					+ "        PRECOS.DT_REFER BETWEEN DRE.DT_INI_EXERC AND DRE.DT_FIM_EXERC\n"
					+ "    LEFT JOIN BPP  -- não esquecer de subtrair o PL, pois o Passivo já é igual ao ativo pois foi somado ao PL\n"
					+ "        ON BPP.CNPJ_CIA = CNPJTOB3.CNPJ_CIA AND \n"
					+ "        BPP.DT_FIM_EXERC = DRE.DT_FIM_EXERC\n"
					+ "    LEFT JOIN BPA  -- não esquecer de subtrair o PL, pois o Passivo já é igual ao ativo pois foi somado ao PL\n"
					+ "        ON BPA.CNPJ_CIA = CNPJTOB3.CNPJ_CIA AND \n"
					+ "        BPA.DT_FIM_EXERC = DRE.DT_FIM_EXERC\n";
			Statement st = connection.prepareStatement(sql);
			ResultSet rs = st.executeQuery("EXPLAIN " + sql);
			System.out.println("Calculating query complexity, the operations behind this query are:");
			while (rs.next()) {
				System.out.println("Operating on table: " + rs.getString("table") + ", with " + rs.getInt("rows") + " rows, " + rs.getString("Extra"));
			}
			// optimize tables, first thing
			System.out.println("\nOptimizing relevant tables before the BIG query\n");
			String opt = "OPTIMIZE TABLE cias_precos_diarios, cias_cods_b3s ,cias_dres";
			st.execute(opt);
			connection.commit();
			System.out.println("\nThis is the new complexity, should be lower than before");
			rs = st.executeQuery("EXPLAIN " + sql);
			while (rs.next()) {
				System.out.println(rs.getString("table") + " " + rs.getInt("rows") + " " + rs.getString("Extra"));
			}
			System.out.println("\n\nExecuting the big query, brace!");
			st.execute(sql);
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return true;
	}

	/**
	 * Drops all the cia tables in the correct order.
	 *
	 * @return true
	 */
	public void dropCiaTables() {
		try {
			Connection connection = DriverManager.getConnection(DB_URL + databasename + "?&allowLoadLocalInfile=true", databaseUsername, databasePassword);
			connection.setAutoCommit(false);
			Statement st = connection.createStatement();
			String sql = "DROP TABLE IF EXISTS cias_bpas";
			st.execute(sql);
			sql = "DROP TABLE IF EXISTS cias_bpps";
			st.execute(sql);
			sql = "DROP TABLE IF EXISTS cias_precos_diarios";
			st.execute(sql);
			sql = "DROP TABLE IF EXISTS cias_dfcs_mds";
			st.execute(sql);
			sql = "DROP TABLE IF EXISTS cias_dmpls";
			st.execute(sql);
			sql = "DROP TABLE IF EXISTS cias_dres";
			st.execute(sql);
			sql = "DROP TABLE IF EXISTS cias_indicadores";
			st.execute(sql);
			sql = "DROP TABLE IF EXISTS cias_cods_b3s";
			st.execute(sql);
			sql = "DROP TABLE IF EXISTS cnpj_cias_abertas";
			st.execute(sql);
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public boolean fillCiaDatabases() {
		{
			System.out.println("\ninfo: Processando cadastro de Companhias de capital aberto");
			String localDVMSubdir = "CIA_ABERTA/CAD/DADOS/";
			String currentDirStr = this.localCVMDir + localDVMSubdir;
			System.out.println("\ninfo: Obtendo Meta de CIAS");
			String[] dataFiles = new File(currentDirStr).list(new FilenameFilter() {
				@Override
				public boolean accept(File f, String name) {
					// return only the consolidated csvs
					return name.endsWith(".csv");
				}
			});
			processaDir(dataFiles, localDVMSubdir, currentDirStr);
			CVMDatabaseManager.CheckDispatchNewThread();
			System.out.println("info: Aguardando " + total_num_threads + " threads terminarem");
			try {
				semaphoreAllDone.acquire();
			} catch (InterruptedException ex) {
			}
			CVMDatabaseThread.ResetTempoTotalThreads();
		}
		String localDVMSubdir = "CIA_ABERTA/DOC/ITR/DADOS/";
		String currentDirStr = this.localCVMDir + localDVMSubdir;
		{
			System.out.println("\ninfo: Obtendo BPAs");
			String[] dataFiles = new File(currentDirStr).list(new FilenameFilter() {
				@Override
				public boolean accept(File f, String name) {
					// return only the consolidated csvs
					return name.contains("BPA_con_") && name.endsWith(".csv");
				}
			});
			processaDir(dataFiles, localDVMSubdir, currentDirStr);
			CVMDatabaseManager.CheckDispatchNewThread();
			System.out.println("info: Aguardando " + total_num_threads + " threads terminarem");
			try {
				semaphoreAllDone.acquire();
			} catch (InterruptedException ex) {
			}
			CVMDatabaseThread.ResetTempoTotalThreads();
		}
		{
			// String localDVMSubdir = "CIA_ABERTA/DOC/DFP/BPP/DADOS/";
			// String currentDirStr = this.localCVMDir + localDVMSubdir;
			System.out.println("\ninfo: Obtendo BPPs");
			String[] dataFiles = new File(currentDirStr).list(new FilenameFilter() {
				@Override
				public boolean accept(File f, String name) {
					// return only the consolidated csvs
					return name.contains("BPP_con_") && name.endsWith(".csv");
				}
			});
			processaDir(dataFiles, localDVMSubdir, currentDirStr);
			CVMDatabaseManager.CheckDispatchNewThread();
			System.out.println("info: Aguardando " + total_num_threads + " threads terminarem");
			try {
				semaphoreAllDone.acquire();
			} catch (InterruptedException ex) {
			}
			CVMDatabaseThread.ResetTempoTotalThreads();
		}
		{
			// String localDVMSubdir = "CIA_ABERTA/DOC/ITR/DADOS/";
			// String currentDirStr = this.localCVMDir + localDVMSubdir;
			System.out.println("\ninfo: Obtendo DREs");
			String[] dataFiles = new File(currentDirStr).list(new FilenameFilter() {
				@Override
				public boolean accept(File f, String name) {
					// return only the consolidated csvs
					return name.contains("DRE_con_") && name.endsWith(".csv");
				}
			});
			processaDir(dataFiles, localDVMSubdir, currentDirStr);
			CVMDatabaseManager.CheckDispatchNewThread();
			System.out.println("info: Aguardando " + total_num_threads + " threads terminarem");
			try {
				semaphoreAllDone.acquire();
			} catch (InterruptedException ex) {
			}
			CVMDatabaseThread.ResetTempoTotalThreads();
		}
		{
			// String localDVMSubdir = "CIA_ABERTA/DOC/ITR/DADOS/";
			// String currentDirStr = this.localCVMDir + localDVMSubdir;
			System.out.println(currentDirStr);
			System.out.println("\ninfo: Obtendo DMPLs");
			String[] dataFiles = new File(currentDirStr).list(new FilenameFilter() {
				@Override
				public boolean accept(File f, String name) {
					// return only the consolidated csvs
					return name.contains("DMPL_con_") && name.endsWith(".csv");
				}
			});
			processaDir(dataFiles, localDVMSubdir, currentDirStr);
			CVMDatabaseManager.CheckDispatchNewThread();
			System.out.println("info: Aguardando " + total_num_threads + " threads terminarem");
			try {
				semaphoreAllDone.acquire();
			} catch (InterruptedException ex) {
			}
			CVMDatabaseThread.ResetTempoTotalThreads();
		}
		{
			// String localDVMSubdir = "CIA_ABERTA/DOC/DFP/DFC_MD/DADOS/";
			// String currentDirStr = this.localCVMDir + localDVMSubdir;
			System.out.println(currentDirStr);
			System.out.println("\ninfo: Obtendo DFCs");
			String[] dataFiles = new File(currentDirStr).list(new FilenameFilter() {
				@Override
				public boolean accept(File f, String name) {
					// return only the consolidated csvs
					return name.contains("DFC_MD_con_") && name.endsWith(".csv");
				}
			});
			processaDir(dataFiles, localDVMSubdir, currentDirStr);
			CVMDatabaseManager.CheckDispatchNewThread();
			System.out.println("info: Aguardando " + total_num_threads + " threads terminarem");
			try {
				semaphoreAllDone.acquire();
			} catch (InterruptedException ex) {
			}
			CVMDatabaseThread.ResetTempoTotalThreads();
		}
		{
			localDVMSubdir = "B3/";
			currentDirStr = localDVMSubdir;
			System.out.println(currentDirStr);
			System.out.println("\ninfo: Obtendo Codigos B3");
			String[] dataFiles = new File(currentDirStr).list(new FilenameFilter() {
				@Override
				public boolean accept(File f, String name) {
					// return only the consolidated csvs
					return (name.contains("Only3") || name.contains("Only4")) && name.endsWith(".csv");
				}
			});
			processaDir(dataFiles, localDVMSubdir, currentDirStr);
			CVMDatabaseManager.CheckDispatchNewThread();
			System.out.println("info: Aguardando " + total_num_threads + " threads terminarem");
			try {
				semaphoreAllDone.acquire();
			} catch (InterruptedException ex) {
			}
			CVMDatabaseThread.ResetTempoTotalThreads();
		}
		{
			localDVMSubdir = "CIA_ABERTA/DOC/DFP/B3/PRICES/";
			currentDirStr = this.localCVMDir + localDVMSubdir;
			System.out.println(currentDirStr);
			System.out.println("\ninfo: Obtendo Preços Diarios");
			String[] dataFiles = new File(currentDirStr).list(new FilenameFilter() {
				@Override
				public boolean accept(File f, String name) {
					// return only the consolidated csvs
					return name.endsWith(".TXT");
				}
			});
			processaDir(dataFiles, localDVMSubdir, currentDirStr);
			CVMDatabaseManager.CheckDispatchNewThread();
			System.out.println("info: Aguardando " + total_num_threads + " threads terminarem");
			try {
				semaphoreAllDone.acquire();
			} catch (InterruptedException ex) {
			}
			CVMDatabaseThread.ResetTempoTotalThreads();
		}
		this.makeFixesToData();
		return true;
	}

	private void makeFixesToData() {
		try {
			System.out.println("deleting PENULTIMOS...");

			Connection connection = DriverManager.getConnection(DB_URL + databasename + "?&allowLoadLocalInfile=true", databaseUsername, databasePassword);
			connection.setAutoCommit(false);
			Statement st = connection.createStatement();

			String sql = "DELETE FROM cias_dfcs_mds WHERE ORDEM_EXERC <> \'ÚLTIMO\'";
			st.addBatch(sql);
			System.out.println("deleted PENULTIMOS from DFC");

			sql = "DELETE FROM cias_dmpls WHERE ORDEM_EXERC <> \'ÚLTIMO\'";
			st.addBatch(sql);
			System.out.println("deleted PENULTIMOS from DMPL");

			sql = "DELETE FROM cias_dres WHERE ORDEM_EXERC <> \'ÚLTIMO\'";
			st.addBatch(sql);
			System.out.println("deleted PENULTIMOS from DRE");

			sql = "DELETE FROM cias_bpas WHERE ORDEM_EXERC <> \'ÚLTIMO\'";
			st.addBatch(sql);
			System.out.println("deleted PENULTIMOS from BPA");

			sql = "DELETE FROM cias_bpps WHERE ORDEM_EXERC <> \'ÚLTIMO\'";
			st.addBatch(sql);
			System.out.println("deleted PENULTIMOS from BPP");

			st.executeBatch();
			connection.commit();

			st.clearBatch();

			sql = "update cias_dres SET DT_INI_EXERC = IF (MONTH(DT_INI_EXERC) = 6 AND DAY(DT_INI_EXERC) = 1, DATE_ADD(DT_INI_EXERC, INTERVAL 1 MONTH), DT_INI_EXERC)";
			st.addBatch(sql);
			System.out.println("Fix incorrect dates");
			st.executeBatch();
			connection.commit();

			st.clearBatch();

			// removing columns where the date interval does not represent an interval
			sql = "DELETE FROM cias_dres WHERE TIMESTAMPDIFF(MONTH, DT_INI_EXERC, DT_FIM_EXERC) > 4";
			st.addBatch(sql);
			System.out.println("deleting columns where the date interval does not represent an interval IN DRE");

			sql = "DELETE FROM cias_dmpls WHERE TIMESTAMPDIFF(MONTH, DT_INI_EXERC, DT_FIM_EXERC) > 4";
			st.addBatch(sql);
			System.out.println("deleting columns where the date interval does not represent an interval in DMPL");

			st.executeBatch();
			connection.commit();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
