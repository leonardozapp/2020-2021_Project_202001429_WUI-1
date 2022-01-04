/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package investfunds2ndoption;

import investfunds.ActionResultLogger;
import investfunds.ThreadCompleteListener;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author rlcancian
 */
public class CVS2SQLConverter implements ThreadCompleteListener {

	private final String databasename;
	private final String databaseUsername;
	private final String databasePassword;
	private final String localCVMDir;
	private final String JDBC_DRIVER;
	private final String DB_URL;
	private final static List<CSV2SQLThreadSQLGen> threadPool = Collections.synchronizedList(new ArrayList<>());
	private static int MAX_RUNNING_THREADS = 4;
	private int total_num_threads;
	private int newCSVThreadID = 0;
	//private int countErros = 0;
	private ActionResultLogger actionResultLogger;
	private static final Semaphore semaphoreAllDone = new Semaphore(0);

	private static void AddThread(CSV2SQLThreadSQLGen thread) {
		CVS2SQLConverter.AddThread(thread, true);
	}

	private static void AddThread(CSV2SQLThreadSQLGen thread, boolean startRunning) {
		synchronized (threadPool) {
			threadPool.add(thread);
		}
		/*
		synchronized (threadPool) {
			CustomComparator comparator = new CustomComparator();
			Collections.sort(threadPool, comparator);
			comparator = null;
		}
		 */
		int pos = threadPool.indexOf(thread);
		System.out.println("info: [" + thread.getCSVThreadID() + "] thread incluída na posição " + pos + " da lista para processamento do arquivo " + thread.getDatafilename() + " de tamanho " + thread.getDatafilelength() + " bytes");
		if (startRunning) {
			CVS2SQLConverter.CheckDispatchNewThread();
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
			int registrosComErro = ((CSV2SQLThreadSQLGen) (thread)).getCountErrosRegistros();
			threadPool.remove(thread);
			size = threadPool.size();
		}
		if (total_num_threads > 0) {
			System.out.println("info: [" + ((CSV2SQLThreadSQLGen) thread).getCSVThreadID() + "] thread removida da lista para execução> Há " + size + " threads restantes (" + size + "/" + total_num_threads + " = " + 100 * size / total_num_threads + "% restantes)");
		}
		if (size > 0) {
			CVS2SQLConverter.CheckDispatchNewThread();
		} else {
			System.out.println("info: TODAS AS THREADS TERMINARAM. FIM DO PROCESSAMENTO.");
			semaphoreAllDone.release();
		}
	}

	private static void CheckDispatchNewThread() {
		try {
			MAX_RUNNING_THREADS = Integer.valueOf(new Scanner(new File("./MAX_RUNNING_THREADS.txt")).useDelimiter("\\Z").next());
		} catch (FileNotFoundException | NumberFormatException ex) {
			Logger.getLogger(CVS2SQLConverter.class.getName()).log(Level.SEVERE, null, ex);
		}
		synchronized (threadPool) {
			// must be in synchronized block 
			Iterator it = threadPool.iterator();
			int countRunning = 0;
			long totalFileSizes = 0;
			CSV2SQLThreadSQLGen t;
			while (it.hasNext()) {
				//System.out.println(it.next());
				t = (CSV2SQLThreadSQLGen) it.next();
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
				t = (CSV2SQLThreadSQLGen) it.next();
				if (!t.isAlive()) {
					System.out.println("info: Thread " + ((CSV2SQLThreadSQLGen) t).getCSVThreadID() + " pode iniciar a executar (" + countRunning + "/" + MAX_RUNNING_THREADS + ") " + " [" + totalFileSizes / 1e6 + "MB para processar]" + ". Há " + threadPool.size() + " threads na fila");
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

	CVS2SQLConverter(String databasename, String databaseUsername, String databasePassword, String localCVMDir, String JDBC_DRIVER, String DB_URL) {
		this.databasename = databasename;
		this.databaseUsername = databaseUsername;
		this.databasePassword = databasePassword;
		this.localCVMDir = localCVMDir;
		this.JDBC_DRIVER = JDBC_DRIVER;
		this.DB_URL = DB_URL;
	}

	/**
	 *
	 * @return
	 */
	public boolean CVMRunsOverDirsAndFindMetaAndData() {         //throws FileNotFoundException, IOException, ClassNotFoundException, SQLException {
		_CVMRunsOverDirsAndFindMetaAndData(this.localCVMDir);
		total_num_threads = threadPool.size();
		System.out.println("info: Aguardando " + total_num_threads + " threads terminarem");
		CVS2SQLConverter.CheckDispatchNewThread();
		try {
			semaphoreAllDone.acquire();
		} catch (InterruptedException ex) {
			Logger.getLogger(CVS2SQLConverter.class.getName()).log(Level.SEVERE, null, ex);
		}
		return true;
	}

	private void _CVMRunsOverDirsAndFindMetaAndData(String actualDir) {//throws FileNotFoundException, IOException, ClassNotFoundException, SQLException {
		System.out.println("info: Procurando na pasta " + actualDir);
		File currentDir = new File(actualDir);
		String[] directories = currentDir.list(new FilenameFilter() {
			@Override
			public boolean accept(File current, String name) {
				return new File(current, name).isDirectory();
			}
		});
		//System.out.println(Arrays.toString(directories));
		Arrays.sort(directories, (a, b) -> a.compareTo(b));
		List<String> directoriesList = Arrays.asList(directories);
		int indexOfMeta = directoriesList.indexOf("META");
		int indexOfData = directoriesList.indexOf("DADOS");
		if (indexOfMeta == -1 && indexOfData == -1) {
			// varre recursivamente procurando pelas pastas META e DADOS
			for (String directorie : directories) {
				_CVMRunsOverDirsAndFindMetaAndData(actualDir + directorie + "/");
			}
		} else { // achou
			File metaDir = new File(actualDir + directories[indexOfMeta]);
			String[] metaFiles = metaDir.list(new FilenameFilter() {
				@Override
				public boolean accept(File f, String name) {
					return name.endsWith(".txt");
				}
			});
			//Arrays.sort(metaFiles, (a, b) -> a.compareTo(b));
			Arrays.sort(metaFiles, new java.util.Comparator<String>() {
				@Override
				public int compare(String s1, String s2) {
					return s2.length() - s1.length();// comparision
				}
			});
			//System.out.println(Arrays.toString(metaFiles));
			File dataDir = new File(actualDir + directories[indexOfData]);
			String[] dataFiles = dataDir.list(new FilenameFilter() {
				@Override
				public boolean accept(File f, String name) {
					// TODO: PROCESSANDO APENAS ARQUIVOS MAIS RECENTES
					return name.endsWith(".csv");
				}
			});
			Arrays.sort(dataFiles, (a, b) -> a.compareTo(b));
			//System.out.println("Encontrados arquivos de metadados: " + Arrays.toString(metaFiles));
			//System.out.println("Encontrados arquivos de dados: " + Arrays.toString(dataFiles));
			ArrayList<CSVMetadata> csvMetadataList = new ArrayList<>();
			CSVMetadata csvMetadata;
			for (String metaFile : metaFiles) {
				//System.out.println("info:  Analisando metadados do arquivo " + metaFile);
				csvMetadata = createCSVMetadataFromFile(metaDir.toString() + "/" + metaFile);
				//System.out.println("info:     Os metadados possuem " + csvMetadata.getCSVFields().size() + " campos: " + csvMetadata.getCSVFields().toString());
				csvMetadataList.add(csvMetadata);
			}
			CSV2SQLThreadSQLGen thread;
			for (int i = 0; i < dataFiles.length; i++) {

				ActionResultLogger.ActionResult result = this.actionResultLogger.getActionResult(new ActionResultLogger.ActionResult(ActionResultLogger.Action.FILE_PROCESSED.toString(), actualDir + directories[indexOfData], dataFiles[i], null, Boolean.FALSE, null, null, Boolean.FALSE));
				if (result == null) {

					System.out.println("info:  Processando dados do arquivo " + dataFiles[i]);
					csvMetadata = BuscaMetadadosDeArquivoDeDados(dataDir.toString() + "/" + dataFiles[i], csvMetadataList);
					if (csvMetadata == null) {
						System.out.println("ERRO: WARNING: Não foi possível identificar a estrutura dos dados do arquivo " + dataFiles[i]);
						actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.CREATE_METADATA.toString(), null, dataFiles[i], null, Boolean.TRUE, "Não foi possível identificar a estrutura dos dados do arquivo", new Date(), Boolean.TRUE));
					} else {// processa o arquivo de dados conhecendo seus metadados
						// TODO: MUDAR ABAIXO. RETIRAR RESTRIÇÃO PARA O ANO 2020
						if (true) {
							//if (dataFiles[i].contains("2020") || dataFiles[i].contains("2019") || !dataFiles[i].contains("20")) { 
							// NEW THREAD      
							newCSVThreadID++;
							thread = new CSV2SQLThreadSQLGen(newCSVThreadID, dataDir.toString(), dataFiles[i], csvMetadata, JDBC_DRIVER, DB_URL, databasename, databaseUsername, databasePassword);
							thread.addListener(this);
							thread.setActionResultLogger(actionResultLogger);
							CVS2SQLConverter.AddThread(thread, false);
						}
					}
				} else {
					////System.out.println("done:  Arquivo previamente processado " + dataFiles[i]);
				}
			}
		}
	}

	private CSVMetadata BuscaMetadadosDeArquivoDeDados(String datafilepath, ArrayList<CSVMetadata> csvMetadataList) {
		File datafile = new File(datafilepath);
		String datafilename = datafile.getName().toLowerCase();
		String metafilename;
		int count = 0;
		CSVMetadata csvMetadataCorreto = null;
		String metafilenameCorreto = "";
		for (CSVMetadata csvMetadata : csvMetadataList) {
			metafilename = csvMetadata.getFilename();
			metafilename = metafilename.substring(5, metafilename.length() - 4).toLowerCase(); // retira meta_ e .txt
			if (datafilename.contains(metafilename) && metafilename.length() > metafilenameCorreto.length()) {
				//System.out.println("O arquivo de dados "+datafilename+" seque a estrutura de "+csvMetadata.getFilename());
				// deveria fazer mais testes para ver se essa mesma a estrutura. Ler a primeira linha do arquivo de dados e comparar os campos resolveria
				csvMetadataCorreto = csvMetadata;
				metafilenameCorreto = metafilename;
				//System.out.println("-->" + csvMetadata.getFilename());
				count++;
			}
		}
		if (count == 1) {
			//System.out.println("info:    O arquivo " + datafilename + " segue a estrutura de " + csvMetadataCorreto.getFilename());
			return csvMetadataCorreto;
		} else if (count > 1) {
			System.out.println("WARN:    O arquivo " + datafilename + " pode seguir mais de uma estrutura possível");
			return csvMetadataCorreto;
		}
		return null;
	}

	private CSVMetadata createCSVMetadataFromFile(String metafilename) {
		BufferedReader lineReader;
		try {
			//throws FileNotFoundException, IOException {
			File metafile = new File(metafilename);
			CSVMetadata metadata = new CSVMetadata();
			metadata.setPath(metafile.getParent());
			metadata.setFilename(metafile.getName());
			lineReader = new BufferedReader(new FileReader(metafilename));
			String lineText = lineReader.readLine(); // pula primeira linha
			String campo = "", dominio = "", descricao = "", tipo = "";
			int tamanho = -1, escala = -1, precisao = -1;
			while ((lineText = lineReader.readLine()) != null) {
				if (lineText.equals("")) { // uma linha em branco indica que um campo acabou de ser lido
					CSVField field = new CSVField();
					field.setCampo(campo);
					field.setDescricao(descricao);
					field.setDominio(dominio);
					field.setTipo(tipo);
					field.setTamanho(tamanho);
					field.setPrecisao(precisao);
					field.setEscala(escala);
					field.setIndexInHeaderArray(-1);// será conhecido apenas após ler o header do arquivo de dados
					metadata.getCSVFields().add(field);
					tamanho = -1;
					escala = -1;
					precisao = -1;
				} else {
					String[] data = lineText.split(":");
					if (data.length == 2) {
						data[0] = data[0].trim();
						data[1] = data[1].trim();
						switch (data[0]) {
							case "Campo":
								campo = data[1];
								break;
							//Descri��o
							case "Descri��o":
							case "Descrição":
								descricao = data[1];
								break;
							case "Dom�nio":
							case "Domínio":
								dominio = data[1];
								break;
							case "Tipo Dados":
								tipo = data[1];
								break;
							case "Tamanho":
								if (data[1].equals("")) {
									tamanho = -1;
								} else {
									tamanho = Integer.valueOf(data[1]);
								}
								break;
							case "Precis�o":
							case "Precisão":
								if (data[1].equals("")) {
									precisao = -1;
								} else {
									precisao = Integer.valueOf(data[1]);
								}
								break;
							case "Scale":
								if (data[1].equals("")) {
									escala = -1;
								} else {
									escala = Integer.valueOf(data[1]);
								}
								break;
							default:
								System.out.println("WARN: Informação não compreendida: " + data[0]);
								break;
						}
					}
				}
			}
			// verifica se o último campo do arquivo foi inserido (só acontece quando o arquivo termina com uma linha em branco, que é a condição para inserção do campo
			boolean found = false;
			for (CSVField field : metadata.getCSVFields()) {
				if (field.getCampo().equals(campo)) {
					found = true;
					break;
				}
			}
			if (!found) {
				CSVField field = new CSVField();
				field.setCampo(campo);
				field.setDescricao(descricao);
				field.setDominio(dominio);
				field.setTipo(tipo);
				field.setTamanho(tamanho);
				field.setPrecisao(precisao);
				field.setEscala(escala);
				field.setIndexInHeaderArray(-1);// será conhecido apenas após ler o header do arquivo de dados
				metadata.getCSVFields().add(field);
			}
			try {
				lineReader.close();
			} catch (IOException ex) {
				Logger.getLogger(CVS2SQLConverter.class.getName()).log(Level.SEVERE, null, ex);
				actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.CREATE_METADATA.toString(), null, metafilename, null, Boolean.TRUE, ex.getMessage(), new Date(), Boolean.TRUE));
			}
			return metadata;
		} catch (IOException | NumberFormatException ex) {
			Logger.getLogger(CVS2SQLConverter.class.getName()).log(Level.SEVERE, null, ex);
			actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.CREATE_METADATA.toString(), null, metafilename, null, Boolean.TRUE, ex.getMessage(), new Date(), Boolean.TRUE));
		}
		return null;
	}

	//public void ReportErrorProcessingFile(String filepath, Exception ex, String datetime, String methodOrDescription) {
	//}

	/**
	 *
	 * @return
	 */
	public boolean createDBInfra() {
		try {
			System.out.println("Creating database infra...");
			Connection connection;
			Class.forName(this.JDBC_DRIVER);
			connection = DriverManager.getConnection(this.DB_URL + this.databasename, this.databaseUsername, this.databasePassword);
			connection.setAutoCommit(false);
			/*
			String sql = "CREATE TABLE IF NOT EXISTS " + this.databasename + ".MAN_arquivo_processado ("
					+ "  ID_ARQUIVO_PROCESSADO int unsigned NOT NULL AUTO_INCREMENT,"
					+ "  PATH_ARQUIVO varchar	(400) NOT NULL,"
					+ "  TAMANHO_ARQUIVO int unsigned NOT NULL,"
					+ "  DATA_PROCESSADO varchar(45) NOT NULL,"
					+ "  NUM_CAMPOS int NOT NULL,"
					+ "  NUM_REGISTROS int DEFAULT NULL,"
					+ "  STATUS int DEFAULT NULL,"
					+ "  PRIMARY KEY (ID_ARQUIVO_PROCESSADO)"
					+ ") ENGINE = InnoDB AUTO_INCREMENT = 1219 DEFAULT CHARSET = utf8mb4;";
			 */
			String sql = "USE " + this.databasename + ";";
			Statement st = connection.createStatement();
			st.execute(sql);
			connection.commit();
			sql = "CREATE TABLE IF NOT EXISTS `logs_atividades` ("
					+ "  `id` int NOT NULL AUTO_INCREMENT, "
					+ "  `action` varchar(100) NOT NULL, "
					+ "  `remoteURI` varchar(400) DEFAULT NULL, "
					+ "  `localURI` varchar(400) DEFAULT NULL, "
					+ "  `result` bigint DEFAULT NULL, "
					+ "  `HasErrors` int DEFAULT NULL, "
					+ "  `message` varchar(400) DEFAULT NULL, "
					+ "  `date` datetime NOT NULL, "
					+ "  PRIMARY KEY (`id`) "
					+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"; //ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;";
			st.execute(sql);
			sql = "CREATE TABLE IF NOT EXISTS `inf_diario_fi` ("
					+ "  `ID_CNPJ` int(11) NOT NULL, "
					+ "  `DT_COMPTC` date NOT NULL, "
					+ "  `VL_TOTAL` decimal(17,2) NOT NULL, "
					+ "  `VL_QUOTA` decimal(17,2) NOT NULL, "
					+ "  `VL_PATRIM_LIQ` decimal(17,2) NOT NULL, "
					+ "  `CAPTC_DIA` decimal(17,2) NOT NULL, "
					+ "  `RESG_DIA` decimal(17,2) NOT NULL, "
					+ "  `NR_COTST` int(10) NOT NULL, "
					+ "  PRIMARY KEY (`ID_CNPJ`,`DT_COMPTC`)"
					+ ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;";
			st.execute(sql);
			connection.commit();
			connection.close();
			return true;
		} catch (SQLException | ClassNotFoundException ex) {
			Logger.getLogger(CVS2SQLConverter.class.getName()).log(Level.SEVERE, null, ex);
		}
		return false;
	}

	/**
	 * @param actionResultLogger the actionResultLogger to set
	 */
	public void setActionResultLogger(ActionResultLogger actionResultLogger) {
		this.actionResultLogger = actionResultLogger;
	}

}
