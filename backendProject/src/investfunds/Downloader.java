/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package investfunds;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipInputStream;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

/**
 *
 * @author rlcancian
 */
public class Downloader implements ThreadCompleteListener {

	private final static List<Thread> threadPool = Collections.synchronizedList(new ArrayList<>());
	private static int MAX_RUNNING_THREADS = 1;
	private ActionResultLogger actionResultLogger;
	private final Semaphore semaphore = new Semaphore(1);
	private final boolean forceDownload;
	private final String[] diretoriosInteresse = {"./CVM/dados/"};// {"./CVM/dados/FI/"};
	//private ZipInputStream zis = null;

	/**
	 *
	 * @param forceDownload
	 */
	public Downloader(boolean forceDownload) {
		this.forceDownload = forceDownload;
	}

	private static void AddThread(Thread thread) {
		synchronized (threadPool) {
			threadPool.add(thread);
		}
		int pos = threadPool.indexOf(thread);
		System.out.println("info: [" + thread.getId() + "] thread incluída na posição " + pos + " da lista para processamento do arquivo " + thread.getName());
		Downloader.CheckDispatchNewThread();
	}

	private static void CheckDispatchNewThread() {
		try {
			MAX_RUNNING_THREADS = Integer.valueOf(new Scanner(new File("./MAX_RUNNING_THREADS.txt")).useDelimiter("\\Z").next());
		} catch (Exception ex) {
			//Logger.getLogger(CVS2SQLConverter.class.getName()).log(Level.SEVERE, null, ex);
		}
		synchronized (threadPool) {
			// must be in synchronized block 
			Iterator it = threadPool.iterator();
			int countRunning = 0;
			Thread t;
			while (it.hasNext()) {
				//System.out.println(it.next());
				t = (Thread) it.next();
				if (t.isAlive()) {
					countRunning++;
				}
			}
			if (countRunning >= MAX_RUNNING_THREADS) {
				System.out.println("info: Quantidade de " + countRunning + " threads executando já atingiu o limite máximo de " + MAX_RUNNING_THREADS + ". Há " + threadPool.size() + " threads na fila");
			}
			it = threadPool.iterator();
			while (it.hasNext() && countRunning < MAX_RUNNING_THREADS) {
				t = (Thread) it.next();
				if (!t.isAlive()) {
					System.out.println("info: Thread " + t.getId() + " pode iniciar a executar (" + countRunning + "/" + MAX_RUNNING_THREADS + ")" + ". Há " + threadPool.size() + " threads na fila");
					t.start();
					countRunning++;
				}
			}
		}
	}

	/**
	 *
	 * @param size
	 * @return
	 */
	public static String FileSizeStr(long size) {
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

	/**
	 *
	 * @param thread
	 */
	@Override
	public void notifyOfThreadComplete(Thread thread) {
		synchronized (threadPool) {
			threadPool.remove(thread);
		}
		System.out.println("info: [" + thread.getId() + "] Thread terminou (" + thread.getName() + ") Ainda há " + threadPool.size() + " threads na fila");
		Downloader.CheckDispatchNewThread();
	}

	/**
	 *
	 * @param threadID
	 * @param fileZip
	 * @param destDirStr
	 * @return
	 */
	public boolean uncompress(long threadID, String fileZip, String destDirStr) {
		try {
			semaphore.acquire(); // mutual exclusive access is unnecessary. It's here for debugging purposes only (for a while)
		} catch (InterruptedException ex) {
			Logger.getLogger(Downloader.class.getName()).log(Level.SEVERE, null, ex);
			actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.UNCOMPRESSED.toString(), null, fileZip, null, Boolean.TRUE, ex.getMessage(), new Date(), Boolean.TRUE));
		}
		String id = "";
		if (threadID > 0) {
			id = "[" + threadID + "] ";
		}

		boolean isOk = true;
		var outputPath = Path.of(destDirStr);
		System.out.println("ação:   " + id + "Descompactando arquivo zip " + fileZip);

		try (var zf = new ZipFile(fileZip)) {
			// Delete if exists, then create a fresh empty directory to put the zip archive contents
			//initialize(outputPath);
			Enumeration<? extends ZipEntry> zipEntries = zf.entries();
			Iterator<? extends ZipEntry> it = zipEntries.asIterator();
			ZipEntry entry;
			while (it.hasNext()) {
				//zipEntries.asIterator().forEachRemaining(entry -> {
				entry = it.next();
				try {
					if (entry.isDirectory()) {
						var dirToCreate = outputPath.resolve(entry.getName());
						Files.createDirectories(dirToCreate);
					} else {
						var fileToCreate = outputPath.resolve(entry.getName());
						ActionResultLogger.ActionResult result = actionResultLogger.getActionResult(new ActionResultLogger.ActionResult(ActionResultLogger.Action.EXTRACTED.toString(), fileZip, entry.getName(), null, Boolean.FALSE, null, null, Boolean.FALSE));
						if (result == null) {
							System.out.print("ação:      " + id + "Descompactando item compactado " + entry.getName());
							Files.deleteIfExists(outputPath.resolve(entry.getName()));
							Files.copy(zf.getInputStream(entry), fileToCreate);
							actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.EXTRACTED.toString(), fileZip, entry.getName(), entry.getSize(), Boolean.FALSE, null, new Date(), Boolean.FALSE));
							System.out.println(" (" + Downloader.FileSizeStr(fileToCreate.toFile().length()) + ")");
						} else {
							////System.out.print("done:      " + id + "Arquivo previamente descompactado em " + result.date.toString() + " : " + entry.getName());
						}
					}
				} catch (IOException ei) {
					isOk = false;
					actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.UNCOMPRESSED.toString(), fileZip, entry.getName(), null, Boolean.TRUE, ei.getMessage(), new Date(), Boolean.TRUE));
					Logger.getLogger(Downloader.class.getName()).log(Level.SEVERE, null, ei);
				}
				//});
			}
		} catch (IOException e) {
			isOk = false;
			actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.UNCOMPRESSED.toString(), null, fileZip, null, Boolean.TRUE, e.getMessage(), new Date(), Boolean.TRUE));
			Logger.getLogger(Downloader.class.getName()).log(Level.SEVERE, null, e);
		} finally {

		}

		semaphore.release();
		Thread.yield();
		return isOk;
		//}
	}

	/**
	 *
	 * @param localFile
	 */
	public void uncompressDownloadedFile(String localFile) {
		NotifyingThread thread = new NotifyingThread() {

			@Override
			public void doRun() {
				File f = new File(localFile);
				String localDir = f.getParent() + "/";
				boolean isOk = uncompress(this.getId(), localFile, localDir);
				if (isOk) {
					Downloader.this.actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.UNCOMPRESSED.toString(), null, localFile, f.length(), Boolean.FALSE, null, new Date(), Boolean.FALSE));
				} else {
					System.out.println("ERRO: [" + this.getId() + "] ERRO ao descompactar o arquivo " + localFile);
					//Logger.getLogger(Downloader.class.getName()).log(Level.SEVERE, null, ex);
				}
				Thread.yield();
			}
		};
		thread.setName(localFile);
		thread.addListener(this);
		Downloader.AddThread(thread);
		Thread.yield();
	}

	/**
	 *
	 * @param remoteFile
	 * @param localFile
	 */
	public void downloadFile(String remoteFile, String localFile) {
		NotifyingThread thread = new NotifyingThread() {

			@Override
			public void doRun() {
				System.out.println("ação: [" + this.getId() + "] Download iniciado do arquivo " + localFile);
				BufferedInputStream in = null;
				String tempLocalFile = localFile + ".temp";
				try {
					Files.deleteIfExists(new File(tempLocalFile).toPath());
				} catch (IOException ex) {
					Logger.getLogger(Downloader.class.getName()).log(Level.SEVERE, null, ex);
				}
				Date inicioDownload = new Date();
				try {
					//System.out.print(remoteFile);
					in = new BufferedInputStream(new URL(remoteFile).openStream());
				} catch (Exception ex) {
					Logger.getLogger(Downloader.class.getName()).log(Level.SEVERE, null, ex);
					actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.DOWNLOAD.toString(), remoteFile, localFile, null, Boolean.TRUE, ex.getMessage(), new Date(), Boolean.TRUE));
					return;
				}
				FileOutputStream fileOutputStream = null;
				try {
					fileOutputStream = new FileOutputStream(tempLocalFile); // salva primeiro num arquivo temporário
				} catch (FileNotFoundException ex) {
					Logger.getLogger(Downloader.class.getName()).log(Level.SEVERE, null, ex);
					actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.DOWNLOAD.toString(), remoteFile, localFile, null, Boolean.TRUE, ex.getMessage(), new Date(), Boolean.TRUE));
					return;
				}
				byte dataBuffer[] = new byte[1024];
				int bytesRead;
				long size = 0;
				try {
					while ((bytesRead = in.read(dataBuffer, 0, 1024)) != -1) {
						fileOutputStream.write(dataBuffer, 0, bytesRead);
						size += bytesRead;
					}
					fileOutputStream.close();
					in.close();
				} catch (IOException ex) {
					Logger.getLogger(Downloader.class.getName()).log(Level.SEVERE, null, ex);
					actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.DOWNLOAD.toString(), remoteFile, localFile, null, Boolean.TRUE, ex.getMessage(), new Date(), Boolean.TRUE));
					return;
				}
				//long tempFileLength = new File(tempLocalFile).length();
				// informa que o download foi concluído (e compara tamanhos??)
				Date finalDownload = new Date();
				double duration = (finalDownload.getTime() - inicioDownload.getTime()) / 1000.0;
				double rate = ((int) (size / duration)) / 1000000.0;
				File finalFile = new File(localFile);
				File tempFile = new File(tempLocalFile);
				System.out.println("info: [" + this.getId() + "] Download de " + Downloader.FileSizeStr(size) + " concluído em " + duration + " segundos (" + rate + "MB/s)");

				boolean isOk = true;
				if (finalFile.exists()) {
					if (finalFile.length() != tempFile.length() || tempFile.length() != size) {
						// TAMANHOS DIFEREM!!
						actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.DOWNLOAD.toString(), remoteFile, localFile, null, Boolean.TRUE, "Download com tamanho de " + tempFile.length() + " bytes difere do tamanho de arquivo prévio de " + finalFile.length() + " bytes", new Date(), Boolean.TRUE));
						System.out.println("ERRO: [" + this.getId() + "] !WARNING!: Download com tamanho de " + tempFile.length() + " bytes difere do tamanho de arquivo prévio de " + finalFile.length() + " bytes");
						//if (finalFile.length() < tempFile.length()) {
						// arquivo final é menor que o download atual. Assume que o download está certo
						try {
							Files.delete(finalFile.toPath());
						} catch (IOException ex) {
							Logger.getLogger(Downloader.class.getName()).log(Level.SEVERE, null, ex);
							actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.DOWNLOAD.toString(), remoteFile, localFile, null, Boolean.TRUE, ex.getMessage(), new Date(), Boolean.TRUE));
						}
						//} else {
						// arquivo final é maior que o download atual. Assume que o download falhou
						isOk = false;
						System.out.println("info: [" + this.getId() + "] Nada será feito com o arquivo recém baixado. Deve ser baixado novamente no futuro para ser processado.");
						//}
					} else {
						// download checked with preexisting file (arquivos de mesmo tamanho)
						System.out.println("info: [" + this.getId() + "] Download com tamanho de " + tempFile.length() + " bytes equivale ao tamanho de arquivo prévio. Download confere.");
						Downloader.this.actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.DOWNLOAD_CHECKED.toString(), remoteFile, localFile, size, Boolean.FALSE, null, new Date(), Boolean.FALSE));
					}
				} else {
					// final file does not exist. This is the first download
					//Downloader.this.actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.DOWNLOAD.toString(), remoteFile, localFile, size, Boolean.FALSE, null, new Date()));
					System.out.println("info: [" + this.getId() + "] Download feito pela primeira vez (ou ainda não conferido).");
				}
				if (isOk) {
					if (finalFile.length() != tempFile.length()) {
						try {
							Files.copy(tempFile.toPath(), finalFile.toPath());
							Files.deleteIfExists(tempFile.toPath());
						} catch (IOException ex) {
							Logger.getLogger(Downloader.class.getName()).log(Level.SEVERE, null, ex);
							actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.DOWNLOAD.toString(), remoteFile, localFile, null, Boolean.TRUE, ex.getMessage(), new Date(), Boolean.TRUE));
						}
					} else {
						try {
							Files.deleteIfExists(tempFile.toPath());
						} catch (IOException ex) {
							Logger.getLogger(Downloader.class.getName()).log(Level.SEVERE, null, ex);
						}
					}
					//Downloader.this.actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.DOWNLOAD.toString(), remoteFile, localFile, size, Boolean.FALSE, null, new Date()));

					if (localFile.endsWith(".zip")) {
						// after download, if it is a compressed file, uncompress it
						ActionResultLogger.ActionResult result = actionResultLogger.getActionResult(new ActionResultLogger.ActionResult(ActionResultLogger.Action.UNCOMPRESSED.toString(), remoteFile, localFile, null, Boolean.FALSE, null, null, Boolean.FALSE));
						if (result == null) { // if the file is compressed, then uncompress
							File f = new File(localFile);
							String localDir = f.getParent() + "/";
							isOk = uncompress(0L, localFile, localDir);
							if (isOk) {
								Downloader.this.actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.UNCOMPRESSED.toString(), remoteFile, localFile, size, Boolean.FALSE, null, new Date(), Boolean.FALSE));
							} else {
								System.out.println("ERRO: [" + this.getId() + "] ERRO ao descompactar o arquivo " + localFile);
								//Logger.getLogger(Downloader.class.getName()).log(Level.SEVERE, null, ex);
							}
						} else {
							System.out.println("done: [" + this.getId() + "] Arquivo previamente descompactado com sucesso");
						}
					}

				}
				Thread.yield();
			}
		};
		thread.setName(localFile);
		thread.addListener(this);
		Downloader.AddThread(thread);
		Thread.yield();
	}

	/**
	 *
	 * @param remoteDir
	 * @param localDir
	 * @param recursive
	 * @return
	 */
	public boolean downloadDir(String remoteDir, String localDir, boolean recursive) {
		//System.out.println("Downloading " + remoteDir);
		//Document doc = Jsoup.parse(filecontent);
		Document doc = null;
		try {
			doc = Jsoup.connect(remoteDir).get();
		} catch (Exception ex) {
			System.out.println("Erro ao conectar ao diretório remoto ('" + remoteDir + "') via Jsoup");
			Logger.getLogger(Downloader.class.getName()).log(Level.SEVERE, null, ex);
			actionResultLogger.recordAction(new ActionResultLogger.ActionResult(ActionResultLogger.Action.DOWNLOAD.toString(), remoteDir, localDir, null, Boolean.TRUE, ex.getMessage(), new Date(), Boolean.TRUE));
			return false;
		}
		//Files.writeString(Path.of(localDir + "dir.data"), doc.html());
		Elements links = doc.select("td > a[href]");
		String name, linkUrl, nomeDir;
		Path path;
		for (Element link : links) {
			linkUrl = link.attr("href");
			if (!linkUrl.startsWith("/") && linkUrl.endsWith("/") && linkUrl.length() > 1 && recursive) { //dir
				//se é um diretório de interesse, entra
				nomeDir = localDir + linkUrl;
				for (int i = 0; i < diretoriosInteresse.length; i++) {
					if (nomeDir.contains(diretoriosInteresse[i])) {
						File directory = new File(nomeDir);
						if (!directory.exists()) {
							directory.mkdir();
						}
						String nextRemoteDir = remoteDir + linkUrl;
						String nextLocalDir = localDir + linkUrl;
						this.downloadDir(nextRemoteDir, nextLocalDir, recursive);
					}
				}
			} else if (!linkUrl.startsWith("/")) { //file
				File file = new File(localDir + linkUrl);
				ActionResultLogger.ActionResult result = actionResultLogger.getActionResult(new ActionResultLogger.ActionResult(ActionResultLogger.Action.DOWNLOAD_CHECKED.toString(), remoteDir + linkUrl, null, null, Boolean.FALSE, null, null, Boolean.FALSE));
				if (result == null || !file.exists() || this.forceDownload) { //() {
					this.downloadFile(remoteDir + linkUrl, localDir + linkUrl);
				} else {
					System.out.println("done: Arquivo previamente baixado com sucesso em " + result.date.toString() + " :" + remoteDir + linkUrl + " [" + Downloader.FileSizeStr(file.length()) + "]");
					// verifica se precisa descompactar 
					file = new File(localDir + linkUrl);
					if (linkUrl.endsWith(".zip") && file.exists()) {
						result = actionResultLogger.getActionResult(new ActionResultLogger.ActionResult(ActionResultLogger.Action.UNCOMPRESSED.toString(), null, localDir + linkUrl, null, Boolean.FALSE, null, null, Boolean.FALSE));
						if (result == null || this.forceDownload) { // if the file is compressed, then uncompress
							uncompressDownloadedFile(localDir + linkUrl);
						} else {
							System.out.println("done: Arquivo previamente descompactado com sucesso em " + result.date.toString() + " :" + localDir + linkUrl);
						}
					}
				}
			}
		}
		while (Downloader.threadPool.size() > 0) {
			Thread.yield();
		}
		return true;
	}

	/**
	 * @param actionResultLogger the actionResultLogger to set
	 */
	public void setActionResultLogger(ActionResultLogger actionResultLogger) {
		this.actionResultLogger = actionResultLogger;
	}

	/**
	 * @param localDir the dir to download to (./CVM/dados)
	 * @param downloadAllHistoric if true then will download everything since
	 * 2010
	 */
	public boolean downloadB3Prices(String localDir, boolean downloadAllHistoric) {
		// create dir
		var dirToCreate = localDir + "CIA_ABERTA/DOC/DFP/B3/PRICES/";
		try {
			Files.createDirectories(Path.of(dirToCreate));
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		LocalDate date = LocalDate.now();
		var filesToDownload = new ArrayList<String>();
		if (downloadAllHistoric) {
			// takes a little while
			for (int i = 10; i < date.getYear() - 2000; i++) {
				filesToDownload.add("http://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A20" + i + ".ZIP");
			}
		}
		filesToDownload.add("http://bvmf.bmfbovespa.com.br/InstDados/SerHist/COTAHIST_A" + date.getYear() + ".ZIP");
		//filesToDownload.stream().parallel().forEach(
		for (int i = 0; i < filesToDownload.size(); i++) {
			String url = filesToDownload.get(i);
			var req = HttpRequest.newBuilder().uri(URI.create(url)).build();
			var splitName = url.split("/");
			String CurrentDownloadFile = dirToCreate + splitName[splitName.length - 1];
			System.out.println("Downloading file " + url);
			var downloadedFile = new File(CurrentDownloadFile);
			//try {
			//	HttpClient.newBuilder().build().send(req, HttpResponse.BodyHandlers.ofFile(downloadedFile.toPath()));
			//} catch (IOException | InterruptedException e) {
			//	e.printStackTrace();
			//}
			// if (response.statusCode() == 200) {
			// }
			//});
		}
		filesToDownload.stream().close();
		//	extractFolder(CurrentDownloadFile.toString(), dirToCreate);
		System.out.println("Download done");
		//	downloadedFile.delete();
		return true;
	}

	private static void extractFolder(String zipFile, String extractFolder) {
		try {
			int BUFFER = 2048;
			File file = new File(zipFile);

			ZipFile zip = new ZipFile(file);
			String newPath = extractFolder;

			new File(newPath).mkdir();
			Enumeration zipFileEntries = zip.entries();

			// Process each entry
			while (zipFileEntries.hasMoreElements()) {
				// grab a zip file entry
				ZipEntry entry = (ZipEntry) zipFileEntries.nextElement();
				String currentEntry = entry.getName();

				File destFile = new File(newPath, currentEntry);
				//destFile = new File(newPath, destFile.getName());
				File destinationParent = destFile.getParentFile();

				// create the parent directory structure if needed
				destinationParent.mkdirs();

				if (!entry.isDirectory()) {
					BufferedInputStream is = new BufferedInputStream(zip
							.getInputStream(entry));
					int currentByte;
					// establish buffer for writing file
					byte data[] = new byte[BUFFER];

					// write the current file to disk
					FileOutputStream fos = new FileOutputStream(destFile);
					BufferedOutputStream dest = new BufferedOutputStream(fos,
							BUFFER);

					// read and write until last byte is encountered
					while ((currentByte = is.read(data, 0, BUFFER)) != -1) {
						dest.write(data, 0, currentByte);
					}
					dest.flush();
					dest.close();
					is.close();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
