/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package investfunds.Oldies;

import investfunds.NotifyingThread;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.Scanner;

/**
 *
 * @author rlcancian
 */
public class CSVRecordThread extends NotifyingThread {

	private final String lineText;
	private final int[] metadataFieldIndex;
	private final CSVMetadata csvMetadata;
	private final String databasename;
	private final String tableName;
	private final Connection connection;
	private final int myCsvThreadID;
	private int countRecords;

	/**
	 *
	 * @param lineText
	 * @param csvMetadata
	 * @param metadataFieldIndex
	 * @param databasename
	 * @param tableName
	 * @param connection
	 * @param myCsvThreadID
	 * @param countRecords
	 */
	public CSVRecordThread(String lineText, CSVMetadata csvMetadata, int[] metadataFieldIndex, String databasename, String tableName, Connection connection, int myCsvThreadID, int countRecords) {
		this.lineText = lineText;
		this.csvMetadata = csvMetadata;
		this.databasename = databasename;
		this.metadataFieldIndex = metadataFieldIndex;
		this.tableName = tableName;
		this.connection = connection;
		this.myCsvThreadID = myCsvThreadID;
		this.countRecords = countRecords;
	}

	/**
	 *
	 */
	@Override
	public void doRun() {
		String[] record = lineText.split(";");
		CSVField csvField;
		String bytesReadedSoFarStr;
		String nomesCampos, valores, sql;
		nomesCampos = "";
		valores = "";
		for (int i = 0; i < record.length; i++) {
			if (!record[i].equals("")) {
				csvField = csvMetadata.getCSVFields().get(metadataFieldIndex[i]);
				nomesCampos += csvField.getCampo() + ", ";
				if (csvField.getTipo().equals("varchar") || csvField.getTipo().equals("char") || csvField.getTipo().equals("date")) {
					valores += "\"" + record[i] + "\", ";
				} else {
					valores += record[i] + ", ";
				}

			}
		}
		nomesCampos = nomesCampos.substring(0, nomesCampos.length() - 2);
		valores = valores.substring(0, valores.length() - 2);
		sql = "insert into `" + databasename + "`.`" + tableName + "` (" + nomesCampos + ") values (" + valores + ");";
		try {
			Statement st = (Statement) connection.createStatement();
			st.execute(sql);
		} catch (SQLException e) {
			System.out.println("[" + myCsvThreadID + "." + this.getId() + "] ERRO no registro " + countRecords + ":" + e.getMessage());
			System.out.println("[" + myCsvThreadID + "." + this.getId() + "] ERRO Query: " + sql);
		}
	}


}
