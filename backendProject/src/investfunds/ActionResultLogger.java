/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package investfunds;

import java.util.Date;

/**
 *
 * @author rlcancian
 */
public interface ActionResultLogger {

	/**
	 *
	 */
	public enum Action {

		/**
		 *
		 */
		BEGIN_UPDATE("beginUpdate"),

		/**
		 *
		 */
		DOWNLOAD("download"),

		/**
		 *
		 */
		DOWNLOAD_CHECKED("downloadChecked"),

		/**
		 *
		 */
		UNCOMPRESSED("uncompressed"),

		/**
		 *
		 */
		EXTRACTED("extracted"),

		/**
		 *
		 */
		CREATE_METADATA("createMetadata"),

		/**
		 *
		 */
		TABLE_PROCESSED("tableProcessed"),

		/**
		 *
		 */
		FILE_PROCESSED("datafileProcessed"),

		/**
		 *
		 */
		INDICADORS_CALCULATED("indicatorsCalculated"),

		/**
		 *
		 */
		INSERT_META_CIA_INTO_DB("datafileProcessed"),

		/**
		 *
		 */
		INSERT_BPA_INTO_DB("datafileProcessed"),

		/**
		 *
		 */
		INSERT_BPP_INTO_DB("datafileProcessed"),

		/**
		 *
		 */
		INSERT_DRE_INTO_DB("datafileProcessed"),

		/**
		 *
		 */
		INSERT_DMPL_INTO_DB("datafileProcessed"),

		/**
		 *
		 */
		INSERT_DFC_MD_INTO_DB("datafileProcessed"),

		/**
		 *
		 */
		INSERT_B3_CODES_INTO_DB("datafileProcessed"),

		/**
		 *
		 */
		INSERT_B3_PRICES_INTO_DB("datafileProcessed"),

		/**
		 *
		 */
		END_UPDATE("endUpdate");
		private final String item;

		Action(String item) {
			this.item = item;
		}

		/**
		 *
		 * @return
		 */
		@Override
		public String toString() {
			return this.item;
		}
	}

	/**
	 *
	 */
	class ActionResult {

		String action;
		String remoteURI;
		String localURI;
		Long result;
		Boolean HasErrors;
		String message;
		Date date;
		Boolean needToRedo;

		/**
		 *
		 * @param action
		 * @param remoteURI
		 * @param localURI
		 * @param result
		 * @param HasErrors
		 * @param message
		 * @param when
		 * @param needToRedo
		 */
		public ActionResult(String action, String remoteURI, String localURI, Long result, Boolean HasErrors, String message, Date when, Boolean needToRedo) {
			this.action = action;
			this.remoteURI = remoteURI;
			this.localURI = localURI;
			this.result = result;
			this.HasErrors = HasErrors;
			this.message = message;
			this.date = when;
			this.needToRedo = needToRedo;
		}

		/**
		 *
		 * @return
		 */
		@Override
		public String toString() {
			return "Ação='" + action + "'; remote='" + remoteURI + "'; local='" + localURI + "'; HasErrors='" + HasErrors + "'; NeedToRedo='" + needToRedo + "'; message='" + message + "'; date='" + date + "'";
		}
	}

	/**
	 *
	 * @param actionResult
	 */
	public void recordAction(ActionResult actionResult);

	/**
	 *
	 * @param actionResult
	 * @return
	 */
	public ActionResult getActionResult(ActionResult actionResult);
}
