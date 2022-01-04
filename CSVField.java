/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package investfunds2ndoption;

/**
 *
 * @author rlcancian
 */
public class CSVField {

    /**
     * @return the campo
     */
    public String getCampo() {
        return campo;
    }

    /**
     * @param campo the campo to set
     */
    public void setCampo(String campo) {
        this.campo = campo;
    }

    /**
     * @return the dominio
     */
    public String getDominio() {
        return dominio;
    }

    /**
     * @param dominio the dominio to set
     */
    public void setDominio(String dominio) {
        this.dominio = dominio;
    }

    /**
     * @return the tipo
     */
    public String getTipo() {
        return tipo;
    }

    /**
     * @param tipo the tipo to set
     */
    public void setTipo(String tipo) {
        this.tipo = tipo;
    }

    /**
     * @return the tamanho
     */
    public int getTamanho() {
        return tamanho;
    }

    /**
     * @param tamanho the tamanho to set
     */
    public void setTamanho(int tamanho) {
        this.tamanho = tamanho;
    }

    /**
     * @return the escala
     */
    public int getEscala() {
        return escala;
    }

    /**
     * @param escala the escala to set
     */
    public void setEscala(int escala) {
        this.escala = escala;
    }
    private String campo;
    private String dominio;
    private String descricao;
    private String tipo;
    private int tamanho;
	private int precisao;
    private int escala;
    private int indexInHeaderArray;

	/**
	 *
	 * @return
	 */
	@Override
    public String toString() {
        return campo+"("+tipo+","+tamanho+","+precisao+","+escala+")";
    }
    
    /**
     * @return the descricao
     */
    public String getDescricao() {
        return descricao;
    }

    /**
     * @param descricao the descricao to set
     */
    public void setDescricao(String descricao) {
        this.descricao = descricao;
    }

    /**
     * @return the indexInArray
     */
    public int getIndexInHeaderArray() {
        return indexInHeaderArray;
    }

    /**
     * @param indexInArray the indexInArray to set
     */
    public void setIndexInHeaderArray(int indexInArray) {
        this.indexInHeaderArray = indexInArray;
    }

	/**
	 * @return the precisao
	 */
	public int getPrecisao() {
		return precisao;
	}

	/**
	 * @param precisao the precisao to set
	 */
	public void setPrecisao(int precisao) {
		this.precisao = precisao;
	}
}
