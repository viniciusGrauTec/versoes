package br.com.sankhya.acoesultramega.services;

import br.com.sankhya.acoesultramega.model.Note;
import br.com.sankhya.jape.vo.DynamicVO;

public interface DBAcessService {
	
	public boolean isEnabledToCreate(Long codtipoper) throws Exception;
	public void saveActualValue(Long nunota, Double actualValue) throws Exception;
	public void aplyDiscountToItem(Long nunota,Long sequencia, Double newValue) throws Exception;
	public void aplyValueToNota(Long nunota, Double newValue) throws Exception;
	public void aplyValueToItem(Long nunota,Long sequencia, Double newValue) throws Exception;
	public void duplicateFin(Long nunota,Double vlrnota,Double vlrdesconto,Long codparcdest) throws Exception;
	public DynamicVO findNote(Long nunota) throws Exception;
	public void updateOriginalFinancial(Long nunota) throws Exception;
	public Note findNotebyTGFVAR(Long nunota) throws Exception ;
	public void deleteFinancialByNunota(Long nunota) throws Exception;
	
}
