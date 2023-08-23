package br.com.sankhya.acoesultramega.services;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.Collection;

import br.com.sankhya.acoesultramega.model.Note;
import br.com.sankhya.acoesultramega.services.DBAcessService;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.core.JapeSession.SessionHandle;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.sql.NativeSql;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.vo.EntityVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.jape.wrapper.fluid.FluidCreateVO;
import br.com.sankhya.jape.wrapper.fluid.FluidUpdateVO;
import br.com.sankhya.modelcore.MGEModelException;
import br.com.sankhya.modelcore.comercial.ComercialUtils;
import br.com.sankhya.modelcore.util.DynamicEntityNames;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

public class DBAcessServiceUltramega implements DBAcessService{

	@Override
	public boolean isEnabledToCreate(Long codtipoper) throws Exception {
		JapeWrapper configDAO = JapeFactory.dao(DynamicEntityNames.TIPO_OPERACAO);
		DynamicVO vo = configDAO.findOne("CODTIPOPER = "+codtipoper+" AND DHALTER = (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = "+codtipoper+" )");
		

		String tag = vo.asString("AD_GERAFINPORDESCONTO");

		if (tag != null) {
			if (tag.equals("S")) {
				return true;
			}else {
				return false;
			}
		} else return false;

	}

	public void saveActualValue(Long nunota, Double actualValue) throws Exception{
		saveActualValueFromNote(nunota, actualValue);
		saveActualValueFromItems(nunota);
		
	}
	
	
	private void saveActualValueFromNote(Long nunota,Double actualValue) throws Exception {
		JapeWrapper notaDAO = JapeFactory.dao(DynamicEntityNames.CABECALHO_NOTA);
		DynamicVO note = notaDAO.findOne("NUNOTA = "+nunota+" AND AD_VALORORIGINAL IS NULL");
		
		if (note != null) {
			FluidUpdateVO update = notaDAO.prepareToUpdate(note);
			update
			.set("AD_VALORORIGINAL", BigDecimal.valueOf(actualValue))
			.update();
		}
		
		
	}
	private void saveActualValueFromItems(Long nunota) throws Exception {
		JapeWrapper itemDAO = JapeFactory.dao(DynamicEntityNames.ITEM_NOTA);
		Collection <DynamicVO> itens = itemDAO.find("NUNOTA = " + nunota+" AND AD_VLRUNITORIG IS NULL");
		for (DynamicVO item : itens) {
			
			double vlrUnit = item.asDouble("VLRUNIT");
			
			FluidUpdateVO update = itemDAO.prepareToUpdate(item);

			update
			.set("AD_VLRUNITORIG", BigDecimal.valueOf(vlrUnit))
			.update();
			
		}
	}
	
	
	
	public void aplyDiscountToItem(Long nunota,Long sequencia, Double newValue) throws Exception{
		JapeWrapper notaDAO = JapeFactory.dao(DynamicEntityNames.ITEM_NOTA);
		FluidUpdateVO update = notaDAO.prepareToUpdateByPK(BigDecimal.valueOf(nunota),BigDecimal.valueOf(sequencia));
		
		update
		.set("VLRUNIT", BigDecimal.valueOf(newValue))
		.update();
	}
	
	@Override
	public void aplyValueToNota(Long nunota, Double newValue) throws Exception {
		JapeWrapper notaDAO = JapeFactory.dao(DynamicEntityNames.CABECALHO_NOTA);
		FluidUpdateVO update = notaDAO.prepareToUpdateByPK(BigDecimal.valueOf(nunota));

		update
		.set("VLRNOTA", BigDecimal.valueOf(newValue))
		.update();
		
	}

	@Override
	public void aplyValueToItem(Long nunota, Long sequencia, Double newValue) throws Exception {
		JapeWrapper notaDAO = JapeFactory.dao(DynamicEntityNames.ITEM_NOTA);
		FluidUpdateVO update = notaDAO.prepareToUpdateByPK(BigDecimal.valueOf(nunota),BigDecimal.valueOf(sequencia));
		
		update
		.set("VLRTOT", BigDecimal.valueOf(newValue))
		.update();
		
	}
	
	public void duplicateFin(Long nunota,Double vlrnota,Double vlrdesconto,Long codparcdest) throws Exception {
		JapeWrapper financeiroDAO = JapeFactory.dao(DynamicEntityNames.FINANCEIRO);
		DynamicVO fin = financeiroDAO.findOne("NUNOTA = "+nunota+ " AND DESDOBRAMENTO='0'");
		EntityFacade dwfFacade = EntityFacadeFactory.getDWFFacade();
		//DynamicVO repasseDifVO = ComercialUtils.duplicar(DynamicEntityNames.FINANCEIRO, fin);
		Long desdobr = findMaxDesdobramento(nunota);
		
		fin.clearReferences();
		fin.setPrimaryKey(null);
		
		fin.setProperty("NUFIN", null);
		fin.setProperty("NUNOTA", null);
		fin.setProperty("NUMNOTA", BigDecimal.valueOf(0));
		
		fin.setProperty("DHBAIXA", null);
		fin.setProperty("VLRBAIXA",  BigDecimal.valueOf(0));
		fin.setProperty("CODUSUBAIXA",  BigDecimal.valueOf(0));
		fin.setProperty("CODTIPOPERBAIXA",  BigDecimal.valueOf(0));
		fin.setProperty("CODEMPBAIXA", null);
		
		
		fin.setProperty("VLRDESDOB",BigDecimal.valueOf(vlrdesconto));
		fin.setProperty("CODPARC",BigDecimal.valueOf(codparcdest));
		fin.setProperty("DESDOBRAMENTO",(desdobr+1)+"");
		fin.setProperty("PROVISAO","N");
		fin.setProperty("AD_NUMPEDORIG",BigDecimal.valueOf(nunota));
		dwfFacade.createEntity(DynamicEntityNames.FINANCEIRO, (EntityVO) fin);
		
		
	}
	
	private Long findMaxDesdobramento(Long nunota) throws Exception {
		JdbcWrapper jdbc = null;
		final EntityFacade dwfEntityFacade = EntityFacadeFactory.getDWFFacade();
		jdbc = dwfEntityFacade.getJdbcWrapper();
		jdbc.openSession();
		NativeSql sql = null;
		try {

			sql = new NativeSql(jdbc);
			sql.appendSql(
					"SELECT MAX(DESDOBRAMENTO) AS DESDOBR FROM TGFFIN WHERE NUNOTA = :PNUNOTA");
			sql.setNamedParameter("PNUNOTA", nunota);
			final ResultSet result = sql.executeQuery();

			if (result.next()) {
				return result.getString("DESDOBR") ==null? -1:Long.parseLong(result.getString("DESDOBR"));
			}

			result.close();
		} catch (Exception throwable) {
			NativeSql.releaseResources(sql);
			throw throwable;
		}
		NativeSql.releaseResources(sql);
		jdbc.closeSession();
		return null;
	}
	
	public DynamicVO findNote(Long nunota) throws Exception {
		JapeWrapper notaDAO = JapeFactory.dao(DynamicEntityNames.CABECALHO_NOTA);
		DynamicVO note = notaDAO.findByPK(BigDecimal.valueOf(nunota));
		return note;
		
	}
	
	public void updateOriginalFinancial(Long nunota) throws Exception {
		JapeWrapper financeiroDAO = JapeFactory.dao(DynamicEntityNames.FINANCEIRO);
		DynamicVO fin = financeiroDAO.findOne("NUNOTA = "+nunota+ " AND DESDOBRAMENTO='0'");
		
		FluidUpdateVO update = financeiroDAO.prepareToUpdate(fin);
		
		update
		.set("AD_NUMPEDORIG", BigDecimal.valueOf(nunota))
		.update();
		
		
		
	}

	@Override
	public Note findNotebyTGFVAR(Long nunota) throws Exception {
		
		JdbcWrapper jdbc = null;
		final EntityFacade dwfEntityFacade = EntityFacadeFactory.getDWFFacade();
		jdbc = dwfEntityFacade.getJdbcWrapper();
		jdbc.openSession();
		NativeSql sql = null;
		try {

			sql = new NativeSql(jdbc);
			sql.appendSql(
					"SELECT CAB.NUNOTA, CAB.VLRNOTA, CAB.CODPARCDEST,CAB.CODPARC, CAB.CODTIPOPER,CAB.AD_VALORORIGINAL AS VLRORIG FROM TGFCAB CAB INNER JOIN TGFVAR VAR ON VAR.NUNOTAORIG = CAB.NUNOTA WHERE VAR.NUNOTA =  :PNUNOTA");
			sql.setNamedParameter("PNUNOTA", nunota);
			final ResultSet result = sql.executeQuery();

			if (result.next()) {
				Note note = new Note();
				note.setCodparc(result.getLong("CODPARC"));
				note.setCodparcdest(result.getLong("CODPARCDEST"));
				note.setCodtipoper(result.getLong("CODTIPOPER"));
				note.setNuNota(result.getLong("NUNOTA"));
				note.setVlrnota(result.getDouble("VLRNOTA"));
				note.setVlrorig(result.getDouble("VLRORIG"));
				
				
				return note;
			}

			result.close();
		} catch (Exception throwable) {
			NativeSql.releaseResources(sql);
			throw throwable;
		}
		NativeSql.releaseResources(sql);
		jdbc.closeSession();
		return null;
	}
	
	public void deleteFinancialByNunota(Long nunota) throws Exception {

		JdbcWrapper jdbc = null;
		NativeSql sql = null;
		SessionHandle hnd = null;
		
		try {
			hnd = JapeSession.open();
			hnd.setFindersMaxRows(-1);
			EntityFacade entity = EntityFacadeFactory.getDWFFacade();
			jdbc = entity.getJdbcWrapper();
			jdbc.openSession();

			sql = new NativeSql(jdbc);
			
			sql.appendSql("DELETE FROM TGFFIN WHERE NUNOTA = :NUNOTA");
			
			sql.setNamedParameter("NUNOTA", nunota);

			sql.executeUpdate();
	

		} catch (Exception e) {
			throw e;
			
		} finally {

			NativeSql.releaseResources(sql);
			JdbcWrapper.closeSession(jdbc);
			JapeSession.close(hnd);

		}
		
	}
	

}
