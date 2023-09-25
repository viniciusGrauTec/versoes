package br.com.sankhya.acoesgrautec.jobs;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import javax.net.ssl.HttpsURLConnection;

import br.com.sankhya.acoesgrautec.util.LogCatcher;
import br.com.sankhya.acoesgrautec.util.LogConfiguration;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.modelcore.util.SWRepositoryUtils;

public class JobGetTitulos implements ScheduledAction {

	@Override
	public void onTime(ScheduledActionContext arg0) {
		
		LogConfiguration.setPath(SWRepositoryUtils.getBaseFolder()+"/personalizacao/bmc/logs");
		
		//LogCatcher.logInfo("Entrou no job");
		System.out.println("Entrou no job");
		
		try {
			/*String[] response = apiGet("https://api.acadweb.com.br/testegrautboavistasankhya/"
					+ "financeiro/acordos?"
					//+ "vencimentoInicial=2023-04-11?"
					//+ "vencimentoFinal=2023-04-24?"
					//+ "dataInicial=2023-04-19 06:36:00?"
					//+ "dataFinal=2023-04-24 06:36:00?"
					+ "quantidade=2");
			
			String responseString = response[1];*/
			String responseString = "";
			
			//LogCatcher.logInfo("Response: " + responseString);
			JsonParser parser = new JsonParser();
			JsonArray jsonArray = parser.parse(responseString).getAsJsonArray();

	        for (JsonElement jsonElement : jsonArray) {
	            JsonObject jsonObject = jsonElement.getAsJsonObject();

	            //LogCatcher.logInfo("Titulo ID: " + jsonObject.get("titulo_id").getAsInt());
	            //LogCatcher.logInfo("Titulo Vencimento: " + jsonObject.get("titulo_vencimento").getAsString());
	            //LogCatcher.logInfo("Titulo Valor: " + jsonObject.get("titulo_valor").getAsDouble());
	            
	            System.out.println("Titulo ID: " + jsonObject.get("titulo_id").getAsInt());
	            System.out.println("Titulo Vencimento: " + jsonObject.get("titulo_vencimento").getAsString());
	            System.out.println("Titulo Valor: " + jsonObject.get("titulo_valor").getAsDouble());

	            // Para os benefícios, você pode fazer um loop interno
	            JsonArray beneficiosArray = jsonObject.getAsJsonArray("beneficios");
	            for (JsonElement beneficioElement : beneficiosArray) {
	                JsonObject beneficioObject = beneficioElement.getAsJsonObject();
	               
	            	System.out.println("Beneficio ID: " + beneficioObject.get("beneficio_id").getAsString());
	            	System.out.println("Beneficio Descrição: " + beneficioObject.get("beneficio_descricao").getAsString());
	                
	            }
	        }
			
		} catch (Exception e) {
			e.printStackTrace();
			LogCatcher.logError(e.getStackTrace().toString());
		}
		
	}
	
	public String[] apiGet(String ur) throws Exception {

		BufferedReader reader;
		String line;
		StringBuilder responseContent = new StringBuilder();
		//String key = preferenciaSenha();
		
		// Preparando a requisição
		URL obj = new URL(ur);
		HttpsURLConnection https = (HttpsURLConnection) obj.openConnection();
		
		System.out.println("URL: " + ur);
		System.out.println("https: " + https);
		
		https.setRequestMethod("GET");
		https.setConnectTimeout(30000);
		https.setRequestProperty("User-Agent",
				"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)");
		https.setRequestProperty("Content-Type",
				"application/json; charset=UTF-8");
		https.setRequestProperty("Authorization", "Bearer " + "2|VFBUMOCUNitomQYMrwWY7dCaTLts1Lsab3Bktpf5");
		https.setDoOutput(true);
		https.setDoInput(true);
		
		int status = https.getResponseCode();
	
		if (status >= 300) {
			reader = new BufferedReader(new InputStreamReader(
					https.getErrorStream()));
			while ((line = reader.readLine()) != null) {
				responseContent.append(line);
			}
			reader.close();
		} else {
			reader = new BufferedReader(new InputStreamReader(
					https.getInputStream()));
			while ((line = reader.readLine()) != null) {
				responseContent.append(line);
			}
			reader.close();
		}
		System.out.println("Output from Server .... \n" + status);
		String response = responseContent.toString();
	
		https.disconnect();
		
		return new String[] {Integer.toString(status), response};

	}
	public BigDecimal insertFin(BigDecimal codemp, BigDecimal codCenCus, BigDecimal codNat,
			BigDecimal codTipOper, BigDecimal codparc, BigDecimal codTipTit, 
			BigDecimal vlrDesdbo, String idMaket, String dtVenc, String dtPedido, String codimp) throws Exception{
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		BigDecimal nufin = getMaxNumFin();
		
		System.out.println("Teste financeiro: " + nufin);
		
		try {
			
			jdbc.openSession();
			
			String sqlUpdate = "INSERT INTO TGFFIN "
					+ "        (NUFIN, "
					+ "         NUNOTA, "
					+ "         NUMNOTA, "
					+ "         ORIGEM, "
					+ "         RECDESP, "
					+ "         CODEMP, "
					+ "         CODCENCUS, "
					+ "         CODNAT, "
					+ "         CODTIPOPER, "
					+ "         DHTIPOPER, "
					+ "         CODTIPOPERBAIXA, "
					+ "         DHTIPOPERBAIXA, "
					+ "         CODPARC, "
					+ "         CODTIPTIT, "
					+ "         VLRDESDOB, "
					+ "         VLRDESC, "
					+ "         VLRBAIXA, "
					+ "         CODBCO, "
					+ "         CODCTABCOINT, "
					+ "         DTNEG, "
					+ "         DHMOV, "
					+ "         DTALTER, "
					+ "         DTVENC, "
					+ "         DTPRAZO, "
					+ "         DTVENCINIC, "
					+ "         TIPJURO, "
					+ "         TIPMULTA, "
					+ "         HISTORICO, "
					+ "         TIPMARCCHEQ, "
					+ "         AUTORIZADO, "
					+ "         BLOQVAR, "
					+ "         INSSRETIDO, "
					+ "         ISSRETIDO, "
					+ "         PROVISAO, "
					+ "         RATEADO, "
					+ "         TIMBLOQUEADA, "
					+ "         IRFRETIDO, "
					+ "         TIMTXADMGERALU, "
					+ "         VLRDESCEMBUT, "
					+ "         VLRINSS, "
					+ "         VLRIRF, "
					+ "         VLRISS, "
					+ "         VLRJURO, "
					+ "         VLRJUROEMBUT, "
					+ "         VLRJUROLIB, "
					+ "         VLRJURONEGOC, "
					+ "         VLRMOEDA, "
					+ "         VLRMOEDABAIXA, "
					+ "         VLRMULTA, "
					+ "         VLRMULTAEMBUT, "
					+ "         VLRMULTALIB, "
					+ "         VLRMULTANEGOC, "
					+ "         VLRPROV, "
					+ "         VLRVARCAMBIAL, "
					+ "         VLRVENDOR, "
					+ "         ALIQICMS, "
					+ "         BASEICMS, "
					+ "         CARTAODESC, "
					+ "         CODMOEDA, "
					+ "         CODPROJ, "
					+ "         CODVEICULO, "
					+ "         CODVEND, "
					+ "         DESPCART, "
					+ "         NUMCONTRATO, "
					+ "         ORDEMCARGA, "
					+ "         CODUSU) "
					+ "        VALUES (?, "
					+ "               NULL, "
					+ "               0, "
					+ "               'F', "
					+ "               1, "
					+ "               ? , " //AS CODEMP
					+ "               ? , " //AS CODCENCUS
					+ "               ? , " //AS CODNAT
					+ "               ? , " //AS CODTIPOPER
					+ "               (SELECT MAX(DHALTER) "
					+ "                  FROM TGFTOP "
					+ "                 WHERE CODTIPOPER = ?), "
					+ "               0, "
					+ "               (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 0), "
					+ "               ? , " //AS CODPARC
					+ "               (select CODTIPTITPAD  "
					+ "					from TGFPPG WHERE CODTIPVENDA = ? AND SEQUENCIA =  "
					+ "					(SELECT MAX(SEQUENCIA) FROM TGFPPG WHERE CODTIPVENDA = ?)) , " //AS CODTIPTIT
					+ "               ? , " //AS VLRDESDOB
					+ "               0, "
					+ "               0, "
					+ "               997, " //AS CODBCO
					+ "               2 , " //AS CODCTABCOINT
					+ "               ? , " //AS DTNEG
					+ "               SYSDATE, "
					+ "               SYSDATE, "
					+ "               ? , " //AS DTVENC
					+ "               SYSDATE, " //AS PRAZO
					+ "               ? , " //AS DTVENCINIC
					+ "               1 , " //AS TIPJURO
					+ "               1 , " //AS TIPMULTA
					+ "               null , " //AS HISTORICO
					+ "               'I' , " //AS TIPMARCCHEQ
					+ "               'N' , " //AS AUTORIZADO
					+ "               'N' , " //AS BLOQVAR
					+ "               'N' , " //AS INSSRETIDO
					+ "               'N' , " //AS ISSRETIDO
					+ "               'N' , " //AS PROVISAO
					+ "               'N' , " //AS RATEADO
					+ "               'N' , " //AS TIMBLOQUEADA
					+ "               'S' , " //AS IRFRETIDO
					+ "               'S' , " //AS TIMTXADMGERALU
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.setBigDecimal(1, nufin);
			pstmt.setBigDecimal(2, codemp);
			pstmt.setBigDecimal(3, codCenCus);
			pstmt.setBigDecimal(4, codNat);
			pstmt.setBigDecimal(5, codTipOper);
			pstmt.setBigDecimal(6, codTipOper);
			pstmt.setBigDecimal(7, codparc);
			pstmt.setBigDecimal(8, codTipTit);
			pstmt.setBigDecimal(9, codTipTit);
			pstmt.setBigDecimal(10, vlrDesdbo);
			pstmt.setString(11, dtPedido);
			pstmt.setString(12, dtVenc);
			pstmt.setString(13, dtVenc);

			pstmt.executeUpdate();
		
		
		}catch (Exception se) {
			se.printStackTrace();
			throw se;
		} finally {
			
			try {
				if (pstmt != null) {
					pstmt.close();
				}
				if (jdbc != null) {
					jdbc.closeSession();
				}
			} catch (Exception se) {
				se.printStackTrace();
			}
			
		}
		
		return nufin;
		
	}
	
	public BigDecimal getMaxNumFin() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null; 
		
		BigDecimal id = BigDecimal.ZERO;
		
		try {
			
			//updateNumFin();
			
			jdbc.openSession();
			
			String sqlNota = "SELECT SEQ_TGFFIN_NUFIN.NEXTVAL FROM DUAL";
			
			pstmt = jdbc.getPreparedStatement(sqlNota);
			
			rs = pstmt.executeQuery();
			
			if (rs.next()){
				
				id = rs.getBigDecimal("NEXTVAL");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
		
		return id;
	}
	
	public void updateNumFin() throws Exception{
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		
		jdbc.openSession();
		
		String sqlUpdate = "UPDATE TGFNUM SET ULTCOD = NVL(ULTCOD, 0) + 1  WHERE ARQUIVO = 'TGFFIN'";
		
		pstmt = jdbc.getPreparedStatement(sqlUpdate);
		pstmt.executeUpdate();
		
		
		try {
			if (pstmt != null) {
				pstmt.close();
			}
			if (jdbc != null) {
				jdbc.closeSession();
			}
		} catch (Exception se) {
			se.printStackTrace();
		}
		
	}

}
