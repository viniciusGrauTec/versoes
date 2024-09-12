package br.com.sankhya.acoesgrautec.jobs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import javax.net.ssl.HttpsURLConnection;

import br.com.sankhya.acoesgrautec.util.LogCatcher;
import br.com.sankhya.acoesgrautec.util.LogConfiguration;
import br.com.sankhya.acoesgrautec.services.SkwServicoCompras;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.core.JapeSession.SessionHandle;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.DynamicEntityNames;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.modelcore.util.SWRepositoryUtils;

public class JobGetTitulos implements ScheduledAction {

	@Override
	public void onTime(ScheduledActionContext arg0) {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal codEmp = BigDecimal.ZERO;
		BigDecimal idCarga = BigDecimal.ZERO;

		String url = "";
		String token = "";
		String matricula = "";
		
		int count = 0;

		System.out.println("Iniciou o financeiro dos alunos");

		try {

			jdbc.openSession();

			//String query3 = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO WHERE CODEMP = 3";
			String query3 = "SELECT LINK.CODEMP, URL, TOKEN, IDCARGA, MATRICULA FROM AD_LINKSINTEGRACAO LINK INNER JOIN AD_CARGAALUNOS CARGA ON CARGA.CODEMP = LINK.CODEMP WHERE LINK.CODEMP = 3 AND NVL(CARGA.INTEGRADO_FIN, 'N') = 'N'";
			String query4 = "SELECT LINK.CODEMP, URL, TOKEN, IDCARGA, MATRICULA FROM AD_LINKSINTEGRACAO LINK INNER JOIN AD_CARGAALUNOS CARGA ON CARGA.CODEMP = LINK.CODEMP WHERE LINK.CODEMP = 4 AND NVL(CARGA.INTEGRADO_FIN, 'N') = 'N'";

			pstmt = jdbc.getPreparedStatement(query3);

			rs = pstmt.executeQuery();

			while (rs.next()) {
				count++;
				codEmp = rs.getBigDecimal("CODEMP");
		        idCarga = rs.getBigDecimal("IDCARGA");
		        
		        url = rs.getString("URL");
		        token = rs.getString("TOKEN");
		        matricula = rs.getString("MATRICULA");

		        iterarEndpoint(url, token, codEmp, matricula);
				updateCarga(idCarga);

			}
			
			if(count == 0){
				resetCarga(codEmp);
			}
			
			System.out.println("Finalizou o financeiro dos alunos");

		} catch (Exception e) {
			e.printStackTrace();
			try {
				insertLogIntegracao(
						"Erro ao integrar financeiro, Mensagem de erro: "
								+ e.getMessage(), "Erro");
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			jdbc.closeSession();
		}
	}
	

	  public void iterarEndpoint(String url, String token, BigDecimal codEmp, String matricula)
	    throws Exception
	  {
	    //int pagina = 1;
		
	    Date dataAtual = new Date();
	      
	    SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");
	      
	    String dataFormatada = formato.format(dataAtual);
	      
		  
	    int paginaInicio = 1;
	    int paginaFim = 10;
	    /*int paginaAtual = getPagina();
	    
	    if(paginaAtual == 0){
	    	paginaInicio = 1;
	    	paginaFim = 30;
	    }else{
	    	paginaInicio = paginaAtual;
	    	paginaFim = paginaAtual + 30;
	    }*/
	    
	    try
	    {
	      for (;;)
	      {
	        System.out.println("While de iteração");
	        
	        String[] response = apiGet(url + "/financeiro" + "/titulos?"
					// + "situacao=A"
					// + "&quantidade=1"
					// + "&dataInicial=2023-04-19 00:00:00"
					// + "&dataFinal=2023-10-24 00:00:00"
					+ "matricula=" + matricula
					+ "&pagina=" + paginaInicio
					//+ "&vencimentoInicial=2024-08-05 00:00:00&vencimentoFinal=2024-08-08 23:59:59"
					//+ "&dataInicial="+dataFormatada+" 00:00:00&dataFinal="+dataFormatada+" 23:59:59"
					, token);
	        
	        int status = Integer.parseInt(response[0]);
	        
	        System.out.println("Status teste: " + status);
	        System.out.println("pagina: " + paginaInicio);
	        
	        String responseString = response[1];
	        System.out.println("response string alunos: " + responseString);
	        
	        if ((responseString.equals("[]")) || (paginaInicio == paginaFim))
	        {
	          System.out.println("Entrou no if da quebra");
	         /* if(responseString.equals("[]")){
	        	  insertUltPagina(paginaInicio); 
	          }else{
	        	  insertUltPagina(paginaFim); 
	          }*/
	          
	          break;
	        }
	        cadastrarFinanceiro(response, url, token, codEmp, matricula);
	        paginaInicio++;
	      }
	    }
	    catch (Exception e)
	    {
	      e.printStackTrace();
	    }
	  }
	  
	
	public void cadastrarFinanceiro(String[] response, String url, String token, BigDecimal codemp, String matricula) throws Exception{

		SimpleDateFormat formatoEntrada = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss.SSS");
		SimpleDateFormat formatoOriginal = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat formatoDesejado = new SimpleDateFormat("dd/MM/yyyy");

		System.out.println("Entrou no job");

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		int count = 0;
		
		Date dataAtual = new Date();
	      
	    SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");
	      
	    String dataFormatada = formato.format(dataAtual);

		try {

			jdbc.openSession();

			//String sqlP = "SELECT ID_EXTERNO, CODPARC, CODCENCUS, INTEGRADO FROM (SELECT ID_EXTERNO, CODPARC, CODCENCUS, INTEGRADO FROM AD_ALUNOS WHERE CODEMP = "+codemp+" AND NVL(INTEGRADO, 'N') = 'N' ORDER BY ID_EXTERNO, CODPARC DESC ) WHERE ROWNUM <= 300"; //AND ROWNUM <= 60";
			String sqlP = "SELECT ID_EXTERNO, CODPARC, CODCENCUS, INTEGRADO FROM AD_ALUNOS WHERE ID_EXTERNO = '" + matricula + "' AND CODEMP = " + codemp; //AND ROWNUM <= 60";
			// + "WHERE ID_EXTERNO = '"+"STB210040"+"'";

			pstmt = jdbc.getPreparedStatement(sqlP);

			rs = pstmt.executeQuery();

			while (rs.next()) {
				count++;
				BigDecimal codparc = rs.getBigDecimal("CODPARC");
				String aluno = rs.getString("ID_EXTERNO");

				String responseString = response[1];
				String responseStatus = response[0];
				/*
				 * String responseString = "[" + "	  {" +
				 * "	    \"titulo_id\": 176720," +
				 * "	    \"titulo_vencimento\": \"2023-04-24\", " +
				 * "	    \"titulo_valor\": 591.34, " +
				 * "	    \"titulo_mes_ref\": \"01\",  " +
				 * "	    \"titulo_ano_ref\": \"2023\", " +
				 * "	    \"titulo_situacao\": \"B\", " +
				 * "	    \"titulo_observação\": \"Aluno indicou o amigo\",  " +
				 * "	    \"taxa_id\": \"10587\", " +
				 * "	    \"taxa_descricao\": \"PARCELA-RFC\"," +
				 * "	    \"taxa_categoria\": \"00002\", " +
				 * "	    \"taxa_categoria_descricao\": \"Parcela\", " +
				 * "	    \"aluno_id\": \"ADM220028\",  " +
				 * "	    \"aluno_nome\": \"Fulano de Tal\", " +
				 * "	    \"curso_id\": \"00003\", " +
				 * "	    \"curriculo_id\": \"ADM20111\", " +
				 * "	    \"turma_id\": \"ADM03-N\", " +
				 * "	    \"beneficios\": [  " + "	      {  " +
				 * "	        \"beneficio_id\": \"04\", " +
				 * "	        \"beneficio_incidencia\": \"D\", " +
				 * "	        \"beneficio_incidencia_grupo\": 0,  " +
				 * "	        \beneficio_tipo_grupo\": \"G\", " +
				 * "	        \"beneficio_descricao\": \"BOLSA 30%\",  " +
				 * "	        \"beneficio_valor\": 30," +
				 * "	        \"beneficio_tipo_valor\": \"P\", " +
				 * "	        \"beneficio_dia_incidencia_inicial\": \"01\"," +
				 * "	        \"beneficio_dia_incidencia_final\": \"32\" " +
				 * "	      } " + "	    ] " + "	  } " + "	]";
				 */

				if (responseStatus.equalsIgnoreCase("200")) {

					JsonParser parser = new JsonParser();
					JsonArray jsonArray = parser.parse(responseString)
							.getAsJsonArray();

					for (JsonElement jsonElement : jsonArray) {
						JsonObject jsonObject = jsonElement.getAsJsonObject();

						String idFin = jsonObject.get("titulo_id")
								.getAsString();

						BigDecimal vlrDesdob = new BigDecimal(jsonObject.get(
								"titulo_valor").getAsDouble());

						String dtVenc = jsonObject.get("titulo_vencimento")
								.getAsString();

						String idCurso = jsonObject.get("curso_id")
								.getAsString();
						
						String taxaId = jsonObject.get("taxa_id")
								.getAsString();
						
						String dtPedidoOrig = jsonObject
								.get("data_atualizacao").getAsString();

						Date dataPedido = formatoEntrada.parse(dtPedidoOrig);

						String dtPedido = formatoDesejado.format(dataPedido);

						System.out.println("Data Pedido: " + dtPedido);

						Date data = formatoOriginal.parse(dtVenc);
						String dataVencFormatada = formatoDesejado.format(data);

						String idAluno = jsonObject.get("aluno_id")
								.getAsString();
						
						String situacao_titulo = jsonObject.get("titulo_situacao")
								.getAsString();
						
						if(!situacao_titulo.equalsIgnoreCase("X") && vlrDesdob.compareTo(new BigDecimal("5")) > 0){
							
							BigDecimal codCenCus = getCodCenCusPeloCusto(taxaId);
							
							if(codCenCus != null){
								if(codCenCus.compareTo(BigDecimal.ZERO) == 0){
									codCenCus = getCodCenCus(idCurso);
								}
							}else{
								codCenCus = getCodCenCus(idCurso);
							}
							
							if(codCenCus != null){
								if(codCenCus.compareTo(BigDecimal.ZERO) == 0){
									codCenCus = rs.getBigDecimal("CODCENCUS");
								}
							}else{
								codCenCus = rs.getBigDecimal("CODCENCUS");
							}

							System.out.println("CodCenCus: " + codCenCus);
							System.out.println("Taxa id: " + taxaId);
							
							// Para os benefícios, você pode fazer um loop interno
							/*
							 * JsonArray beneficiosArray = jsonObject
							 * .getAsJsonArray("beneficios"); for (JsonElement
							 * beneficioElement : beneficiosArray) { JsonObject
							 * beneficioObject = beneficioElement
							 * .getAsJsonObject();
							 * 
							 * System.out .println("Beneficio ID: " +
							 * beneficioObject.get("beneficio_id") .getAsString());
							 * System.out.println("Beneficio Descrição: " +
							 * beneficioObject.get("beneficio_descricao")
							 * .getAsString());
							 * 
							 * }
							 */

							System.out.println("CodParc: " + codparc);
							
							if(validarDataLimite(dtPedido)){
								
								if (codparc.compareTo(BigDecimal.ZERO) != 0) {
									System.out.println("Entrou no parceiro: " + codparc);
									if (validarFin(idFin, codemp)) {
										System.out.println("Entrou no financeiro");
										
										BigDecimal codConta = getCodConta(codemp);
										
										BigDecimal codBanco = getCodBanco(codemp);
										
										String recDesp;
										
										if(getRecDesp(taxaId)){
											recDesp = "-1";
										}else{
											recDesp = "1";
										}
										
										BigDecimal nufin = insertFin(codemp, /* codemp */
												codCenCus, /* codCenCus */
												getNatureza(taxaId), /* codNat */
												BigDecimal.valueOf(1300), /* codTipOper */
												codparc, /* codparc */
												BigDecimal.valueOf(4), /* codtiptit */
												vlrDesdob, /* vlrDesdob */
												dataVencFormatada, /* dtvenc */
												// "25/11/2023", /* dtvenc */
												dtPedido, /* dtPedido */
												// "22/11/2023", /* dtPedido */
												idFin, aluno, codConta, codBanco, recDesp);
										System.out.println("Financeiro cadastrado");
										/*insertLogIntegracao(
												"Financeiro com Id Externo: "
														+ idFin
														+ " Criado Com Sucesso, numero unico interno: "
														+ nufin, "Sucesso");*/
									}else{
										System.out.println("Financeiro " + idFin + " ja cadastrado para o parceiro: " + codparc);
									}

								} else {
									/*insertLogIntegracao("Aluno com Id : " + idAluno
											+ " não Encontrado", "Aviso");*/
								}
								
							}else{
								System.out.println("Data pedido inferior a data limite");
							}
							
						}
					}
					
					//updateFlagAlunoIntegrado(aluno);
					
				} else {
					insertLogIntegracao(
							"Status retornado pela API diferente de 200, "
									+ "por favor verificar, Status retornado: "
									+ responseStatus, "Aviso");
				}

			}
			
			/*if(count == 0){
				updateResetarAlunos();
			}*/
			
		} catch (Exception e) {
			e.printStackTrace();
			try {
				insertLogIntegracao(
						"Erro ao integrar financeiro, Mensagem de erro: "
								+ e.getMessage(), "Erro");
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} finally {
			if (pstmt != null) {
				try {
					pstmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			jdbc.closeSession();
		}

	}

	public String[] apiGet(String ur, String token) throws Exception {

		BufferedReader reader;
		String line;
		StringBuilder responseContent = new StringBuilder();
		// String key = preferenciaSenha();

		// Preparando a requisição
		URL obj = new URL(ur);
		HttpURLConnection https = (HttpURLConnection) obj.openConnection();

		System.out.println("Entrou na API");
		System.out.println("URL: " + ur);
		System.out.println("https: " + https);

		https.setRequestMethod("GET");
		// https.setConnectTimeout(50000);
		https.setRequestProperty("User-Agent",
				"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)");
		https.setRequestProperty("Content-Type",
				"application/json; charset=UTF-8");
		https.setRequestProperty("Authorization", "Bearer "
		// + "2|VFBUMOCUNitomQYMrwWY7dCaTLts1Lsab3Bktpf5");
				+ token);
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

		return new String[] { Integer.toString(status), response };

	}

	public BigDecimal insertFin(BigDecimal codemp, BigDecimal codCenCus,
			BigDecimal codNat, BigDecimal codTipOper, BigDecimal codparc,
			BigDecimal codTipTit, BigDecimal vlrDesdbo, String dtVenc,
			String dtPedido, String idExterno, String idAluno, 
			BigDecimal codConta, BigDecimal codBanco, String recDesp) throws Exception {

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
					+ "         CODUSU,"
					+ "         AD_IDEXTERNO,"
					+ "         AD_IDALUNO) "
					+ "        VALUES (?, "
					+ "               NULL, "
					+ "               0, "
					+ "               'F', "
					+ "               "+recDesp+", "
					+ "               "+codemp+" , " // AS CODEMP
					+ "               "+codCenCus+" , " // AS CODCENCUS
					+ "               "+codNat+" , " // AS CODNAT
					+ "               "+codTipOper+" , " // AS CODTIPOPER
					+ "               (SELECT MAX(DHALTER) "
					+ "                  FROM TGFTOP "
					+ "                 WHERE CODTIPOPER = "+codTipOper+"), "
					+ "               0, "
					+ "               (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 0), "
					+ "               "+codparc+" , " // AS CODPARC
					+ "               "+codTipTit+" , " // AS CODTIPTIT
					+ "               "+vlrDesdbo+" , " // AS VLRDESDOB
					+ "               0, "
					+ "               0, "
					+ "               "+codBanco+", " // AS CODBCO
					+ "               "+codConta+", " // AS CODCTABCOINT
					+ "               '"+dtPedido+"' , " // AS DTNEG
					+ "               SYSDATE, "
					+ "               SYSDATE, "
					+ "               '"+dtVenc+"' , " // AS DTVENC
					+ "               SYSDATE, " // AS PRAZO
					+ "               '"+dtVenc+"' , " // AS DTVENCINIC
					+ "               1 , " // AS TIPJURO
					+ "               1 , " // AS TIPMULTA
					+ "               null , " // AS HISTORICO
					+ "               'I' , " // AS TIPMARCCHEQ
					+ "               'N' , " // AS AUTORIZADO
					+ "               'N' , " // AS BLOQVAR
					+ "               'N' , " // AS INSSRETIDO
					+ "               'N' , " // AS ISSRETIDO
					+ "               'N' , " // AS PROVISAO
					+ "               'N' , " // AS RATEADO
					+ "               'N' , " // AS TIMBLOQUEADA
					+ "               'S' , " // AS IRFRETIDO
					+ "               'S' , " // AS TIMTXADMGERALU
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0, "
					+ "               0, " + "               0,"
					+ "               '"+idExterno+"'," + "     '"+idAluno+"')";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.setBigDecimal(1, nufin);
			/*pstmt.setBigDecimal(2, codemp);
			pstmt.setBigDecimal(3, codCenCus);
			pstmt.setBigDecimal(4, codNat);
			pstmt.setBigDecimal(5, codTipOper);
			pstmt.setBigDecimal(6, codTipOper);
			pstmt.setBigDecimal(7, codparc);
			pstmt.setBigDecimal(8, codTipTit);
			pstmt.setBigDecimal(9, vlrDesdbo);
			pstmt.setString(10, dtPedido);
			pstmt.setString(11, dtVenc);
			pstmt.setString(12, dtVenc);
			pstmt.setString(13, idExterno);
			pstmt.setString(14, idAluno);*/

			pstmt.executeUpdate();

		} catch (Exception se) {
			se.printStackTrace();
			try {
				insertLogIntegracao(
						"Erro ao integrar financeiro, Mensagem de erro: "
								+ se.getMessage(), "Erro");
			} catch (Exception e1) {
				e1.printStackTrace();
			}
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

			updateNumFin();

			jdbc.openSession();

			// String sqlNota = "SELECT SEQ_TGFFIN_NUFIN.NEXTVAL FROM DUAL";
			String sqlNota = "SELECT MAX(ULTCOD) AS ULTCOD FROM TGFNUM WHERE ARQUIVO = 'TGFFIN'";

			pstmt = jdbc.getPreparedStatement(sqlNota);

			rs = pstmt.executeQuery();

			if (rs.next()) {

				id = rs.getBigDecimal("ULTCOD");
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

	public BigDecimal getCodParc(String idAluno) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;

		try {

			jdbc.openSession();

			String sqlNota = "SELECT NVL((SELECT CODPARC FROM AD_ALUNOS WHERE ID_EXTERNO = ?), 0) AS ID FROM DUAL";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setString(1, idAluno);

			rs = pstmt.executeQuery();

			if (rs.next()) {

				id = rs.getBigDecimal("ID");

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
	
	public BigDecimal getCodBanco(BigDecimal codEmp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		BigDecimal id = BigDecimal.ZERO;
		
		try {
			
			jdbc.openSession();
			
			String sqlNota = "select CODBCO " + "from ad_infobankbaixa "
					+ "WHERE CODEMP = ? " + "AND IDEXTERNO IS NULL";
			
			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setBigDecimal(1, codEmp);
			
			rs = pstmt.executeQuery();
			
			if (rs.next()) {
				
				id = rs.getBigDecimal("CODBCO");
				
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
	
	public BigDecimal getNatureza(String idExterno) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		BigDecimal id = BigDecimal.ZERO;
		
		try {
			
			jdbc.openSession();
			
			String sqlNota = "SELECT CODNAT FROM AD_NATACAD WHERE IDEXTERNO = '" + idExterno +"'"
					+" union "
					+ "SELECT 0 FROM DUAL WHERE NOT EXISTS (SELECT CODNAT FROM AD_NATACAD WHERE IDEXTERNO = '" + idExterno+"')" ;
			
			pstmt = jdbc.getPreparedStatement(sqlNota);
			
			rs = pstmt.executeQuery();
			
			if (rs.next()) {
				
				id = rs.getBigDecimal("CODNAT");
				
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
		
		if(id.compareTo(BigDecimal.ZERO) == 0){
			id = BigDecimal.valueOf(10101002);
		}
		
		return id;
	}
	
	public BigDecimal getCodConta(BigDecimal codEmp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		BigDecimal id = BigDecimal.ZERO;
		
		try {
			
			jdbc.openSession();
			
			String sqlNota = "select CODCTABCOINT " + "from ad_infobankbaixa "
					+ "WHERE CODEMP = ? " + "AND IDEXTERNO IS NULL";
			
			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setBigDecimal(1, codEmp);
			
			rs = pstmt.executeQuery();
			
			if (rs.next()) {
				
				id = rs.getBigDecimal("CODCTABCOINT");
				
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

	public BigDecimal getCodCenCus(String idCurso) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;

		try {

			jdbc.openSession();

			String sqlNota = "select codcencus from tsicus where ad_idexterno = ? "
					+ "	union     "
					+ "	select 0 from dual where not exists (select codcencus from tsicus where ad_idexterno = ?)";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setString(1, idCurso);
			pstmt.setString(2, idCurso);

			rs = pstmt.executeQuery();

			if (rs.next()) {

				id = rs.getBigDecimal("codcencus");

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
	
	public BigDecimal getCodCenCusPeloCusto(String idTaxa) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		BigDecimal id = BigDecimal.ZERO;
		
		try {
			
			jdbc.openSession();
			
			String sqlNota = "SELECT CODCENCUS FROM AD_NATACAD WHERE IDEXTERNO = " + idTaxa;
			
			pstmt = jdbc.getPreparedStatement(sqlNota);
			
			rs = pstmt.executeQuery();
			
			if (rs.next()) {
				
				id = rs.getBigDecimal("CODCENCUS");
				
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

	public boolean validarFin(String idFin, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int count = 0;

		try {

			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TGFFIN WHERE AD_IDEXTERNO = ? AND CODEMP = " + codemp;

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setString(1, idFin);

			rs = pstmt.executeQuery();

			if (rs.next()) {

				count = rs.getInt("COUNT");

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

		if (count > 0) {
			return false;
		} else {
			return true;
		}
	}
	
	public boolean validarDataLimite(String data) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		int count = 0;
		
		try {
			
			jdbc.openSession();
			
			String sqlNota = "SELECT COUNT(0) AS COUNT FROM DUAL WHERE TO_DATE('"+data+"') < ADD_MONTHS(SYSDATE, 12 * -(SELECT NVL(INTEIRO,0) FROM TSIPAR WHERE CHAVE = 'LIMINFANO')) ";
			
			pstmt = jdbc.getPreparedStatement(sqlNota);
			
			rs = pstmt.executeQuery();
			
			if (rs.next()) {
				
				count = rs.getInt("COUNT");
				
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
		
		if (count > 0) {
			return false;
		} else {
			return true;
		}
	}

	public BigDecimal getAluno(BigDecimal codparc) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;

		try {

			jdbc.openSession();

			String sqlNota = "SELECT ID_EXTERNO FROM AD_ALUNOS WHERE CODPARC = ?";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setBigDecimal(1, codparc);

			rs = pstmt.executeQuery();

			if (rs.next()) {

				id = rs.getBigDecimal("ID_EXTERNO");

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
	
	public boolean getRecDesp(String idExterno) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		int id = 0;
		
		try {
			
			jdbc.openSession();
			
			String sqlNota = "SELECT SUBSTR(CODNAT, 0, 1) NAT "
					+ "FROM AD_NATACAD where idexterno = " + idExterno + " "
						+ "union SELECT '0' FROM DUAL "
						+ "WHERE NOT EXISTS (SELECT SUBSTR(CODNAT, 0, 1) NAT FROM AD_NATACAD where idexterno = " + idExterno+")";
			
			pstmt = jdbc.getPreparedStatement(sqlNota);
			
			rs = pstmt.executeQuery();
			
			if (rs.next()) {
				
				id = Integer.parseInt(rs.getString("NAT"));
				
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
		
		if(id > 1){
			return true;
		}else{
			return false;
		}
	}

	public void updateNumFin() throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		try {
		
		jdbc.openSession();

		String sqlUpdate = "UPDATE TGFNUM SET ULTCOD = NVL(ULTCOD, 0) + 1  WHERE ARQUIVO = 'TGFFIN'";

		pstmt = jdbc.getPreparedStatement(sqlUpdate);
		pstmt.executeUpdate();

		
			
		} catch (Exception se) {
			se.printStackTrace();
		}finally{
			if (pstmt != null) {
				pstmt.close();
			}
			if (jdbc != null) {
				jdbc.closeSession();
			}
		}

	}
	
	public void updateFlagAlunoIntegrado(String idAluno) throws Exception {
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		try {
			
			jdbc.openSession();
			
			String sqlUpdate = "UPDATE AD_ALUNOS SET INTEGRADO = 'S' WHERE ID_EXTERNO = '"+idAluno+"'";
			
			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.executeUpdate();
			
			
			
		} catch (Exception se) {
			se.printStackTrace();
		}finally{
			if (pstmt != null) {
				pstmt.close();
			}
			if (jdbc != null) {
				jdbc.closeSession();
			}
		}
		
	}
	
	public void updateResetarAlunos() throws Exception {
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		try {
			System.out.println("Entrou no UPDATE da flag dos alunos");
			jdbc.openSession();
			
			String sqlUpdate = "UPDATE AD_ALUNOS SET INTEGRADO = 'N'";
			
			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.executeUpdate();
			
			
			
		} catch (Exception se) {
			se.printStackTrace();
		}finally{
			if (pstmt != null) {
				pstmt.close();
			}
			if (jdbc != null) {
				jdbc.closeSession();
			}
		}
		
	}

	public void insertLogIntegracao(String descricao, String status)
			throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		try {
		
		jdbc.openSession();

		String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, STATUS)"
				+ "VALUES (((SELECT NVL(MAX(NUMUNICO), 0) + 1 FROM AD_LOGINTEGRACAO)), ?, SYSDATE, ?)";

		pstmt = jdbc.getPreparedStatement(sqlUpdate);
		pstmt.setString(1, descricao);
		pstmt.setString(2, status);
		pstmt.executeUpdate();

		
			
		} catch (Exception se) {
			se.printStackTrace();
		}finally{
			if (pstmt != null) {
				pstmt.close();
			}
			if (jdbc != null) {
				jdbc.closeSession();
			}
		}

	}
	
	private void updateCarga(BigDecimal idCarga) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try
		{
			jdbc.openSession();
			
			String sqlUpd = "UPDATE AD_CARGAALUNOS SET INTEGRADO_FIN = 'S' WHERE IDCARGA = " + idCarga;
			
			pstmt = jdbc.getPreparedStatement(sqlUpd);
			pstmt.executeUpdate();
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}
	
	private void resetCarga(BigDecimal codEmp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try
		{
			jdbc.openSession();
			
			String sqlUpd = "UPDATE AD_CARGAALUNOS SET INTEGRADO_FIN = 'N' WHERE CODEMP = " + codEmp + " AND INTEGRADO_FIN = 'S'";
			
			pstmt = jdbc.getPreparedStatement(sqlUpd);
			pstmt.executeUpdate();
		}
		catch (SQLException e)
		{
			e.printStackTrace();
		}
		finally
		{
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}
	
}
