package br.com.sankhya.acoesgrautec.jobs;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

public class JobGetTituloFornecedor implements ScheduledAction{

	@Override
	public void onTime(ScheduledActionContext arg0) {
		// TODO Auto-generated method stub
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal codEmp = BigDecimal.ZERO;

		String url = "";
		String token = "";

		System.out.println("Iniciou a JobGetTituloFornecedor");

		try {

			jdbc.openSession();
 
			String query3 = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO WHERE CODEMP = 3";
			String query4 = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO WHERE CODEMP = 4";

			pstmt = jdbc.getPreparedStatement(query3);

			rs = pstmt.executeQuery();

			while (rs.next()) {
				
				codEmp = rs.getBigDecimal("CODEMP");
				url = rs.getString("URL");
				token = rs.getString("TOKEN");
				leituraJSON(url, token, codEmp);
				
			}

		} catch (Exception e) {
			e.printStackTrace();
			try {
				insertLogIntegracao("Erro ao integrar financeiro, Mensagem de erro: "+ e.getMessage(), "Erro");
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
			System.out.println("Finalizou a JobGetTituloFornecedor");
		}
	}
	
	public void insertLogIntegracao(String descricao, String status)
			throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		String descFormatada = descricao;
		
		jdbc.openSession();
		
		if(descricao.length() > 4000){
			
		descFormatada = descricao.substring(0,4000);
		}else{
			descFormatada = descricao;
		}

		String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, STATUS)"
				+ "VALUES (((SELECT NVL(MAX(NUMUNICO), 0) + 1 FROM AD_LOGINTEGRACAO)), ?, SYSDATE, ?)";

		pstmt = jdbc.getPreparedStatement(sqlUpdate);
		pstmt.setString(1, descFormatada);
		pstmt.setString(2, status);
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
	
	public String[] apiGet(String ur, String token) throws Exception {

		BufferedReader reader;
		String line;
		StringBuilder responseContent = new StringBuilder();
		// String key = preferenciaSenha();

		// Preparando a requisi��o
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
		https.setRequestProperty("Authorization", "Bearer "	+ token);
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
	
	public void updateNumFin() throws Exception {

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

	public void leituraJSON(String url,String token, BigDecimal codemp) throws Exception {
		System.out.println("Inicio leitura do JSON - JobGetTituloFornecedor");
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		BigDecimal codparc = null;
		
		Date dataAtual = new Date();
        SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");

        Calendar calendar = Calendar.getInstance();
        calendar.setTime(dataAtual);
        calendar.add(Calendar.DAY_OF_MONTH, -1); 

        Date dataUmDiaAtras = calendar.getTime();
        
        String dataUmDiaFormatada = formato.format(dataUmDiaAtras);
        String dataAtualFormatada = formato.format(dataAtual);
        
        System.out.println("data um dia atras forn titulo: " + dataUmDiaFormatada);
        System.out.println("data normal forn titulo: " + dataAtualFormatada);
		
		int count = 0;
		try{
			jdbc.openSession();
									/*,ad_idfornecedor*/
			String sqlP = "select * from AD_IDFORNACAD where NVL(INTEGRADOFIN, 'N') = 'N' AND CODEMP = "+codemp+" AND ROWNUM <= 300";

			pstmt = jdbc.getPreparedStatement(sqlP);

			rs = pstmt.executeQuery();

			while (rs.next()) {
				
			count++;
			
			String idFornecedor = rs.getString("IDACADWEB");
			String [] response = apiGet(url+ "/financeiro/clientes/titulos-pagar?fornecedor="+idFornecedor
					//+"situacao=A"
					//+"&vencimentoInicial=2024-09-01&vencimentoFinal=2024-12-31"
					+"&dataInicial="+dataUmDiaFormatada+" 00:00:00&dataFinal="+dataAtualFormatada+" 23:59:59"
					,token);
			
			System.out.println("Retorno da api");
			System.out.println(response);
			System.out.println("Retorno da api [0]");
			System.out.println(response[0]);
			System.out.println("Retorno da api[1]");
			System.out.println(response[1]);
			JsonParser parser = new JsonParser();
			
			JsonArray jsonArray = parser.parse(response[1]).getAsJsonArray();
			codparc = rs.getBigDecimal("CODPARC");
			
			
			if(response[0].equalsIgnoreCase("200")) {
				System.out.println("API response code: " + response[0]);
				
				for(JsonElement jsonElement : jsonArray){
					System.out.println("comecou a leitura do JSON");
						JsonObject JSON = jsonElement.getAsJsonObject();
						
	//					pegando os campos do response
//						String fornecedorId = JSON.get("fornecedor_id").getAsString();
						
//						String nodeParc = JSON.get("fornecedor_nome").getAsString();
						
						String idFin = JSON.get("titulo_id").getAsString();
						
						String taxaId = JSON.get("taxa_id").getAsString();
//						String taxaDescricao = JSON.get("taxa_descricao").getAsString();
//						String taxaCategoria = JSON.get("titulo_categoria").getAsString();
//						String taxaCategoriaDesc = JSON.get("taxa_categoria_descricao").getAsString();
						
						String dtVenc = JSON.get("titulo_vencimento").getAsString();
						
						String vlrDesdob = JSON.get("titulo_valor").getAsString();
						
//						String tituloMesRef = JSON.get("titulo_mes_ref").getAsString();
//						String tituloAnoRef = JSON.get("titulo_ano_ref").getAsString();
						String tituloSituacao = "";//= JSON.get("titulo_situacao").getAsString();
						
						if(!JSON.get("titulo_situacao").isJsonNull()){
							
							tituloSituacao = JSON.get("titulo_situacao").getAsString();		
						}
						
						String tituloObservacao = "";
						if(!JSON.get("titulo_observacao").isJsonNull()){
							
							tituloObservacao = JSON.get("titulo_observacao").getAsString();		
						}
						
						System.out.println("teste campo curso ir: " + JSON.get("curso_id").isJsonNull());
						String cursoId ="";
						if(!JSON.get("curso_id").isJsonNull()){
							
							cursoId = JSON.get("curso_id").getAsString();			
						}
						
//						String curriculoId = JSON.get("curriculo_id").getAsString();
//						String turmaId = JSON.get("turma_id").getAsString();
						String dtPedido =  JSON.get("data_atualizacao").getAsString();
//						String fornecedor = JSON.get("fornecedor").getAsString();
//						
						
						System.out.println("Leu todos os campo de um JSON");
						String recDesp = "";
						BigDecimal codCenCus = getCodCenCusPeloCusto(taxaId);
						
						
//						Formatando o DATA DO PEDIDO
						SimpleDateFormat formatoHoraMs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
						Date dataHora = formatoHoraMs.parse(dtPedido);
						SimpleDateFormat formatoHora = new SimpleDateFormat("dd/MM/yyyy");
						String dtPedidoFormatado = formatoHora.format(dataHora);
			         
//						Formatando o DATA DE VENCIMENTO
						SimpleDateFormat formatoEntrada = new SimpleDateFormat("yyyy-MM-dd");
			            Date data = formatoEntrada.parse(dtVenc);
			            SimpleDateFormat formatoSaida = new SimpleDateFormat("dd/MM/yyyy");
			            String dataVencFormatada = formatoSaida.format(data);
			            
			      
			            System.out.println("Formatou as datas");
						
						if(codCenCus != null){
							if(codCenCus.compareTo(BigDecimal.ZERO) == 0){
								codCenCus = getCodCenCus(cursoId);
							}
						}else{
							codCenCus = getCodCenCus(cursoId);
						}
						
						System.out.println("CodCenCus: " + codCenCus);
						System.out.println("Taxa id: " + taxaId);
						
				
						if(codCenCus != null){
							if(codCenCus.compareTo(BigDecimal.ZERO) == 0){
								codCenCus = getCodCenCusPeloCusto(taxaId);
							}
						}else{
							codCenCus = getCodCenCusPeloCusto(taxaId);
						}
						System.out.println("Formatou as datas");
						
						if(!tituloSituacao.equalsIgnoreCase("X")){
							if (codparc.compareTo(BigDecimal.ZERO) != 0) {
								System.out.println("Entrou no parceiro: " + codparc);
								if (validarFin(idFin, codemp)) {
									System.out.println("Entrou no financeiro");
									
									BigDecimal codConta = getCodConta(codemp);
									
									BigDecimal codBanco = getCodBanco(codemp);
									
									/*codConta = new BigDecimal("1");
									codBanco = new BigDecimal("341");*/
									
									recDesp = "-1";
									
									/*if(getRecDesp(taxaId)){
										recDesp = "-1";
									}else{
										recDesp = "1";
									}*/
									
									BigDecimal vlrDesdobBigDecimal = new BigDecimal( vlrDesdob);
									
									BigDecimal nufin = insertFinanceiro(codemp, /* codemp */
											codCenCus, /* codCenCus */
											getNatureza(taxaId), /* codNat */
											BigDecimal.valueOf(1300), /* codTipOper */
											codparc, /* codparc */
											BigDecimal.valueOf(4), /* codtiptit */
											vlrDesdobBigDecimal, /* vlrDesdob */
											dataVencFormatada, /* dtvenc */
											// "25/11/2023", /* dtvenc */
											dtPedidoFormatado, /* dtPedido */
											// "22/11/2023", /* dtPedido */
											idFin, "", codConta, codBanco, recDesp, tituloObservacao);
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
								insertLogIntegracao("Financeiro com Id Externo: "+ idFin+ " Criado Com erro", "Erro");
							}
						}
						
//						insertFinanceiro(codemp);
								
					}
				}else{
					//insertLogIntegracao("Response Code Diferente de 200","Response Code: " + response[0]);
				}
				
				updateFlagFornIntegrado(idFornecedor, codemp);
				
			}
			
			if(count == 0){
				updateResetarForn();
			}
			
		}catch(Exception e){
			
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			String stackTraceAsString = sw.toString();
			insertLogIntegracao("Ocorreu um erro na fun��o LeituraJSON(); "+ stackTraceAsString,"erro");
			e.printStackTrace();
		}
		finally{
			
			if(rs != null){
				rs.close();
			}
			if(pstmt != null){
				pstmt.close();
			}
			jdbc.closeSession();
			System.out.println("Fim leitura do JSON - JobGetTituloFornecedor");
		}

		
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
			id = BigDecimal.valueOf(30701010);
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
		
	public BigDecimal insertFinanceiro(BigDecimal codemp, BigDecimal codCenCus,
			BigDecimal codNat, BigDecimal codTipOper, BigDecimal codparc,
			BigDecimal codTipTit, BigDecimal vlrDesdbo, String dtVenc,
			String dtPedido, String idExterno, String idAluno, 
			BigDecimal codConta, BigDecimal codBanco, String recDesp, String obs) throws Exception {
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement ps = null;
		
		BigDecimal nufin = getMaxNumFin();			
		
		try	{
			System.out.println("Deu inicio ao insertTGFFIN()");
			
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
					+ "               -1, "
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
					+ "               '"+obs+"' , " // AS HISTORICO
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

			ps = jdbc.getPreparedStatement(sqlUpdate);
			ps.setBigDecimal(1, nufin);
	

			ps.executeUpdate();

			
		}catch(Exception e){
			e.printStackTrace();
			throw new Exception(e.getCause());
		}finally{
			if(ps != null){
				ps.close();
			}
			jdbc.closeSession();
			System.out.println("Deu Fim ao insertTGFFIN()");
		}
		
		return nufin;
		
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
	

	public void updateFlagFornIntegrado(String idAluno, BigDecimal codemp) throws Exception {
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		try {
			
			jdbc.openSession();
			
			String sqlUpdate = "UPDATE AD_IDFORNACAD SET INTEGRADOFIN = 'S' WHERE IDACADWEB = '"+idAluno+"' AND CODEMP = " + codemp;
			
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
	
	public void updateResetarForn() throws Exception {
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		try {
			System.out.println("Entrou no UPDATE da flag dos alunos");
			jdbc.openSession();
			
			String sqlUpdate = "UPDATE AD_IDFORNACAD SET INTEGRADOFIN = 'N'";
			
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

	
}