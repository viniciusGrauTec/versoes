package br.com.sankhya.acoesgrautec.extensions;

import br.com.sankhya.acoesgrautec.util.EnviromentUtils;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.activiti.engine.impl.util.json.JSONArray;
import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

public class AcaoGetFornecedoresCarga implements AcaoRotinaJava, ScheduledAction {

	 	private List<String> selectsParaInsert = new ArrayList<String>();
	 	private EnviromentUtils util = new EnviromentUtils();
	 	
	 	@Override
	 	public void doAction(ContextoAcao contexto) throws Exception {
	 		
	 		Registro[] linhas = contexto.getLinhas();
	 		Registro registro = linhas[0];
	 		
	 		String url = (String) registro.getCampo("URL");
	 		String token = (String) registro.getCampo("TOKEN");
	 		BigDecimal codEmp = (BigDecimal) registro.getCampo("CODEMP");
	 		
	 		String dataInicio = contexto.getParam("DTINICIO").toString().substring(0, 10);
	 		String dataFim = contexto.getParam("DTFIM").toString().substring(0, 10);
	 		String idForn = (String) contexto.getParam("IDFORN");
	 		
	 		
	 		try {

	 			// Parceiros
	 			List<Object[]> listInfParceiro = retornarInformacoesParceiros();
	 			Map<String, BigDecimal> mapaInfParceiros = new HashMap<String, BigDecimal>();
	 			for (Object[] obj : listInfParceiro) {

	 				BigDecimal codParc = (BigDecimal) obj[0];
	 				String cpf_cnpj = (String) obj[1];
	 				String idExterno = (String) obj[2];
	 				BigDecimal codemp = (BigDecimal) obj[3];

	 				if (mapaInfParceiros.get(cpf_cnpj) == null) {
	 					mapaInfParceiros.put(cpf_cnpj, codParc);
	 				}
	 			}

	 			// Id Forncedor
	 			Map<String, BigDecimal> mapaInfIdParceiros = new HashMap<String, BigDecimal>();
	 			for (Object[] obj : listInfParceiro) {

	 				BigDecimal codParc = (BigDecimal) obj[0];
	 				String cpf_cnpj = (String) obj[1];
	 				String idExterno = (String) obj[2];
	 				BigDecimal codemp = (BigDecimal) obj[3];

	 				if (mapaInfIdParceiros.get(idExterno + "###" + cpf_cnpj + "###"
	 						+ codemp) == null) {
	 					mapaInfIdParceiros.put(idExterno + "###" + cpf_cnpj + "###"
	 							+ codemp, codParc);
	 				}
	 			}
	 			
	 			processDateRange(mapaInfIdParceiros, mapaInfParceiros, url,                               //processDateRange
	 					token, codEmp, dataInicio, dataFim, idForn);
	 			
	 			contexto.setMensagemRetorno("Periodo Processado!");
	 			
	 		}catch(Exception e){
	 			e.printStackTrace();
	 			contexto.mostraErro(e.getMessage());
	 		}finally{

	 			if(selectsParaInsert.size() > 0){
	 				
	 				StringBuilder msgError = new StringBuilder();
	 				
	 				System.out.println("Entrou na lista do finally: " + selectsParaInsert.size());
	 				
	 				//BigDecimal idInicial = util.getMaxNumLog();
	 				
	 				int qtdInsert = selectsParaInsert.size();
	 				
	 				int i = 1;
	 				for (String sqlInsert : selectsParaInsert) {
	 					String sql = sqlInsert;
	 					int nuFin = util.getMaxNumLog();
	 					sql = sql.replace("<#NUMUNICO#>", String.valueOf(nuFin));
	 					msgError.append(sql);

	 					if (i < qtdInsert) {
	 						msgError.append(" \nUNION ALL ");
	 					}
	 					i++;
	 				}
	 				
	 				System.out.println("Consulta de log: \n" + msgError);
	 				insertLogList(msgError.toString());
	 				
	 			}
	 		
	 		}
	 		
	 	}
	 	
	 	@Override
	 	public void onTime(ScheduledActionContext arg0) {
	 		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	 		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	 		PreparedStatement pstmt = null;
	 		ResultSet rs = null;

	 		BigDecimal codEmp = BigDecimal.ZERO;

	 		String url = "";
	 		String token = "";

	 		System.out.println("Iniciou o cadastro dos fornecedores no Job");
	 		try {

	 			// Parceiros
	 			List<Object[]> listInfParceiro = retornarInformacoesParceiros();
	 			Map<String, BigDecimal> mapaInfParceiros = new HashMap<String, BigDecimal>();
	 			for (Object[] obj : listInfParceiro) {

	 				BigDecimal codParc = (BigDecimal) obj[0];
	 				String cpf_cnpj = (String) obj[1];
	 				String idExterno = (String) obj[2];
	 				BigDecimal codemp = (BigDecimal) obj[3];

	 				if (mapaInfParceiros.get(cpf_cnpj) == null) {
	 					mapaInfParceiros.put(cpf_cnpj, codParc);
	 				}
	 			}

	 			// Id Fornecedor
	 			Map<String, BigDecimal> mapaInfIdParceiros = new HashMap<String, BigDecimal>();
	 			for (Object[] obj : listInfParceiro) {

	 				BigDecimal codParc = (BigDecimal) obj[0];
	 				String cpf_cnpj = (String) obj[1];
	 				String idExterno = (String) obj[2];
	 				BigDecimal codemp = (BigDecimal) obj[3];

	 				if (mapaInfIdParceiros.get(idExterno + "###" + cpf_cnpj + "###"
	 						+ codemp) == null) {
	 					mapaInfIdParceiros.put(idExterno + "###" + cpf_cnpj + "###"
	 							+ codemp, codParc);
	 				}
	 			}

	 			jdbc.openSession();

	 			String query = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO";
	 			//String query3 = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO WHERE CODEMP = 3";
	 			//String query4 = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO WHERE CODEMP = 4";

	 			pstmt = jdbc.getPreparedStatement(query);

	 			rs = pstmt.executeQuery();
	 			while (rs.next()) {
	 				System.out.println("While principal");

	 				codEmp = rs.getBigDecimal("CODEMP");

	 				url = rs.getString("URL");
	 				token = rs.getString("TOKEN");
	 				
	 				
	 				//System.out.println("ENTROU NO ONTIME DA  acaoGetFornecedorTiposCarga! ");
	 				
	 				//chamar o m�todo selecionarTiposFornecedores
	 				//selecionarTiposFornecedores(jdbc, mapaInfIdParceiros, mapaInfParceiros, url, token, codEmp);

	 				iterarEndpoint(mapaInfIdParceiros, mapaInfParceiros, url,
	 						token, codEmp);
	 			}
	 			System.out
	 					.println("Finalizou o cadastro dos fornecedores no Job");
	 		} catch (Exception e) {
	 			e.printStackTrace();
	 			try {
	 				insertLogIntegracao(
	 						"Erro ao integrar Fornecedores, Mensagem de erro: "
	 								+ e.getMessage(), "Erro", "");
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
	 			

	 			if(selectsParaInsert.size() > 0){
	 				
	 				StringBuilder msgError = new StringBuilder();
	 				
	 				System.out.println("Entrou na lista do finally: " + selectsParaInsert.size());
	 				
	 				//BigDecimal idInicial = util.getMaxNumLog();
	 				
	 				int qtdInsert = selectsParaInsert.size();
	 				
	 				int i = 1;
	 				for (String sqlInsert : selectsParaInsert) {
	 					String sql = sqlInsert;
	 					int nuFin = 0;
	 					
	 					try {
	 						nuFin = util.getMaxNumLog();
	 					} catch (Exception e) {
	 						e.printStackTrace();
	 					}
	 					
	 					sql = sql.replace("<#NUMUNICO#>", String.valueOf(nuFin));
	 					msgError.append(sql);

	 					if (i < qtdInsert) {
	 						msgError.append(" \nUNION ALL ");
	 					}
	 					i++;
	 				}
	 				
	 				System.out.println("Consulta de log: \n" + msgError);
	 				try {
	 					insertLogList(msgError.toString());
	 				} catch (Exception e) {
	 					e.printStackTrace();
	 				}
	 				
	 				msgError = null;
	 				this.selectsParaInsert = new ArrayList<String>();
	 				
	 			}
	 		
	 		}
	 	}
	 	
	 	//NOVO METODO AUXILIAR AQUI
//	 	public void selecionarTiposFornecedores(JdbcWrapper jdbc, Map<String, BigDecimal> mapaInfIdParceiros,
//	 			Map<String, BigDecimal> mapaInfParceiros, String url, String token, BigDecimal codEmp) {
//	 		PreparedStatement pstmt = null;
//	 		ResultSet rs = null;
	 //
//	 		try {
//	 			jdbc.openSession();
	 //
//	 			String query = "SELECT PK, DESCRICAO FROM AD_FORNECEDOR_TIPO";
//	 			pstmt = jdbc.getPreparedStatement(query);
	 //
//	 			rs = pstmt.executeQuery();
	 //
//	 			// Itera sobre o ResultSet
//	 			while (rs.next()) {
//	 				String sigla = rs.getString("SIGLA"); // Coluna PK
//	 				String descricao = rs.getString("DESCRICAO"); 
//	 															
//	 				String urlComTipoFornecedor = url + "&fornecedor_tipo=" + descricao;
//	                 
//	 				System.out.println("ENTROU NO METODO DA  acaoGetFornecedorTiposCarga! ");
//	 				
//	 				// Chama o m�todo iterarEndpoint para cada tipo de fornecedor
//	 				iterarEndpoint(mapaInfIdParceiros, mapaInfParceiros, urlComTipoFornecedor, token, codEmp);
//	 			}
	 //
//	 		} catch (SQLException e) {
//	 			e.printStackTrace();
//	 		} catch (Exception e) {
//	 			e.printStackTrace();
//	 		} finally {
//	 			if (rs != null) {
//	 				try {
//	 					rs.close();
//	 				} catch (SQLException e) {
//	 					e.printStackTrace();
//	 				}
//	 			}
//	 			if (pstmt != null) {
//	 				try {
//	 					pstmt.close();
//	 				} catch (SQLException e) {
//	 					e.printStackTrace();
//	 				}
//	 			}
//	 		}
//	 	}
	 	
	 	
	 	//processarFornecedoresPorTipo
	 	//dentro desse m�todo vai precisar ter 1 select nessa tabela nova e no while desse select da tabela nova vai passar o iterarendpoint.
	 	/*
	 	 * ResultSet rs = null;
	 		List<Object[]> listRet = new ArrayList<>();

	 		try {
	 			jdbc.openSession();
	 			String sql = "SELECT p.CODPARC, p.CGC_CPF, "
	 					+ "a.IDACADWEB, a.codemp " + "FROM TGFPAR p "
	 					+ "LEFT join AD_IDFORNACAD a "
	 					+ "on a.codparc = p.codparc " + "where fornecedor = 'S'";

	 			pstmt = jdbc.getPreparedStatement(sql);
	 			rs = pstmt.executeQuery();

	 			while (rs.next()) {
	 				Object[] ret = new Object[4];
	 				ret[0] = rs.getBigDecimal("CODPARC");
	 				ret[1] = rs.getString("CGC_CPF");
	 				ret[2] = rs.getString("IDACADWEB");
	 				ret[3] = rs.getBigDecimal("codemp");

	 				listRet.add(ret);
	 			}

	 		}
	 	 */


//	 	public void iterarEndpoint(Map<String, BigDecimal> mapaInfIdParceiros,
//	 			Map<String, BigDecimal> mapaInfParceiros, String url, String token,
//	 			BigDecimal codEmp, String dataInicio, String dataFim, String idForn) 
//	 			throws Exception {
	 //
	 //
//	 		try {
//	 			
//	 			// Convertendo as Strings para LocalDate
//	 	        LocalDate inicio = LocalDate.parse(dataInicio);
//	 	        LocalDate fim = LocalDate.parse(dataFim);
	 //
//	 	        // Loop para percorrer o intervalo de dias
//	 	        LocalDate atual = inicio;
//	 	        
//	 	        String[] response = null;
//	 	        
//	 			while (!atual.isAfter(fim)) {
//	 				
//	 				String dataAtual = atual.toString();
//	 				
//	 				System.out.println("While de itera��o");
//	 				
//	 				if(idForn != null && !idForn.isEmpty()){
	 //
//	 					response = apiGet(url
//	 							+ "/financeiro/clientes/fornecedores?" + "quantidade=0"
//	 							+ "&dataInicial=" + dataAtual
//	 							+ " 00:00:00&dataFinal=" + dataAtual + " 23:59:59"
//	 							+ "&fornecedor=" + idForn
//	 							,token);
//	 				}else{
	 //
//	 					response = apiGet(url
//	 							+ "/financeiro/clientes/fornecedores?" + "quantidade=0"
//	 							+ "&dataInicial=" + dataAtual
//	 							+ " 00:00:00&dataFinal=" + dataAtual + " 23:59:59"
//	 							,token);
//	 				}
	 //
//	 				int status = Integer.parseInt(response[0]);
//	 				System.out.println("Status teste: " + status);
	 //
//	 				String responseString = response[1];
//	 				System.out.println("response string: " + responseString);
	 //
//	 				cadastrarFornecedor(mapaInfIdParceiros, mapaInfParceiros,
//	 						response, codEmp);
//	 				
//	 				// Incrementa para o proximo dia
//	 				atual = atual.plusDays(1);
//	 			}
//	 			
	 //
//	 		} catch (Exception e) {
//	 			e.printStackTrace();
//	 		}
//	 	}
	 	
	 	public void processDateRange(
	 	        Map<String, BigDecimal> mapaInfIdParceiros,
	 	        Map<String, BigDecimal> mapaInfParceiros,
	 	        String url,
	 	        String token,
	 	        BigDecimal codEmp,
	 	        String dataInicio,
	 	        String dataFim,
	 	        String idForn) throws Exception {

	 	    try {
	 	        // Convertendo as Strings para LocalDate
	 	        LocalDate inicio = LocalDate.parse(dataInicio);
	 	        LocalDate fim = LocalDate.parse(dataFim);
	 	        LocalDate atual = inicio;

	 	        while (!atual.isAfter(fim)) {
	 	            String dataAtual = atual.toString();
	 	            
	 	            // Preparar as datas completas para o dia atual
	 	            String dataInicialCompleta = dataAtual + " 00:00:00";
	 	            String dataFinalCompleta = dataAtual + " 23:59:59";

	 	            // Codificar os parâmetros
	 	            String dataInicialEncoded = URLEncoder.encode(dataInicialCompleta, "UTF-8");
	 	            String dataFinalEncoded = URLEncoder.encode(dataFinalCompleta, "UTF-8");

	 	            // Lista para armazenar todos os registros do dia
	 	            JSONArray registrosDoDia = new JSONArray();
	 	            int pagina = 1;
	 	            boolean temMaisRegistros = true;

	 	            while (temMaisRegistros) {
	 	                // Construir a URL para a página atual
	 	                StringBuilder urlBuilder = new StringBuilder();
	 	                urlBuilder.append(url.trim())
	 	                        .append("/financeiro/clientes/fornecedores")
	 	                        .append("?pagina=").append(pagina)
	 	                        .append("&quantidade=100")
	 	                        .append("&dataInicial=").append(dataInicialEncoded)
	 	                        .append("&dataFinal=").append(dataFinalEncoded);

	 	                // Adicionar parâmetro de fornecedor se estiver presente
	 	                if (idForn != null && !idForn.isEmpty()) {
	 	                    String fornecedorEncoded = URLEncoder.encode(idForn, "UTF-8");      
	 	                    urlBuilder.append("&fornecedor=").append(fornecedorEncoded);
	 	                }

	 	                String urlCompleta = urlBuilder.toString();
	 	                System.out.println("URL para fornecedores (data: " + dataAtual + ", página " + pagina + "): " + urlCompleta);

	 	                // Fazer a requisição
	 	                String[] response = apiGet2(urlCompleta, token);
	 	                int status = Integer.parseInt(response[0]);

	 	                if (status == 200) {
	 	                    JSONArray paginaAtual = new JSONArray(response[1]);
	 	                    
	 	                    // Adicionar registros ao array acumulado
	 	                    for (int i = 0; i < paginaAtual.length(); i++) {
	 	                        registrosDoDia.put(paginaAtual.getJSONObject(i));
	 	                    }
	 	                    
	 	                    // Verificar se é a última página
	 	                    if (paginaAtual.length() < 100) {
	 	                        temMaisRegistros = false;
	 	                    } else {
	 	                        pagina++;
	 	                    }
	 	                    
	 	                    System.out.println("Data " + dataAtual + " - Página " + pagina + ": " + 
	 	                                     paginaAtual.length() + " registros. Total acumulado: " + 
	 	                                     registrosDoDia.length());
	 	                } else {
	 	                    throw new Exception(String.format(
	 	                        "Erro na requisição de fornecedores. Status: %d. Resposta: %s. URL: %s",
	 	                        status, response[1], urlCompleta
	 	                    ));
	 	                }
	 	            }

	 	            // Processar todos os registros do dia atual
	 	            String[] responseArray = new String[]{String.valueOf(200), registrosDoDia.toString()};
	 	            
	 	            
	 	            
	 	            cadastrarFornecedor(mapaInfIdParceiros, mapaInfParceiros, responseArray, codEmp);

	 	            // Incrementar para o próximo dia
	 	            atual = atual.plusDays(1);
	 	            System.out.println("Total de registros processados para " + dataAtual + ": " + registrosDoDia.length());
	 	        }

	 	    } catch (Exception e) {
	 	        System.err.println("Erro ao processar período " + dataInicio + " até " + dataFim + ": " + e.getMessage());
	 	        e.printStackTrace();
	 	        throw e;
	 	    }
	 	}
	 	
	 	public void iterarEndpoint(Map<String, BigDecimal> mapaInfIdParceiros,
	 			Map<String, BigDecimal> mapaInfParceiros, String url, String token,
	 			BigDecimal codEmp) throws Exception {
	 		
	 		Date dataAtual = new Date();

	 		SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");

	 		String dataFormatada = formato.format(dataAtual);
	 		
	 		try {

	 			System.out.println("While de iteracao");

	 			String[] response = apiGet2(url
	 					+ "/financeiro/clientes/fornecedores?" + "quantidade=0"
	 					+ "&dataInicial=" + dataFormatada + " 00:00:00&dataFinal="
	 					+ dataFormatada + " 23:59:59", token);

	 			int status = Integer.parseInt(response[0]);
	 			System.out.println("Status teste: " + status);

	 			String responseString = response[1];
	 			System.out.println("response string: " + responseString);

	 			cadastrarFornecedor(mapaInfIdParceiros, mapaInfParceiros,
	 					response, codEmp);

	 		} catch (Exception e) {
	 			e.printStackTrace();
	 		}
	 	}

	 	public void cadastrarFornecedor(Map<String, BigDecimal> mapaInfIdParceiros,
	 			Map<String, BigDecimal> mapaInfParceiros, String[] response,
	 			BigDecimal codEmp) {
	 		System.out.println("Cadastro principal");
	 		
	 		EnviromentUtils util = new EnviromentUtils();
	 		
	 		String fornecedorId = "";
	 		
	 		try {
	 			
	 			String responseString = response[1];
	 			String status = response[0];
	 			
	 			if(status.equalsIgnoreCase("200")){

	 				JsonParser parser = new JsonParser();
	 				JsonArray jsonArray = parser.parse(responseString).getAsJsonArray();
	 				int count = 0;
	 				System.out.println("contagem: " + count);
	 				
	 				for (JsonElement jsonElement : jsonArray) {
	 					System.out.println("contagem2: " + count);
	 					JsonObject jsonObject = jsonElement.getAsJsonObject();

	 					String fornecedorTipo = jsonObject.get("fornecedor_tipo")
	 							.isJsonNull() ? null : jsonObject
	 							.get("fornecedor_tipo").getAsString();

	 					fornecedorId = jsonObject.get("fornecedor_id")
	 							.isJsonNull() ? null : jsonObject.get("fornecedor_id")
	 							.getAsString();

	 					String fornecedorNome = jsonObject.get("fornecedor_nome")
	 							.isJsonNull() ? null : jsonObject
	 							.get("fornecedor_nome").getAsString();

	 					String fornecedorNomeFantasia = jsonObject.get(
	 							"fornecedor_nomefantasia").isJsonNull() ? null
	 							: jsonObject.get("fornecedor_nomefantasia")
	 									.getAsString();
	 					if (fornecedorNomeFantasia == null) {
	 						fornecedorNomeFantasia = fornecedorNome;
	 					}
	 					String fornecedorEndereco = jsonObject.get(
	 							"fornecedor_endereco").isJsonNull() ? null : jsonObject
	 							.get("fornecedor_endereco").getAsString();
	 					
	 					if (fornecedorEndereco == null) {
	 						/*
	 						 * insertLogIntegracao(
	 						 * "Fornecedor com vari�vel Endere�o nula, Endere�o cadastrado como vazio: "
	 						 * , "Aviso", fornecedorNome);
	 						 */
	 					}
	 					
	 					String fornecedorBairro = jsonObject.get("fornecedor_bairro")
	 							.isJsonNull() ? null : jsonObject.get(
	 							"fornecedor_bairro").getAsString();
	 					
	 					if (fornecedorBairro == null) {
	 						/*
	 						 * insertLogIntegracao(
	 						 * "Fornecedor com vari�vel Bairro nulo, Bairro cadastrado como vazio: "
	 						 * , "Aviso", fornecedorNome);
	 						 */
	 					}
	 					String fornecedorCidade = jsonObject.get("fornecedor_cidade")
	 							.isJsonNull() ? null : jsonObject.get(
	 							"fornecedor_cidade").getAsString();
	 					if (fornecedorCidade == null) {
	 						/*
	 						 * insertLogIntegracao(
	 						 * "Fornecedor com vari�vel Cidade nulo, n�o ser� cadastrado: "
	 						 * , "Aviso", fornecedorNome);
	 						 */
	 					}
	 					String fornecedorUf = jsonObject.get("fornecedor_uf")
	 							.isJsonNull() ? null : jsonObject.get("fornecedor_uf")
	 							.getAsString();

	 					String fornecedorCep = jsonObject.get("fornecedor_cep")
	 							.isJsonNull() ? null : jsonObject.get("fornecedor_cep")
	 							.getAsString();

	 					String fornecedorInscMunicipal = jsonObject.get(
	 							"fornecedor_isncmunicipal").isJsonNull() ? null
	 							: jsonObject.get("fornecedor_isncmunicipal")
	 									.getAsString();

	 					String fornecedorInscestadual = jsonObject.get(
	 							"fornecedor_inscestadual").isJsonNull() ? null
	 							: jsonObject.get("fornecedor_inscestadual")
	 									.getAsString();

	 					String fornecedorFone1 = jsonObject.get("fornecedor_fone1")
	 							.isJsonNull() ? null : jsonObject.get(
	 							"fornecedor_fone1").getAsString();

	 					String fornecedorFone2 = jsonObject.get("fornecedor_fone2")
	 							.isJsonNull() ? null : jsonObject.get(
	 							"fornecedor_fone2").getAsString();

	 					String fornecedorFax = jsonObject.get("fornecedor_fax")
	 							.isJsonNull() ? null : jsonObject.get("fornecedor_fax")
	 							.getAsString();

	 					String fornecedorCelular = jsonObject.get("fornecedor_celular")
	 							.isJsonNull() ? null : jsonObject.get(
	 							"fornecedor_celular").getAsString();

	 					String fornecedorContato = jsonObject.get("fornecedor_contato")
	 							.isJsonNull() ? null : jsonObject.get(
	 							"fornecedor_contato").getAsString();

	 					String fornecedorCpfcnpj = jsonObject.get("fornecedor_cpfcnpj")
	 							.isJsonNull() ? null : jsonObject.get(
	 							"fornecedor_cpfcnpj").getAsString();
	 					if (fornecedorCpfcnpj == null) {
	 						/*
	 						 * insertLogIntegracao(
	 						 * "Fornecedor com vari�vel CpfCnpj nulo, n�o ser� cadastrado: "
	 						 * , "Aviso", fornecedorNome);
	 						 */
	 					}
	 					String fornecedorEmail = jsonObject.get("fornecedor_email")
	 							.isJsonNull() ? null : jsonObject.get(
	 							"fornecedor_email").getAsString();

	 					String fornecedorHomepage = jsonObject.get(
	 							"fornecedor_homepage").isJsonNull() ? null : jsonObject
	 							.get("fornecedor_homepage").getAsString();

	 					String fornecedorAtivo = jsonObject.get("fornecedor_ativo")
	 							.isJsonNull() ? null : jsonObject.get(
	 							"fornecedor_ativo").getAsString();
	 					if (fornecedorAtivo == null) {
	 						/*
	 						 * insertLogIntegracao(
	 						 * "Fornecedor com vari�vel Ativo nulo, n�o ser� cadastrado: "
	 						 * , "Aviso", fornecedorNome);
	 						 */
	 					}
	 					String dataAtualizacao = jsonObject.get("data_atualizacao")
	 							.isJsonNull() ? null : jsonObject.get(
	 							"data_atualizacao").getAsString();
	 					if ((fornecedorCpfcnpj != null) && (fornecedorAtivo != null)
	 							&& (fornecedorCidade != null)) {
	 						
	 						System.out.println("Primeiro if de valida��o de dados");
	 						
	 						boolean fornecedor = mapaInfParceiros.get(fornecedorCpfcnpj) == null ? true : false;
	 						
	 						System.out.println("Validacao parceiro: " + fornecedor);
	 						
	 						if (fornecedor) {
	 							
	 							System.out.println("Entrou no cadastro");
	 							
	 							insertFornecedor(
	 									fornecedorTipo, fornecedorId, fornecedorNome,
	 									fornecedorNomeFantasia, fornecedorEndereco,
	 									fornecedorBairro, fornecedorCidade,
	 									fornecedorUf, fornecedorCep,
	 									fornecedorInscMunicipal, fornecedorCpfcnpj,
	 									fornecedorHomepage, fornecedorAtivo,
	 									dataAtualizacao, fornecedorInscestadual,
	 									fornecedorFone1, fornecedorFone2,
	 									fornecedorFax, fornecedorCelular,
	 									fornecedorContato, fornecedorNome,
	 									fornecedorEmail, codEmp);

	 							System.out.println("Fornecedor cadastrado");
	 							
	 						} else {
	 							boolean IdFornecedor = mapaInfIdParceiros.get(fornecedorId + "###" + fornecedorCpfcnpj + "###" + codEmp)
	 									== null ? true : false;
	 							//getIfIdFornecedorExist(fornecedorCpfcnpj, fornecedorId, codEmp);
	 							
	 							System.out.println("Validacao id fornecedor: " + IdFornecedor);
	 							
	 							if (IdFornecedor) {
	 								insertIdForn(fornecedorId, fornecedorCpfcnpj,
	 										codEmp);
	 							}

	 						}
	 						
	 						count++;
	 						
	 					}else{
	 						
	 						selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Fornecedor Nao Sera Cadastrado, Informacoes Nulas ou invalidas', SYSDATE, 'Aviso', "+codEmp+", '"+fornecedorId+"' FROM DUAL");
	 						
	 						/*util.inserirLog("Fornecedor N�o Ser� Cadastrado, Informa��es Nulas ou invalidas", "Aviso"
	 								, fornecedorId, codEmp);*/
	 					}
	 				}
	 			}else{

	 				selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Status de Retorno da API Diferente de Sucesso, Status Retornado: "+status+"', SYSDATE, 'Aviso', "+codEmp+", '"+fornecedorId+"' FROM DUAL");
	 				
	 			}
	 			
	 		} catch (Exception e) {
	 			e.printStackTrace();
	 			try {
	 				
	 				selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro no chamado do endpoint: " + e.getMessage()+"', SYSDATE, 'Erro', "+codEmp+", '"+fornecedorId+"' FROM DUAL");
	 				
	 				/*util.inserirLog(
	 						"Erro no chamado do endpoint: " + e.getMessage(),
	 						"Erro", fornecedorId, codEmp);*/
	 			} catch (Exception e1) {
	 				e1.printStackTrace();
	 			}
	 		}
	 	}

	 	public boolean getIfFornecedorExist(String fornecedorCpfcnpj)
	 			throws Exception {
	 		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	 		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	 		PreparedStatement pstmt = null;
	 		ResultSet rs = null;

	 		int fornecedor = 0;
	 		try {

	 			jdbc.openSession();

	 			String sqlSlt = "SELECT COUNT(0) AS FORNECEDOR FROM TGFPAR WHERE CGC_CPF = ?";

	 			pstmt = jdbc.getPreparedStatement(sqlSlt);
	 			pstmt.setString(1, fornecedorCpfcnpj);
	 			rs = pstmt.executeQuery();
	 			while (rs.next()) {
	 				fornecedor = rs.getInt("FORNECEDOR");
	 			}
	 		} catch (SQLException e) {
	 			e.printStackTrace();
	 		} finally {
	 			if (pstmt != null) {
	 				pstmt.close();
	 			}
	 			if (rs != null) {
	 				rs.close();
	 			}
	 			jdbc.closeSession();
	 		}
	 		if (fornecedor > 0) {
	 			return false;
	 		}
	 		return true;
	 	}

	 	public boolean getIfIdFornecedorExist(String fornecedorCpfcnpj,
	 			String idAcad, BigDecimal codemp) throws Exception {
	 		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	 		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	 		PreparedStatement pstmt = null;
	 		ResultSet rs = null;

	 		int fornecedor = 0;
	 		try {

	 			jdbc.openSession();

	 			String sqlSlt = "SELECT COUNT(0) AS C FROM AD_IDFORNACAD WHERE CODPARC = (SELECT CODPARC FROM TGFPAR WHERE CGC_CPF = '"
	 					+ fornecedorCpfcnpj
	 					+ "') AND IDACADWEB = '"
	 					+ idAcad
	 					+ "' AND CODEMP = " + codemp;

	 			pstmt = jdbc.getPreparedStatement(sqlSlt);
	 			rs = pstmt.executeQuery();
	 			while (rs.next()) {
	 				fornecedor = rs.getInt("C");
	 			}
	 		} catch (SQLException e) {
	 			e.printStackTrace();
	 		} finally {
	 			if (pstmt != null) {
	 				pstmt.close();
	 			}
	 			if (rs != null) {
	 				rs.close();
	 			}
	 			jdbc.closeSession();
	 		}
	 		if (fornecedor > 0) {
	 			return false;
	 		}
	 		return true;
	 	}

	 	public void updateTgfNumParc() throws Exception {
	 		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	 		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	 		PreparedStatement pstmt = null;
	 		try {
	 			jdbc.openSession();

	 			String sqlUpd = "UPDATE TGFNUM SET ULTCOD = ULTCOD + 1  WHERE ARQUIVO = 'TGFPAR'";

	 			pstmt = jdbc.getPreparedStatement(sqlUpd);
	 			pstmt.executeUpdate();
	 		} catch (SQLException e) {
	 			e.printStackTrace();
	 		} finally {
	 			if (pstmt != null) {
	 				pstmt.close();
	 			}
	 			jdbc.closeSession();
	 		}
	 	}

	 	public BigDecimal getMaxNumParc() throws Exception {
	 		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	 		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	 		PreparedStatement pstmt = null;
	 		ResultSet rs = null;
	 		BigDecimal bd = BigDecimal.ZERO;
	 		try {
	 			updateTgfNumParc();

	 			jdbc.openSession();

	 			String sqlUpd = "SELECT MAX (ULTCOD) AS ULTCOD FROM TGFNUM WHERE ARQUIVO = 'TGFPAR'";

	 			pstmt = jdbc.getPreparedStatement(sqlUpd);
	 			rs = pstmt.executeQuery();
	 			while (rs.next()) {
	 				bd = rs.getBigDecimal("ULTCOD");
	 			}
	 		} catch (SQLException e) {
	 			e.printStackTrace();
	 		} finally {
	 			if (pstmt != null) {
	 				pstmt.close();
	 			}
	 			if (rs != null) {
	 				rs.close();
	 			}
	 			jdbc.closeSession();
	 		}
	 		return bd;
	 	}

	 	public BigDecimal insertFornecedor(String fornecedorTipo,
	 			String fornecedorId, String fornecedorNome,
	 			String fornecedorNomeFantasia, String fornecedorEndereco,
	 			String fornecedorBairro, String fornecedorCidade,
	 			String fornecedorUf, String fornecedorCep,
	 			String fornecedorInscMunicipal, String fornecedorCpfcnpj,
	 			String fornecedorHomepage, String fornecedorAtivo,
	 			String dataAtualizacao, String fornecedorInscestadual,
	 			String fornecedorFone1, String fornecedorFone2,
	 			String fornecedorFax, String fornecedorCelular,
	 			String fornecedorContato, String fornecedorNome2,
	 			String fornecedorEmail, BigDecimal codEmp) throws Exception {
	 		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	 		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	 		PreparedStatement pstmt = null;
	 		
	 		EnviromentUtils util = new EnviromentUtils();
	 		
	 		BigDecimal atualCodparc = util.getMaxNumParc();

	 		String tipPessoa = "";
	 		if (fornecedorCpfcnpj.length() == 11) {
	 			tipPessoa = "F";
	 		} else if (fornecedorCpfcnpj.length() == 14) {
	 			tipPessoa = "J";
	 		}
	 		try {
	 			jdbc.openSession();

	 			String sqlP = "INSERT INTO TGFPAR(CODPARC, AD_ID_EXTERNO_FORN, AD_IDENTINSCMUNIC, AD_TIPOFORNECEDOR, FORNECEDOR, IDENTINSCESTAD, HOMEPAGE, ATIVO, NOMEPARC, RAZAOSOCIAL ,TIPPESSOA, AD_ENDCREDOR, CODBAI, CODCID, CEP, CGC_CPF, DTCAD, DTALTER, CODEMP) \t\tVALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NVL((select max(codbai) from tsibai where TRANSLATE( \t\t\t    upper(nomebai), \t\t\t    '������������������������������������', \t\t\t    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC' \t\t\t  ) like TRANSLATE( \t\t\t    upper(?), \t\t\t    '������������������������������������', \t\t\t    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC' \t\t\t  )), 0), NVL((SELECT max(codcid) FROM tsicid WHERE TRANSLATE(              UPPER(descricaocorreio),               '������������������������������������',               'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')               LIKE TRANSLATE(UPPER(?),               '������������������������������������',               'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')               OR SUBSTR(UPPER(descricaocorreio),               1, INSTR(UPPER(descricaocorreio), ' ') - 1)               LIKE TRANSLATE(UPPER(?),               '������������������������������������',               'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')),0),  ?, ?, SYSDATE, SYSDATE, ?)";

	 			pstmt = jdbc.getPreparedStatement(sqlP);
	 			pstmt.setBigDecimal(1, atualCodparc);
	 			pstmt.setString(2, fornecedorId);
	 			pstmt.setString(3, fornecedorInscMunicipal);
	 			pstmt.setString(4, fornecedorTipo);
	 			pstmt.setString(5, "S");
	 			pstmt.setString(6, fornecedorInscestadual);
	 			pstmt.setString(7, fornecedorHomepage);
	 			pstmt.setString(8, fornecedorAtivo);
	 			pstmt.setString(9, fornecedorNome.toUpperCase());
	 			pstmt.setString(10, fornecedorNomeFantasia.toUpperCase());
	 			pstmt.setString(11, tipPessoa);
	 			pstmt.setString(12, fornecedorEndereco);

	 			pstmt.setString(13, fornecedorBairro);

	 			pstmt.setString(14, fornecedorCidade.trim());
	 			pstmt.setString(15, fornecedorCidade.trim());

	 			pstmt.setString(16, fornecedorCep);

	 			pstmt.setString(17, fornecedorCpfcnpj);
	 			pstmt.setBigDecimal(18, codEmp);

	 			pstmt.executeUpdate();
	 			if ((fornecedorFone1 != null) && (fornecedorFone2 != null)
	 					&& (fornecedorFax != null) && (fornecedorCelular != null)
	 					&& (fornecedorContato != null) && (fornecedorNome != null)
	 					&& (fornecedorEmail != null) && (atualCodparc != null)) {
	 				insertContatoFornecedor(fornecedorFone1, fornecedorFone2,
	 						fornecedorFax, fornecedorCelular, fornecedorContato,
	 						fornecedorNome, fornecedorEmail, atualCodparc, fornecedorId,
	 						codEmp);
	 			} else {

	 				selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Uma ou mais variaveis sao nulas, a funcao de cadastrar contato nao sera chamada.', SYSDATE, 'Aviso', "+codEmp+", '"+fornecedorId+"' FROM DUAL");
	 				
	 				/*util.inserirLog(
	 						"Uma ou mais vari�veis s�o nulas, a fun��o de cadastrar contato n�o ser� chamada.",
	 						"Aviso", fornecedorId, codEmp);*/
	 			}

	 			insertIdForn(fornecedorId, fornecedorCpfcnpj, codEmp);

	 		} catch (SQLException e) {
	 			/*util.inserirLog(
	 					"Erro ao cadastrar fornecedor: " + e.getMessage(), "Erro",
	 					fornecedorId, codEmp);*/

	 			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro ao cadastrar fornecedor: " + e.getMessage().replace("'", "\"")+"', SYSDATE, 'Erro', "+codEmp+", '"+fornecedorId+"' FROM DUAL");
	 			
	 			
	 			e.printStackTrace();
	 		} finally {
	 			if (pstmt != null) {
	 				pstmt.close();
	 			}
	 			jdbc.closeSession();
	 		}
	 		return atualCodparc;
	 	}

	 	public void updateIdForn(String idForn, String cgc) throws Exception {
	 		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	 		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	 		PreparedStatement pstmt = null;

	 		try {
	 			jdbc.openSession();

	 			String sqlP = "UPDATE TGFPAR SET AD_ID_EXTERNO_FORN = '"
	 					+ idForn
	 					+ "' WHERE CODPARC = (SELECT CODPARC FROM TGFPAR WHERE CGC_CPF = '"
	 					+ cgc + "')";

	 			pstmt = jdbc.getPreparedStatement(sqlP);

	 			pstmt.executeUpdate();
	 		} catch (SQLException e) {
	 			e.printStackTrace();
	 		} finally {
	 			if (pstmt != null) {
	 				pstmt.close();
	 			}
	 			jdbc.closeSession();
	 		}
	 	}

	 	public void insertIdForn(String idForn, String cgc, BigDecimal codemp)
	 			throws Exception {
	 		
	 		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	 		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	 		PreparedStatement pstmt = null;
	 		
	 		try {

	 			jdbc.openSession();

	 			String sqlP = "INSERT INTO AD_IDFORNACAD (CODPARC, ID, IDACADWEB, CODEMP) VALUES ((SELECT CODPARC FROM TGFPAR WHERE CGC_CPF = '"
	 					+ cgc
	 					+ "'), (SELECT NVL(MAX(ID), 0) + 1 FROM AD_IDFORNACAD), '"
	 					+ idForn + "', " + codemp + ")";

	 			pstmt = jdbc.getPreparedStatement(sqlP);

	 			pstmt.executeUpdate();
	 		} catch (SQLException e) {
	 			e.printStackTrace();

	 			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro ao Cadastrar Id de Fornecedor', SYSDATE, 'Erro', "+codemp+", '"+idForn+"' FROM DUAL");
	 			
	 			//util.inserirLog("Erro ao Cadastrar Id de Fornecedor", "Erro", idForn, codemp);
	 		} finally {
	 			if (pstmt != null) {
	 				pstmt.close();
	 			}

	 			jdbc.closeSession();
	 		}
	 	}

	 	private void insertContatoFornecedor(String fornecedorFone1,
	 			String fornecedorFone2, String fornecedorFax,
	 			String fornecedorCelular, String fornecedorContato,
	 			String fornecedorNome, String fornecedorEmail,
	 			BigDecimal credotAtual, String fornecedorId,
	 			BigDecimal codEmp) throws Exception {
	 		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	 		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	 		PreparedStatement pstmt = null;
	 		try {
	 			jdbc.openSession();

	 			String sqlP = "INSERT INTO TGFCTT (CODCONTATO, CODPARC, NOMECONTATO, CELULAR, TELEFONE, TELRESID, FAX, EMAIL) VALUES ((SELECT MAX(NVL(CODCONTATO, 0)) + 1 FROM TGFCTT WHERE CODPARC = ?), ?, ?, ?, ?, ?, ?, ?)";

	 			pstmt = jdbc.getPreparedStatement(sqlP);
	 			pstmt.setBigDecimal(1, credotAtual);
	 			pstmt.setBigDecimal(2, credotAtual);
	 			pstmt.setString(3, fornecedorNome);
	 			pstmt.setString(4, fornecedorCelular);
	 			pstmt.setString(5, fornecedorFone1);
	 			pstmt.setString(6, fornecedorFone2);
	 			pstmt.setString(7, fornecedorFax);
	 			pstmt.setString(8, fornecedorEmail);
	 			pstmt.executeUpdate();
	 		} catch (SQLException e) {
	 			/*insertLogIntegracao("Erro ao cadastrar contatos do fornecedor: "
	 					+ e.getMessage(), "Erro", fornecedorNome);*/

	 			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro ao cadastrar contatos do fornecedor: "
	 					+ e.getMessage()+"', SYSDATE, 'Erro', "+codEmp+", '"+fornecedorId+"' FROM DUAL");
	 			
	 			e.printStackTrace();
	 		} finally {
	 			if (pstmt != null) {
	 				pstmt.close();
	 			}
	 			jdbc.closeSession();
	 		}
	 	}

	 	public void insertLogIntegracao(String descricao, String status,
	 			String fornecedorNome) throws Exception {
	 		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	 		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	 		PreparedStatement pstmt = null;
	 		try {
	 			jdbc.openSession();

	 			String descricaoCompleta = null;
	 			if (fornecedorNome.equals("")) {
	 				descricaoCompleta = descricao;
	 			} else if (!fornecedorNome.isEmpty()) {
	 				descricaoCompleta = descricao + " " + " Fornecedor:"
	 						+ fornecedorNome;
	 			} else if (!fornecedorNome.isEmpty()) {
	 				descricaoCompleta = descricao + " " + " Fornecedor:"
	 						+ fornecedorNome;
	 			}
	 			String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, STATUS)VALUES (((SELECT NVL(MAX(NUMUNICO), 0) + 1 FROM AD_LOGINTEGRACAO)), ?, SYSDATE, ?)";

	 			pstmt = jdbc.getPreparedStatement(sqlUpdate);
	 			pstmt.setString(1, descricaoCompleta);
	 			pstmt.setString(2, status);
	 			pstmt.executeUpdate();
	 		} catch (Exception se) {
	 			se.printStackTrace();
	 		} finally {
	 			if (pstmt != null) {
	 				pstmt.close();
	 			}
	 			jdbc.closeSession();
	 		}
	 	}

	 	/**
		 * Versão otimizada do método apiGet com melhor tratamento de erros e recursos
		 */
		 public String[] apiGet2(String ur, String token) throws Exception {
			    BufferedReader reader;
			    StringBuilder responseContent = new StringBuilder();
			    String encodedUrl = ur.replace(" ", "%20");
			    URL obj = new URL(encodedUrl);
			    HttpURLConnection https = (HttpURLConnection)obj.openConnection();
			    System.out.println("Entrou na API");
			    System.out.println("URL: " + encodedUrl);
			    System.out.println("Token Enviado: [" + token + "]");
			    https.setRequestMethod("GET");
			    https.setRequestProperty("User-Agent", 
			        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)");
			    https.setRequestProperty("Content-Type", 
			        "application/json; charset=UTF-8");
			    https.setRequestProperty("Accept", "application/json");
			    https.setRequestProperty("Authorization", "Bearer " + token);
			    https.setDoInput(true);
			    int status = https.getResponseCode();
			    if (status >= 300) {
			      reader = new BufferedReader(new InputStreamReader(
			            https.getErrorStream()));
			    } else {
			      reader = new BufferedReader(new InputStreamReader(
			            https.getInputStream()));
			    } 
			    String line;
			    while ((line = reader.readLine()) != null)
			      responseContent.append(line); 
			    reader.close();
			    System.out.println("Output from Server .... \n" + status);
			    String response = responseContent.toString();
			    https.disconnect();
			    return new String[] { Integer.toString(status), response };
			  }
	 	 

	 	private List<Object[]> retornarInformacoesParceiros() throws Exception {
	 		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	 		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	 		PreparedStatement pstmt = null;
	 		ResultSet rs = null;
	 		List<Object[]> listRet = new ArrayList<>();

	 		try {
	 			jdbc.openSession();
	 			String sql = "SELECT p.CODPARC, p.CGC_CPF, "
	 					+ "a.IDACADWEB, a.codemp " + "FROM TGFPAR p "
	 					+ "LEFT join AD_IDFORNACAD a "
	 					+ "on a.codparc = p.codparc " + "where fornecedor = 'S'";

	 			pstmt = jdbc.getPreparedStatement(sql);
	 			rs = pstmt.executeQuery();

	 			while (rs.next()) {
	 				Object[] ret = new Object[4];
	 				ret[0] = rs.getBigDecimal("CODPARC");
	 				ret[1] = rs.getString("CGC_CPF");
	 				ret[2] = rs.getString("IDACADWEB");
	 				ret[3] = rs.getBigDecimal("codemp");

	 				listRet.add(ret);
	 			}

	 		} catch (SQLException e) {
	 			e.printStackTrace();
	 		} finally {
	 			if (rs != null) {
	 				rs.close();
	 			}
	 			if (pstmt != null) {
	 				pstmt.close();
	 			}
	 			jdbc.closeSession();
	 		}

	 		return listRet;
	 	}

	 	public void insertLogList(String listInsert) throws Exception {
	 		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	 		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	 		PreparedStatement pstmt = null;
	 		try {
	 			jdbc.openSession();
	 			
	 			String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, "
	 							 + "	STATUS, CODEMP, MATRICULA_IDFORN) " + listInsert;
	 			
	 			pstmt = jdbc.getPreparedStatement(sqlUpdate);
	 			//pstmt.setString(1, listInsert);
	 			pstmt.executeUpdate();
	 		} catch (Exception se) {
	 			se.printStackTrace();
	 		} finally {
	 			if (pstmt != null) {
	 				pstmt.close();
	 			}
	 			jdbc.closeSession();
	 		}
	 	}
	 }