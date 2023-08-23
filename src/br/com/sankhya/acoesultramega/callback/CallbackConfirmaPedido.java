package br.com.sankhya.acoesultramega.callback;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import br.com.sankhya.acoesultramega.model.Note;
import br.com.sankhya.acoesultramega.model.CentralEvents;
import br.com.sankhya.acoesultramega.services.DBAcessService;
import br.com.sankhya.acoesultramega.services.ProjectControllerService;
import br.com.sankhya.acoesultramega.util.LogCatcher;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.modelcore.MGEModelException;
import br.com.sankhya.modelcore.auth.AuthenticationInfo;
import br.com.sankhya.modelcore.comercial.BarramentoRegra;
import br.com.sankhya.modelcore.comercial.CentralFaturamento;
import br.com.sankhya.modelcore.comercial.ConfirmacaoNotaHelper;
import br.com.sankhya.modelcore.custommodule.ICustomCallBack;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.util.troubleshooting.SKError;
import br.com.sankhya.util.troubleshooting.TSLevel;

public class CallbackConfirmaPedido implements ICustomCallBack{

	private static DBAcessService dbacess;

	static {
		try {
			dbacess = ProjectControllerService.getDBAcessService();
		} catch (Exception e) {
			
			e.printStackTrace();
			
			//LogCatcher.logError(e);
		}
	}

	@Override
	public Object call(final String event, final Map<String, Object> fields) {
		JapeSession.SessionHandle hnd = null;
		
		StringBuilder erro = null;
		List<String> list = new ArrayList<>();
		List<String> listG = new ArrayList<>();
		
		String response = null;
		String response2 = null;
		String responseErro = null;
		
		try {
				if (CentralEvents.EVENTO_BEFORE_CENTRAL.getDescription().equals(event) || CentralEvents.EVENTO_BEFORE_PORTAL.getDescription().equals(event)) {
					//LogCatcher.logInfo(fields.get("nunota").toString());
					
					System.out.println("Entrou no if");
					
					System.out.println("Print do Fields: " + fields);
					
					Long nunota = Long.parseLong(fields.get("nunota").toString());
					
					System.out.println("Teste Nunota: " + nunota);
					
					DynamicVO note = dbacess.findNote(nunota);
					
					System.out.println("Passou do VO: " + note);
					
					BigDecimal codparc = note.asBigDecimal("CODPARC");
					
					System.out.println("codparc: " + codparc);
					
					String cgc_cpf = getCgc(codparc);
					
					System.out.println("Nunota: " + nunota);
					System.out.println("Parceiro: " + codparc);
					
					//codparc = BigDecimal.valueOf(59);
					
					if(validarPerfil(codparc).equalsIgnoreCase("COMÉRCIO ATACADISTA") || validarPerfil(codparc).equalsIgnoreCase("INDÚSTRIA")){
						
						//cgc = getCgc(BigDecimal.valueOf(18611));
						
						response = apiLicenssas("http://192.168.1.41:9090/api/enterprise/bussinessLicenses/"+cgc_cpf);
						
						JsonParser parser = new JsonParser();
				        JsonObject jsonResponse;

				        JsonArray licensasArray;
						
				        if(response.substring(0, 3).equalsIgnoreCase("ERRO")){
				        	erro = new StringBuilder();
				        	jsonResponse = parser.parse(response).getAsJsonObject();
				        	responseErro = jsonResponse.get("message").getAsString();
				        	erro.append("Erro na requisição da api: " + responseErro);
				        	throw new Exception("Erro na requisição da api: " + responseErro);
				        }else{
				        	
				        	jsonResponse = parser.parse(response).getAsJsonObject();
				        	licensasArray = jsonResponse.getAsJsonArray("licensas");
				        	
				        	for (int i = 0; i < licensasArray.size(); i++) {
								
					            JsonObject licensa = licensasArray.get(i).getAsJsonObject();
					            
					            listG = getGrupoProd(new BigDecimal(nunota));
					            //listG = getGrupoProd(BigDecimal.valueOf(909));

					            String id = licensa.get("id").getAsString();
					            String tipoProduto = licensa.get("tipoProduto").getAsString();
					            boolean ativa = licensa.get("ativa").getAsBoolean();
					            
					            /*for(int j = 0; j < listG.size(); j++){
					            	String elemento = listG.get(j);
					            	
					            	if(elemento.equalsIgnoreCase(tipoProduto)){
					            		System.out.println("Tipo Produto e Grupo de Produto iguais");
					            		if(ativa == true){
					            			System.out.println("Licensa ativa");
					            		}else{
					            			erro = new StringBuilder();
					            			System.out.println("Licensa desativada");
					            			erro.append("Erro de Licensa Inativa: \nLicensa: " + id + "\nTipo da Licensa: " + tipoProduto + "\nAtiva: " + ativa);
					            			System.out.println(erro);
					        	        	contexto.mostraErro("Erro de Licensa Inativa");
					            		}
					            	}
					            }*/
					            
					            System.out.println("ID: " + id);
					            System.out.println("Tipo de Produto: " + tipoProduto);
					            System.out.println("Ativa: " + ativa + "\n");
					            
					            apiPdfLicenssas("http://192.168.1.41:9090/api/enterprise/bussinessLicense/download/" + id, "Licensa"+tipoProduto, codparc, id);
					        }
				        }
				        
				        response2 = apiCertificados("http://192.168.1.41:9090/api/enterprise/certificateLicenses/" + cgc_cpf);// + cgc);
				        
				        JsonObject jsonResponse2 = parser.parse(response2).getAsJsonObject();

				        JsonArray licensasArray2 = jsonResponse2.getAsJsonArray("certificados");
				        
				        if(response2.substring(0, 4).equalsIgnoreCase("ERRO")){
				        	erro = new StringBuilder();
				        	responseErro = jsonResponse2.get("message").getAsString();
				        	erro.append("Erro na requisição da api: " + responseErro);
				        	throw new Exception("Erro na requisição da api: " + responseErro);
				        }else{
				        					  //licensasArray2.size()
				        	for(int i = 0; i < 1; i++){
				        		
				        		JsonObject certificado = licensasArray2.get(i).getAsJsonObject();
				        		
				        		String id = null;
				        		String tipoCertificado = null;
				        		String status = null;
				        		String dataValidade = null;
				        		
				        		id = certificado.get("id").getAsString();
				        		status = certificado.get("status").getAsString();
				        		tipoCertificado = certificado.get("tipoCertificado").getAsString();
				        		dataValidade = certificado.get("datavalidade").getAsString();
				        		
				        		SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
				                Date date = inputDateFormat.parse(dataValidade);

				                SimpleDateFormat outputDateFormat = new SimpleDateFormat("dd/MM/yyyy");
				                String outputDateString = outputDateFormat.format(date);
				                
				        		System.out.println("id: " + id);
				        		System.out.println("status: " + status);
				        		System.out.println("Tipo de Certificado: " + tipoCertificado);
				        		
				        		apiPdfCertificado("http://192.168.1.41:9090/api/enterprise/certificateLicenses/download/"+id, tipoCertificado, codparc, id, outputDateString);
				        	}
				        }
				    }
					
					if(validarPerfil(codparc).equalsIgnoreCase("INDÚSTRIA")){
						
						response2 = apiCertificados("http://192.168.1.41:9090/api/enterprise/certificateLicenses/" + cgc_cpf);// + cgc);
				        
						JsonParser parser = new JsonParser();
						JsonObject jsonResponse2;

				        JsonArray licensasArray2;
						
				        if(response2.substring(0, 4).equalsIgnoreCase("ERRO")){
				        	erro = new StringBuilder();
				        	jsonResponse2 = parser.parse(response2).getAsJsonObject();
				        	responseErro = jsonResponse2.get("message").getAsString();
				        	erro.append("Erro na requisição da api: " + responseErro);
				        	throw new Exception("Erro na requisição da api: " + responseErro);
				        }else{
					        
					        jsonResponse2 = parser.parse(response2).getAsJsonObject();

					        licensasArray2 = jsonResponse2.getAsJsonArray("certificados");
				        					  //licensasArray2.size()
				        	for(int i = 0; i < 1; i++){
				        		
				        		JsonObject certificado = licensasArray2.get(i).getAsJsonObject();
				        		
				        		String id = null;
				        		String tipoCertificado = null;
				        		String status = null;
				        		String dataValidade = null;
				        		
				        		id = certificado.get("id").getAsString();
				        		status = certificado.get("status").getAsString();
				        		tipoCertificado = certificado.get("tipoCertificado").getAsString();
				        		dataValidade = certificado.get("datavalidade").getAsString();
				        		
				        		SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
				                Date date = inputDateFormat.parse(dataValidade);

				                SimpleDateFormat outputDateFormat = new SimpleDateFormat("dd/MM/yyyy");
				                String outputDateString = outputDateFormat.format(date);

				        		System.out.println("id: " + id);
				        		System.out.println("status: " + status);
				        		System.out.println("Tipo de Certificado: " + tipoCertificado);
				        		System.out.println("dataValidade: " + dataValidade);
				        		System.out.println("dataValidade Formatada: " + outputDateString);
				        		
				        		apiPdfCertificado("http://192.168.1.41:9090/api/enterprise/certificateLicenses/download/"+id, tipoCertificado, codparc, id, outputDateString);
				        	}
				        }
						
					}
					//1267501420036
					validarProduto(new BigDecimal(nunota));
					
				}
					
		} catch (Exception e) {
			System.out.println(erro);
			e.printStackTrace();
		} finally {
			JapeSession.close(hnd);
		}

		return null;
	}
	
	public String getCgc(BigDecimal codparc) throws Exception {
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		String cgc = null;

		jdbc.openSession();

		String sqlSenha = "SELECT CGC_CPF FROM TGFPAR WHERE CODPARC = ?";
		
		pstmt = jdbc.getPreparedStatement(sqlSenha);
		pstmt.setBigDecimal(1, codparc);
		rs = pstmt.executeQuery();
		
		while (rs.next()) {

			cgc = rs.getString("CGC_CPF");

		}

		System.out.println("Teste cgc: "+cgc);

		jdbc.closeSession();

		return cgc;

		
	}
	
	public String validarPerfil(BigDecimal codparc) throws Exception{
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		String perfil = null;
		
		try{
			
			jdbc.openSession();
			
			String query = "SELECT DESCRPAI FROM (SELECT P.CODPARC, P.CODTIPPARC, "
					+ "TP.DESCRTIPPARC, TP.CODTIPPARCPAI, (SELECT DESCRTIPPARC "
					+ "FROM TGFTPP WHERE CODTIPPARC = TP.CODTIPPARCPAI) "
					+ "AS DESCRPAI FROM TGFPAR P INNER JOIN TGFTPP TP ON "
					+ "TP.CODTIPPARC = P.CODTIPPARC) WHERE CODPARC = ?";
			
			pstmt = jdbc.getPreparedStatement(query);
			pstmt.setBigDecimal(1, codparc);
			
			rs = pstmt.executeQuery();
			
			while(rs.next()){
				
				perfil = rs.getString("DESCRPAI");
				
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(pstmt != null){
				pstmt.close();
			}
			jdbc.closeSession();
		}
		
		return perfil;
	}
	
	public void validarProduto(BigDecimal nunota) throws Exception{
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		String codanvisa = null;
		String response = null;
		String situacao = null;
		String dtVenc = null;
		String cnpj = null;
		String nuAut = null;
		
		BigDecimal codprod = null;
		
		JsonParser parser = new JsonParser();
		JsonObject jsonResponse;
		JsonObject jsonEmpresa;
		
		try{
			
			jdbc.openSession();
			
			String query = "SELECT I.CODPROD, "
					+ "(SELECT CODANVISA FROM TGFPRO WHERE CODPROD = I.CODPROD) "
					+ "AS CODANVISA FROM TGFITE I WHERE NUNOTA = ?";
			
			pstmt = jdbc.getPreparedStatement(query);
			pstmt.setBigDecimal(1, nunota);
			
			rs = pstmt.executeQuery();
			
			while(rs.next()){
				
				codanvisa = rs.getString("CODANVISA");
				codprod = rs.getBigDecimal("CODPROD");
				
				response = apiProdutos("http://192.168.1.41:9090/api/medicine/details/"+codanvisa);
				
				jsonResponse = parser.parse(response).getAsJsonObject();
				
				situacao = jsonResponse.get("situacao").getAsString();
				dtVenc = jsonResponse.get("dataVencimentoRegistro").getAsString();
				
				SimpleDateFormat inputDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
                Date date = inputDateFormat.parse(dtVenc);

                SimpleDateFormat outputDateFormat = new SimpleDateFormat("dd/MM/yyyy");
                String outputDateString = outputDateFormat.format(date);
                
                System.out.println("Entrou na validação de produtos");
                
                if(situacao.trim().equalsIgnoreCase("Válido")){
                	situacao = "Ativo";
                }
                
                updateProduto(outputDateString, situacao, codprod);
                
                jsonEmpresa = jsonResponse.get("empresa").getAsJsonObject();
                
                cnpj = jsonEmpresa.get("cnpj").getAsString();
                nuAut = jsonEmpresa.get("numeroAutorizacao").getAsString();
                
                updateParceiro(nuAut, cnpj);
				
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(pstmt != null){
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}
	
	public List<String> getEventoLib(BigDecimal nunota) throws Exception{
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		List<String> evento = new ArrayList<>();
		
		try{
			
			jdbc.openSession();
			
			String query = "SELECT EVENTO FROM TSILIB WHERE NUCHAVE = ? AND TABELA = 'TGFCAB'";
			
			pstmt = jdbc.getPreparedStatement(query);
			pstmt.setBigDecimal(1, nunota);
			
			rs = pstmt.executeQuery();
			
			while(rs.next()){
				
				evento.add(rs.getString("EVENTO"));
				
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(pstmt != null){
				pstmt.close();
			}
			jdbc.closeSession();
		}
		
		return evento;
	}
	
	public boolean validarCadastro(BigDecimal codparc, String nuIdent) throws Exception{
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		BigDecimal count = BigDecimal.ZERO;
		
		try{
			
			jdbc.openSession();
			
			String query = "SELECT COUNT(0) AS COUNT FROM AD_DOCPARC WHERE CODPARC = ? AND NUIDENT = ?";
			
			pstmt = jdbc.getPreparedStatement(query);
			pstmt.setBigDecimal(1, codparc);
			pstmt.setString(2, nuIdent);
			
			rs = pstmt.executeQuery();
			
			while(rs.next()){
				
				count = rs.getBigDecimal("COUNT");
				
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(pstmt != null){
				pstmt.close();
			}
			jdbc.closeSession();
		}
		
		if(count.compareTo(BigDecimal.ZERO) != 0){
			return true;
		}else{
			return false;
		}
	}
	
	public List<String> getGrupoProd(BigDecimal nunota) throws Exception{
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		List<String> grupo = new ArrayList<>();
		
		try{
			
			jdbc.openSession();
			
			String query = "SELECT (SELECT DESCRGRUPOPROD FROM TGFGRU GRU WHERE GRU.CODGRUPOPROD = T.GRUPO) GRUPOPAI FROM (select "
						+"	(SELECT G.CODGRUPAI FROM TGFGRU G INNER JOIN TGFPRO P ON P.CODGRUPOPROD = G.CODGRUPOPROD WHERE CODPROD = I.CODPROD) grupo "
						+"	from tgfite i  "
						+"	where nunota = ?) T";
			
			pstmt = jdbc.getPreparedStatement(query);
			pstmt.setBigDecimal(1, nunota);
			
			rs = pstmt.executeQuery();
			
			while(rs.next()){
				
				grupo.add(rs.getString("GRUPOPAI"));
				
			}
			
		}catch(Exception e){
			e.printStackTrace();
		}finally{
			if(rs != null){
				rs.close();
			}
			if(pstmt != null){
				pstmt.close();
			}
			jdbc.closeSession();
		}
		
		return grupo;
	}
	
	public String apiLicenssas(String ur) throws IOException {
	
		BufferedReader reader;
		String line;
		StringBuilder responseContent = new StringBuilder();
		
		URL url = new URL(ur);
		
		HttpURLConnection http = (HttpURLConnection) url.openConnection();
		http.setConnectTimeout(10000);
		http.addRequestProperty("User-Agent",
				"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)");
		http.setRequestProperty("Content-Type", "application/json");
		http.setDoOutput(true);
		http.setDoInput(true);

		int status = http.getResponseCode();

		if (status >= 300) {
			reader = new BufferedReader(new InputStreamReader(
					http.getErrorStream(), StandardCharsets.UTF_8));
			while ((line = reader.readLine()) != null) {
				responseContent.append(line);
			}
			reader.close();
		} else {
			reader = new BufferedReader(new InputStreamReader(
					http.getInputStream(), StandardCharsets.UTF_8));
			while ((line = reader.readLine()) != null) {
				responseContent.append(line);
			}
			reader.close();
		}
		String response = null;
		System.out.println("Output from Server .... \n" + status);
		if(status != 200){
			response = "ERRO " + responseContent.toString();
		}else{
			response = responseContent.toString();
		}
		System.out.println("Response Licenssas: " + response);
		
		http.disconnect();
		
		return response;
	}
	
	public String apiProdutos(String ur) throws IOException {
		
		BufferedReader reader;
		String line;
		StringBuilder responseContent = new StringBuilder();
		
		URL url = new URL(ur);
		
		HttpURLConnection http = (HttpURLConnection) url.openConnection();
		http.setConnectTimeout(10000);
		http.addRequestProperty("User-Agent",
				"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)");
		http.setRequestProperty("Content-Type", "application/json");
		http.setDoOutput(true);
		http.setDoInput(true);
		
		int status = http.getResponseCode();
		
		if (status >= 300) {
			reader = new BufferedReader(new InputStreamReader(
					http.getErrorStream(), StandardCharsets.UTF_8));
			while ((line = reader.readLine()) != null) {
				responseContent.append(line);
			}
			reader.close();
		} else {
			reader = new BufferedReader(new InputStreamReader(
					http.getInputStream(), StandardCharsets.UTF_8));
			while ((line = reader.readLine()) != null) {
				responseContent.append(line);
			}
			reader.close();
		}
		String response = null;
		System.out.println("Output from Server .... \n" + status);
		if(status != 200){
			response = "ERRO " + responseContent.toString();
		}else{
			response = responseContent.toString();
		}
		System.out.println("Response Produto: " + response);
		
		http.disconnect();
		
		return response;
	}
	
	public String apiCertificados(String ur) throws IOException {
		
		BufferedReader reader;
		String line;
		StringBuilder responseContent = new StringBuilder();
		
		URL url = new URL(ur);
		
		HttpURLConnection http = (HttpURLConnection) url.openConnection();
		http.setConnectTimeout(10000);
		http.addRequestProperty("User-Agent",
				"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)");
		http.setRequestProperty("Content-Type", "application/json");
		http.setDoOutput(true);
		http.setDoInput(true);
		
		int status = http.getResponseCode();
		
		String contentType = http.getHeaderField("Content-Type");
		if (contentType != null) {
		    int charsetIndex = contentType.indexOf("charset=");
		    if (charsetIndex != -1) {
		        String encoding = contentType.substring(charsetIndex + 8);
		        System.out.println("Codificação da resposta: " + encoding);
		    } else {
		        System.out.println("Codificação da resposta não encontrada no cabeçalho Content-Type.");
		    }
		} else {
		    System.out.println("Cabeçalho Content-Type não encontrado na resposta.");
		}
		
		if (status >= 300) {
			reader = new BufferedReader(new InputStreamReader(
					http.getErrorStream(), StandardCharsets.UTF_8));
			while ((line = reader.readLine()) != null) {
				responseContent.append(line);
			}
			reader.close();
		} else {
			reader = new BufferedReader(new InputStreamReader(
					http.getInputStream(), StandardCharsets.UTF_8));
			while ((line = reader.readLine()) != null) {
				responseContent.append(line);
			}
			reader.close();
		}
		String response = null;
		System.out.println("Output from Server .... \n" + status);
		if(status != 200){
			response = "ERRO " + responseContent.toString();
		}else{
			response = responseContent.toString();
		}
		System.out.println("Response Certificados: " + response);
		
		http.disconnect();
		
		return response;
	}
	
	public void apiPdfCertificado(String ur, String nomeArquivo, BigDecimal codparc, String id, String validade) throws Exception {
	    Date now = new Date();
	    SimpleDateFormat formatter2 = new SimpleDateFormat("MMMM dd, yyyy HH:mm:ss", Locale.ENGLISH);
	    String formattedDateTime = formatter2.format(now);
	    
	    URL url = new URL(ur);
	    HttpURLConnection http = (HttpURLConnection) url.openConnection();
	    http.setConnectTimeout(10000);
	    http.setRequestProperty("Content-Type", "application/pdf");
	    http.setDoInput(true);

	    int status = http.getResponseCode();

	    if (status != 200) {
	        // Tratar erro da API, se necessário
	        System.out.println("Erro ao obter o PDF da API. Código de resposta: " + status);
	    } else {
	        try (InputStream inputStream = http.getInputStream()) {
	            byte[] pdfBytes = readBytesFromInputStream(inputStream);
	            
	            System.out.println("Teste nome: " + nomeArquivo);
	            
	            String arquivoString = "__start_fileinformation__{\"name\":\""+nomeArquivo+".pdf\",\"size\":null,\"type\":\"application/pdf\",\"lastModifiedDate\":\""+ formattedDateTime +"\"}__end_fileinformation__";
				
				byte[] cabecalhoBytes = arquivoString.getBytes("ISO-8859-1");
	            
				byte[] mergedBytes = mergeByteArrays(cabecalhoBytes, pdfBytes);
				
				System.out.println("Teste Byts: " + mergedBytes);
				System.out.println("Id: " + id);
				System.out.println("codparc: " + codparc);
				
				if(validarCadastro(codparc, id)){
					System.out.println("Entrou no update dos certificados");
					updateDocumento(mergedBytes, id, codparc);
				}else{
					System.out.println("Entrou no insert dos certificados");
					insertPDFCertificado(mergedBytes, codparc, new BigDecimal("6"), id, null, validade);
				}
				
	        }
	    }

	    http.disconnect();
	}
	
	public void apiPdfLicenssas(String ur, String nomeArquivo, BigDecimal codparc, String id) throws Exception {
		Date now = new Date();
		SimpleDateFormat formatter2 = new SimpleDateFormat("MMMM dd, yyyy HH:mm:ss", Locale.ENGLISH);
		String formattedDateTime = formatter2.format(now);
		
		URL url = new URL(ur);
		HttpURLConnection http = (HttpURLConnection) url.openConnection();
		http.setConnectTimeout(10000);
		http.setRequestProperty("Content-Type", "application/pdf");
		http.setDoInput(true);
		
		int status = http.getResponseCode();
		
		if (status != 200) {
			// Tratar erro da API, se necessário
			System.out.println("Erro ao obter o PDF da API. Código de resposta: " + status);
		} else {
			try (InputStream inputStream = http.getInputStream()) {
				byte[] pdfBytes = readBytesFromInputStream(inputStream);
				
				String arquivoString = "__start_fileinformation__{\"name\":\""+nomeArquivo+".pdf\",\"size\":null,\"type\":\"application/pdf\",\"lastModifiedDate\":\""+ formattedDateTime +"\"}__end_fileinformation__";
				
				byte[] cabecalhoBytes = arquivoString.getBytes(StandardCharsets.UTF_8);
				
				byte[] mergedBytes = mergeByteArrays(cabecalhoBytes, pdfBytes);
				
				System.out.println("Teste Byts: " + mergedBytes);
				
				if(validarCadastro(codparc, id)){
					System.out.println("Entrou no update das licenssas");
					updateDocumento(mergedBytes, id, codparc);
				}else{
					System.out.println("Entrou no insert das licenssas");
					insertPDFLicenssas(mergedBytes, codparc, new BigDecimal("1"), id, null, null);
				}
			}
		}
		
		http.disconnect();
	}

	public void insertPDFCertificado(byte[] pdfBytes, BigDecimal codparc, BigDecimal codtipdoc, 
			String nuIdent, String dtEmissao, String dtValidade) throws Exception {
	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	    PreparedStatement pstmt = null;

	    try {
	        jdbc.openSession();

	        String sqlUpdate = "INSERT INTO AD_DOCPARC(CODPARC, CODTIPDOC, NUIDENT, "
	        		+ "								DTEMISSAO, DTVALIDADE, LINKARQUIVO) "
	                + "VALUES ((SELECT NVL(MAX(SEQUENCIA), 0) + 1 FROM AD_DOCPARC WHERE CODPARC = ?),"
	                + "        ?,"
	                + "        ?,"
	                + "        ?,"
	                + "		   ?,"
	                + "  	   SYSDATE,"
	                + "		   ?)";

	        pstmt = jdbc.getPreparedStatement(sqlUpdate);

	        // Configurar os parâmetros do PreparedStatement
	        pstmt.setBigDecimal(1, codparc);
	        pstmt.setBigDecimal(2, codparc);
	        pstmt.setBigDecimal(3, codtipdoc);
	        pstmt.setString(4, nuIdent);
	        pstmt.setString(5, dtEmissao);
	        //pstmt.setString(5, dtValidade);
	        pstmt.setBytes(6, pdfBytes);
	        pstmt.executeUpdate();

	        System.out.println("O PDF foi inserido no banco de dados com sucesso.");
	    } catch (Exception se) {
	        se.printStackTrace();
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
	}
	
	public void updateProduto(String validade, String status, BigDecimal codprod) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		try {
			
			System.out.println("Entrou no update de produtos");
			
			jdbc.openSession();
			
			String sqlUpdate = "UPDATE TGFPRO SET AD_VALIDADE = ?, AD_STATUS = ? WHERE CODPROD = ?";
			
			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.setString(1, validade);
			pstmt.setString(2, status);
			pstmt.setBigDecimal(3, codprod);
			pstmt.executeUpdate();
			
		} catch (Exception se) {
			se.printStackTrace();
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
	}
	
	public void updateDocumento(byte[] pdfBytes, String nuIdent, BigDecimal codparc) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		try {
			jdbc.openSession();
			
			String sqlUpdate = "UPDATE AD_DOCPARC SET LINKARQUIVO = ? WHERE NUIDENT = ? AND CODPARC = ?";
			
			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.setBytes(1, pdfBytes);
			pstmt.setString(2, nuIdent);
			pstmt.setBigDecimal(3, codparc);
			pstmt.executeUpdate();
			
			System.out.println("Update de Documento Executado");
			
		} catch (Exception se) {
			se.printStackTrace();
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
	}
	
	public void updateParceiro(String nuAutorizacao, String cgc_cpf) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		try {
			jdbc.openSession();
			
			String sqlUpdate = "UPDATE TGFPAR SET AD_NUAUTH = ? WHERE CGC_CPF = ?";
			
			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.setString(1, nuAutorizacao);
			pstmt.setString(2, cgc_cpf);
			pstmt.executeUpdate();
			
		} catch (Exception se) {
			se.printStackTrace();
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
	}
	
	public void insertPDFLicenssas(byte[] pdfBytes, BigDecimal codparc, BigDecimal codtipdoc, 
			String nuIdent, String dtEmissao, String dtValidade) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		try {
			jdbc.openSession();
			
			String sqlUpdate = "INSERT INTO AD_DOCPARC(SEQUENCIA, CODPARC, CODTIPDOC, NUIDENT, "
					+ "								DTEMISSAO, DTVALIDADE, LINKARQUIVO) "
					+ "VALUES ((SELECT NVL(MAX(SEQUENCIA), 0) + 1 FROM AD_DOCPARC WHERE CODPARC = ?), "
					+ "        ?,"
					+ "        ?,"
					+ "        ?,"
					+ "		   ?,"
					+ "  	   ?,"
					+ "		   ?)";
			
			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			
			// Configurar os parâmetros do PreparedStatement
			pstmt.setBigDecimal(1, codparc);
			pstmt.setBigDecimal(2, codparc);
			pstmt.setBigDecimal(3, codtipdoc);
			pstmt.setString(4, nuIdent);
			pstmt.setString(5, dtEmissao);
			pstmt.setString(6, dtValidade);
			pstmt.setBytes(7, pdfBytes);
			pstmt.executeUpdate();
			
			System.out.println("O PDF foi inserido no banco de dados com sucesso.");
		} catch (Exception se) {
			se.printStackTrace();
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
	}

	private byte[] readBytesFromInputStream(InputStream inputStream) throws IOException {
	    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
	    int nRead;
	    byte[] data = new byte[1024];

	    while ((nRead = inputStream.read(data, 0, data.length)) != -1) {
	        buffer.write(data, 0, nRead);
	    }

	    buffer.flush();
	    return buffer.toByteArray();
	}
	
	public byte[] mergeByteArrays(byte[] array1, byte[] array2) {
	    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

	    try {
	        outputStream.write(array1);
	        outputStream.write(array2);
	    } catch (IOException e) {
	        e.printStackTrace();
	    }

	    return outputStream.toByteArray();
	}

}
