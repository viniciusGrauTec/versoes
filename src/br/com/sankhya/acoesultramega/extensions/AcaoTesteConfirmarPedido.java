package br.com.sankhya.acoesultramega.extensions;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.modelcore.auth.AuthenticationInfo;
import br.com.sankhya.modelcore.comercial.BarramentoRegra;
import br.com.sankhya.modelcore.comercial.CentralFaturamento;
import br.com.sankhya.modelcore.comercial.ConfirmacaoNotaHelper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

public class AcaoTesteConfirmarPedido implements AcaoRotinaJava {

	@Override
	public void doAction(ContextoAcao contexto) throws Exception {
		
		Registro[] linhas = contexto.getLinhas();
		Registro registro = linhas[0];
		
		BigDecimal nunota = (BigDecimal) registro.getCampo("NUNOTA");
		BigDecimal codparc = (BigDecimal) registro.getCampo("CODPARC");
		
		StringBuilder erro = null;
		List<String> list = new ArrayList<>();
		List<String> listG = new ArrayList<>();
		
		String cgc = null;
		String response = null;
		String responseErro = null;
		
		try{
			
			System.out.println("Nunota: " + nunota);
			System.out.println("Parceiro: " + codparc);
			
			if(validarLib(nunota)){
				erro = new StringBuilder();
				
				list = getEventoLib(nunota);
				
				erro.append("Existem Liberações Pendentes Para Este Pedido Com Os Seguintes Eventos: ");
				
				for(int i = 0; i < list.size(); i++){
					String elemento = list.get(i);
					erro.append(elemento+"  ");
				}
				
				contexto.mostraErro(erro.toString());
			}
			
			//cgc = getCgc(codparc);
			cgc = getCgc(BigDecimal.valueOf(18611));
			
			response = api("http://192.168.1.41:9090/api/enterprise/bussinessLicenses/"+cgc);
			
			JsonParser parser = new JsonParser();
	        JsonObject jsonResponse = parser.parse(response).getAsJsonObject();

	        JsonArray licensasArray = jsonResponse.getAsJsonArray("licensas");
			
	        apiCertificados("http://192.168.1.41:9090/api/enterprise/certificateLicenses/" + cgc);
	        
	        if(response.substring(0, 3).equalsIgnoreCase("ERRO")){
	        	erro = new StringBuilder();
	        	responseErro = jsonResponse.get("message").getAsString();
	        	erro.append("Erro na requisição da api: " + responseErro);
	        	contexto.mostraErro("Erro na requisição da api: " + responseErro);
	        }else{
	        	for (int i = 0; i < licensasArray.size(); i++) {
					
		            JsonObject licensa = licensasArray.get(i).getAsJsonObject();
		            
		            //listG = getGrupoProd(nunota);
		            listG = getGrupoProd(BigDecimal.valueOf(909));
		            
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
		            
		        }
	        }
			
			
			/*BarramentoRegra barramentoConfirmacao = BarramentoRegra.build(CentralFaturamento.class, "regrasConfirmacaoSilenciosa.xml", AuthenticationInfo.getCurrent());
			barramentoConfirmacao.setValidarSilencioso(true);
			ConfirmacaoNotaHelper.confirmarNota(nunota, barramentoConfirmacao);*/
			
			System.out.println("Passou da confirmação sem erro");
			
		}catch(Exception e){
			System.out.println("Entrou no Catch");
			e.printStackTrace();
			if(erro != null){
				contexto.mostraErro(erro.toString());
			}else{
				String output = null;
				
				String startMarker = "java.";
				String endMarker = ":";
				
				int startIndex = e.toString().indexOf(startMarker);
				int endIndex = e.toString().indexOf(endMarker)
						+ endMarker.length();
				
				if (startIndex != -1 && endIndex != -1) {
					output = e.toString().substring(0, startIndex)
							+ e.toString().substring(endIndex);
					System.out.println("Arquivo formatado: \n" + output);
					
				}
				
				contexto.mostraErro(output);
			}
		}
		//EnvioNotaSefazHelper nf = new EnvioNotaSefazHelper();
		//nf.gerarLoteNotas(notas);
		
	}
	
	public boolean validarLib(BigDecimal nunota) throws Exception{
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		
		BigDecimal count = BigDecimal.ZERO;
		
		try{
			
			jdbc.openSession();
			
			String query = "SELECT COUNT(0) AS COUNT FROM TSILIB WHERE NUCHAVE = ? AND TABELA = 'TGFCAB'";
			
			pstmt = jdbc.getPreparedStatement(query);
			pstmt.setBigDecimal(1, nunota);
			
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
		
		if(count.compareTo(BigDecimal.ZERO) == 0){
			return false;
		}else{
			return true;
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
	
	public String api(String ur) throws IOException {
	
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
					http.getErrorStream()));
			while ((line = reader.readLine()) != null) {
				responseContent.append(line);
			}
			reader.close();
		} else {
			reader = new BufferedReader(new InputStreamReader(
					http.getInputStream()));
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
		System.out.println("Response: " + response);
		
		http.disconnect();
		
		 // Parse the JSON response
        JsonParser parser = new JsonParser();
        JsonObject jsonResponse = parser.parse(response).getAsJsonObject();

        JsonArray licensasArray = jsonResponse.getAsJsonArray("licensas");

        /*for (int i = 0; i < licensasArray.size(); i++) {
            JsonObject licensa = licensasArray.get(i).getAsJsonObject();

            String id = licensa.get("id").getAsString();
            String tipoProduto = licensa.get("tipoProduto").getAsString();
            boolean ativa = licensa.get("ativa").getAsBoolean();
            
            System.out.println("ID: " + id);
            System.out.println("Tipo de Produto: " + tipoProduto);
            System.out.println("Ativa: " + ativa);
            System.out.println();
        }*/
		
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
		
		if (status >= 300) {
			reader = new BufferedReader(new InputStreamReader(
					http.getErrorStream()));
			while ((line = reader.readLine()) != null) {
				responseContent.append(line);
			}
			reader.close();
		} else {
			reader = new BufferedReader(new InputStreamReader(
					http.getInputStream()));
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
		
		// Parse the JSON response
		JsonParser parser = new JsonParser();
		JsonObject jsonResponse = parser.parse(response).getAsJsonObject();
		
		JsonArray licensasArray = jsonResponse.getAsJsonArray("licensas");
		
		/*for (int i = 0; i < licensasArray.size(); i++) {
            JsonObject licensa = licensasArray.get(i).getAsJsonObject();

            String id = licensa.get("id").getAsString();
            String tipoProduto = licensa.get("tipoProduto").getAsString();
            boolean ativa = licensa.get("ativa").getAsBoolean();
            
            System.out.println("ID: " + id);
            System.out.println("Tipo de Produto: " + tipoProduto);
            System.out.println("Ativa: " + ativa);
            System.out.println();
        }*/
		
		return response;
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

}
