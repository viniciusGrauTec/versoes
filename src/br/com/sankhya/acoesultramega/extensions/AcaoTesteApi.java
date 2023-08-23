package br.com.sankhya.acoesultramega.extensions;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class AcaoTesteApi implements /* AcaoRotinaJava */ScheduledAction{

	//public static void main(String[] args) throws Exception {
		
		public static String api(String ur) throws IOException {
			
			//String ur = "192.168.1.41:9090/api/health-check";
		
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
			System.out.println("Output from Server .... \n" + status);
			String response = responseContent.toString();
			
			System.out.println("Response: " + response);
			
			http.disconnect();
			
			 // Parse the JSON response
            JsonParser parser = new JsonParser();
            JsonObject jsonResponse = parser.parse(response).getAsJsonObject();

            // Access the "licensas" array in the JSON response
            JsonArray licensasArray = jsonResponse.getAsJsonArray("licensas");

            // Iterate over all objects in the "licensas" array
            for (int i = 0; i < licensasArray.size(); i++) {
                // Get the current "licensa" object
                JsonObject licensa = licensasArray.get(i).getAsJsonObject();

                // Access the fields of the "licensa" object
                String id = licensa.get("id").getAsString();
                String tipoProduto = licensa.get("tipoProduto").getAsString();
                boolean ativa = licensa.get("ativa").getAsBoolean();
                
                // Perform any necessary processing with the data
                System.out.println("ID: " + id);
                System.out.println("Tipo de Produto: " + tipoProduto);
                System.out.println("Ativa: " + ativa);
                System.out.println();
            }
			
			return response;

		//}

	}
	
	public void insertPDF(byte[] pdfBytes) throws Exception{
	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	    PreparedStatement pstmt = null;

	    try {
	        jdbc.openSession();

	        String sqlUpdate = "INSERT INTO AD_TESTEPDF(ID, PDF, DATA) "
	                + "VALUES ((SELECT NVL(MAX(ID), 0) + 1 FROM AD_TESTEPDF), "
	                + "        ?,"
	                + "        SYSDATE) ";

	        pstmt = jdbc.getPreparedStatement(sqlUpdate);

	        // Configurar o parâmetro do PreparedStatement como um BLOB
	        pstmt.setBlob(1, new ByteArrayInputStream(pdfBytes));
	        pstmt.executeUpdate();
	        
	        System.out.println("Inseriu o pdf");

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
	
	public void apiPdf(String ur) throws Exception {
	    Date now = new Date();
	    SimpleDateFormat formatter2 = new SimpleDateFormat("MMMM dd, yyyy HH:mm:ss", Locale.ENGLISH);
	    String formattedDateTime = formatter2.format(now);
	    
	    
	    
	    URL url = new URL(ur);
	    HttpURLConnection http = (HttpURLConnection) url.openConnection();
	    http.setConnectTimeout(10000);
	    http.setRequestProperty("Content-Type", "application/pdf");
	    http.setDoInput(true);

	    int status = http.getResponseCode();

	    if (status >= 300) {
	        // Tratar erro da API, se necessário
	        System.out.println("Erro ao obter o PDF da API. Código de resposta: " + status);
	    } else {
	        try (InputStream inputStream = http.getInputStream()) {
	            byte[] pdfBytes = readBytesFromInputStream(inputStream);
	            
	            String arquivoString = "__start_fileinformation__{\"name\":\""+"teste"+".pdf\",\"size\":null,\"type\":\"application/pdf\",\"lastModifiedDate\":\""+ formattedDateTime +"\"}__end_fileinformation__";
				
				byte[] cabecalhoBytes = arquivoString.getBytes(StandardCharsets.UTF_8);
	            
				byte[] mergedBytes = mergeByteArrays(cabecalhoBytes, pdfBytes);
				
	            //insertPDF(mergedBytes, formattedDateTime);
	        }
	    }

	    http.disconnect();
	}

	public void insertPDF(byte[] pdfBytes, BigDecimal codparc, BigDecimal codtipdoc, 
			BigDecimal nuIdent, String dtEmissao, String dtValidade) throws Exception {
	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	    PreparedStatement pstmt = null;

	    try {
	        jdbc.openSession();

	        String sqlUpdate = "INSERT INTO AD_DOCPARC(CODPARC, CODTIPDOC, NUIDENT, "
	        		+ "								DTEMISSAO, DTVALIDADE, LINKARQUIVO) "
	                + "VALUES (?,"
	                + "        ?,"
	                + "        ?,"
	                + "		   ?,"
	                + "  	   ?,"
	                + "		   ?)";

	        pstmt = jdbc.getPreparedStatement(sqlUpdate);

	        // Configurar os parâmetros do PreparedStatement
	        pstmt.setBigDecimal(1, codparc);
	        pstmt.setBigDecimal(2, codtipdoc);
	        pstmt.setBigDecimal(3, nuIdent);
	        pstmt.setString(4, dtEmissao);
	        pstmt.setString(5, dtValidade);
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

	@Override
	public void onTime(ScheduledActionContext arg0) {
		
		try {
			System.out.println("Teste");
			//api("http://192.168.1.41:9090/api/health-check");
			//api("http://192.168.1.41:9090/api/enterprise/bussinessLicenses/10588595001092");
			apiPdf("http://192.168.1.41:9090/api/enterprise/bussinessLicense/download/");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}
