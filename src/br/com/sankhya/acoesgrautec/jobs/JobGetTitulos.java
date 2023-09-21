package br.com.sankhya.acoesgrautec.jobs;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import br.com.sankhya.acoesgrautec.util.LogCatcher;
import br.com.sankhya.acoesgrautec.util.LogConfiguration;
import br.com.sankhya.modelcore.util.SWRepositoryUtils;

public class JobGetTitulos implements ScheduledAction {

	@Override
	public void onTime(ScheduledActionContext arg0) {
		
		LogConfiguration.setPath(SWRepositoryUtils.getBaseFolder()+"/personalizacao/financeirofromdiscount/logs");
		
		try {
			String[] response = apiGet("https://api.acadweb.com.br/"
					+ "{cliente-sigla}/financeiro/titulos?"
					//+ "vencimentoInicial=2023-04-11?"
					//+ "vencimentoFinal=2023-04-24?"
					+ "dataInicial=2023-04-19 06:36:00?"
					+ "dataFinal=2023-04-24 06:36:00?"
					+ "quantidade=500");
			
			String responseString = response[1];
			
			LogCatcher.logInfo("Response: " + responseString);
			JsonParser parser = new JsonParser();
			JsonArray jsonArray = parser.parse(responseString).getAsJsonArray();

	        for (JsonElement jsonElement : jsonArray) {
	            JsonObject jsonObject = jsonElement.getAsJsonObject();

	            LogCatcher.logInfo("Titulo ID: " + jsonObject.get("titulo_id").getAsInt());
	            LogCatcher.logInfo("Titulo Vencimento: " + jsonObject.get("titulo_vencimento").getAsString());
	            LogCatcher.logInfo("Titulo Valor: " + jsonObject.get("titulo_valor").getAsDouble());

	            // Para os benefícios, você pode fazer um loop interno
	            JsonArray beneficiosArray = jsonObject.getAsJsonArray("beneficios");
	            for (JsonElement beneficioElement : beneficiosArray) {
	                JsonObject beneficioObject = beneficioElement.getAsJsonObject();
	                LogCatcher.logInfo("Beneficio ID: " + beneficioObject.get("beneficio_id").getAsString());
	                LogCatcher.logInfo("Beneficio Descrição: " + beneficioObject.get("beneficio_descricao").getAsString());
	                // E assim por diante para os campos dos benefícios
	            }
	        }
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public String[] apiGet(String ur) throws Exception {

		BufferedReader reader;
		String line;
		StringBuilder responseContent = new StringBuilder();
		//String key = preferenciaSenha();
		
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
	
		http.disconnect();
		
		return new String[] {Integer.toString(status), response};

	}

}
