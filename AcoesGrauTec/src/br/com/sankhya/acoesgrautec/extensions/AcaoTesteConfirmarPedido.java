package br.com.sankhya.acoesgrautec.extensions;

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
import com.google.gson.JsonPrimitive;

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

public class AcaoTesteConfirmarPedido {
	
	public static void main(String[] arg0){
		
		JsonObject requestBody = new JsonObject();

    	requestBody.addProperty("fatherUuid", "9d58a4c0-0692-41cc-9f54-a007aa17587f");
    	requestBody.addProperty("nome", "Teste Automação BMC");
    	requestBody.addProperty("descricao", "Teste de integração BMC");
    	requestBody.addProperty("responsavelId", 301);
    	requestBody.addProperty("agencyId", 10101);
    	requestBody.addProperty("inicioPrevisto", "01/09/2023");
    	requestBody.addProperty("terminoPrevisto", "05/09/2023");

        /*JsonArray tagIds = new JsonArray();
        tagIds.add(new JsonPrimitive(0));
        requestBody.add("tagIds", tagIds);*/

        JsonArray dynamicFieldList = new JsonArray();
        //JsonObject dynamicField = new JsonObject();
        
        String sqlDynamicFields = "SELECT LEVEL FROM DUAL CONNECT BY LEVEL <= 3";
        
        int dynamicFieldId = 31130;
        int count = 0;
        while (count < 3) {
            JsonObject dynamicField = new JsonObject();
            dynamicField.addProperty("dynamicFieldId", dynamicFieldId);

            JsonArray dynamicFieldValue = new JsonArray();
            dynamicFieldValue.add(new JsonPrimitive("Valor do Campo " + dynamicFieldId));
            dynamicField.add("value", dynamicFieldValue);

            dynamicFieldList.add(dynamicField);
            
            count++;
            dynamicFieldId += 10000; // Apenas para simulação, ajuste conforme necessário
        }
        
        requestBody.add("dynamicFieldList", dynamicFieldList);

        System.out.println(requestBody.toString());
		
	}
	
	
}
