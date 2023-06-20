package br.com.sankhya.acoestreinamento.jobs;

import java.io.ByteArrayInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Date;

import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;

public class TesteJson implements AcaoRotinaJava{
	
	@Override
	public void doAction(ContextoAcao arg0) throws Exception {
		
		
		
	}

	/*public void main(String[] args) throws Exception {
		
		gerarJsonCliente();
		gerarJsonProdutos();
		gerarJsonFv();
		gerarJsonPagamento();
		gerarJsonEndCliente();
		gerarJsonEstoque();
		gerarJsonFaturamento();
		
	}*/
	
	public static void gerarJsonCliente(){
		
		Date data = new Date();
        SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
        String dataFormatada = formatter.format(data);
		
		  String caminhoArquivo = "C:\\Users\\bmcode\\Desktop\\teste\\101_CLIENTES_"+dataFormatada+".json"; // Especifique o caminho completo do diretório e nome do arquivo

	        JsonArray jsonArray = new JsonArray();
	        
	        //Estrutura Cliente
	        for (int i = 0; i < 5; i++) {
	        	JsonObject item = new JsonObject();
	        	item.addProperty("NOME_FANTASIA_CLIENTE", "null");
	        	item.addProperty("MUNICIPIO_END_MATRIZ_CLIENTE", "CIANORTE");
	        	item.addProperty("CATEGORIA_CLIENTE_MERCADO", "COMERCIO");
	        	item.addProperty("PESSOA_CONTATO", "null");
	        	item.addProperty("STATUS_FINANCEIRO_CLIENTE", "null");
	        	item.addProperty("COMPLEMENTO_END_MATRIZ_CLIENTE", "null");
	        	item.addProperty("FLAG_ATIVO_CLIENTE", "null");
	        	item.addProperty("RAZAO_SOCIAL_CLIENTE", "null");
	        	item.addProperty("FLAG_FINANCEIRO_ATIVO", "null");
	        	item.addProperty("BAIRRO_END_MATRIZ_CLIENTE", "CENTRO");
	        	item.addProperty("EMAIL_END_MATRIZ_CLIENTE", "null");
	        	item.addProperty("CEP_END_MATRIZ_CLIENTE", "87200000");
	        	item.addProperty("UF_END_MATRIZ_CLIENTE", "PR");
	        	item.addProperty("GRUPO_ECONOMICO_CLIENTE", "null");
	        	item.addProperty("TEL_END_MATRIZ_CLIENTE", "null");
	        	item.addProperty("NUMERO_END_MATRIZ_CLIENTE", "null");
	        	item.addProperty("ID_DISTRIBUIDOR", "07480776000103");
	        	item.addProperty("DATA_DESATIVACAO_CLIENTE", "null");
	        	item.addProperty("ID_CLIENTE", 21);
	        	item.addProperty("RAMO_NEGOCIO_CLIENTE", "COMERCIO");
	        	item.addProperty("PAIS_END_MATRIZ_CLIENTE", "BRASIL");
	        	item.addProperty("CNPJ_CPF_CLIENTE", "11111111111111");
	        	item.addProperty("PBU_CLIENTE", "null");
	        	item.addProperty("TIPO_CLIENTE_VENDA", "V");
	        	item.addProperty("COD_MUNICIPIO_IBGE_MATRIZ", 4105508);
	        	item.addProperty("DATA_CADASTRO_CLIENTE", "null");
	        	item.addProperty("RUA_END_MATRIZ_CLIENTE", "null");
	        	jsonArray.add(item);
	        }
	        
	        JsonObject jsonObject = new JsonObject();
	        jsonObject.add("", jsonArray);

	        Gson gson = new GsonBuilder().setPrettyPrinting().create();

	        try (FileWriter fileWriter = new FileWriter(caminhoArquivo)) {
	            gson.toJson(jsonObject, fileWriter);
	            System.out.println("Arquivo JSON Clientes gerado com sucesso!");
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	
		
	}
	
	public static void gerarJsonProdutos(){
		
		Date data = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		String dataFormatada = formatter.format(data);
		
		String caminhoArquivo = "C:\\Users\\bmcode\\Desktop\\teste\\102_PRODUTOS_"+dataFormatada+".json"; // Especifique o caminho completo do diretório e nome do arquivo
		
		JsonArray jsonArray = new JsonArray();
		
		//Estrutura produtos
        for (int i = 0; i < 5; i++) {
            JsonObject item = new JsonObject();
            item.addProperty("MARCA_PRODUTO", "null");
            item.addProperty("TIPO_PNEU", "null");
            item.addProperty("CATEGORIA_PRODUTO", "null");
            item.addProperty("TIPO_PRODUTO", "null");
            item.addProperty("FLAG_ATIVO_PRODUTO", "A");
            item.addProperty("SKU_GOODYEAR", "120309");
            item.addProperty("SKU_PRODUTO", "113417");
            item.addProperty("ID_DISTRIBUIDOR", "08295772000100");
            jsonArray.add(item);
        }
		
		JsonObject jsonObject = new JsonObject();
		jsonObject.add("", jsonArray);
		
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		
		try (FileWriter fileWriter = new FileWriter(caminhoArquivo)) {
			gson.toJson(jsonObject, fileWriter);
			System.out.println("Arquivo JSON Produtos gerado com sucesso!");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	
	public void gerarJsonFv() throws Exception{
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		Date data = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		String dataFormatada = formatter.format(data);
		
		insertMestre();
		
		//String caminhoArquivo = "C:\\Users\\bmcode\\Desktop\\teste\\103_FV_"+dataFormatada+".json"; // Especifique o caminho completo do diretório e nome do arquivo
		
		JsonArray jsonArray = new JsonArray();
		
		//Estrutura FV
        for (int i = 0; i < 5; i++) {
        	JsonObject item = new JsonObject();
        	item.addProperty("ID_FV", 1);
        	item.addProperty("PBU_FV", "null");
        	item.addProperty("TIPO_FV", "null");
        	item.addProperty("TEL_FV", "null");
        	item.addProperty("AREA_ATUACAO_FV", "null");
        	item.addProperty("NOME_FV", "null");
        	item.addProperty("EMAIL_FV", "null");
        	item.addProperty("TIPO_FV_VENDA", "null");
        	item.addProperty("ID_DISTRIBUIDOR", "05979727000186");
        	item.addProperty("REGIAO_FV", "Sul/Sudeste");
        	jsonArray.add(item);
        }
		
		JsonObject jsonObject = new JsonObject();
		jsonObject.add("", jsonArray);
		
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		
		String formattedJson = gson.toJson(jsonObject);
		
		byte[] jsonBytes = formattedJson.toString().getBytes();
		InputStream inputStream = new ByteArrayInputStream(jsonBytes);
		
		try {
			
			jdbc.openSession();
			
			String sqlUpdate = "INSERT INTO AD_TESTEITEMLOG(IDLOG, IDITEM, ARQUIVO, STATUS) "
					+"  VALUES ((SELECT NVL(MAX(IDLOG), 0) FROM AD_TESTELOG), "
					+ "			(SELECT NVL(MAX(IDITEM), 0) + 1 FROM AD_TESTEITEMLOG), "
					+ "			 ?, "
					+ "			 'Arquivo Gerado com Sucesso') ";
			
			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.setBinaryStream(1, inputStream, jsonBytes.length);
			pstmt.executeUpdate();
			
		} catch (Exception se) {
			se.printStackTrace();
		}finally{
			
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
		
		
		
		/*try (FileWriter fileWriter = new FileWriter(caminhoArquivo)) {
			gson.toJson(jsonObject, fileWriter);
			System.out.println("Arquivo JSON FV gerado com sucesso!");
		} catch (IOException e) {
			e.printStackTrace();
		}*/
		
		
	}
	
	public static void gerarJsonPagamento(){
		
		Date data = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		String dataFormatada = formatter.format(data);
		
		String caminhoArquivo = "C:\\Users\\bmcode\\Desktop\\teste\\104_FORMA_PAGAMENTO_"+dataFormatada+".json"; // Especifique o caminho completo do diretório e nome do arquivo
		
		JsonArray jsonArray = new JsonArray();
		
		//Estrutura Forma de Pagamento
        for (int i = 0; i < 5; i++) {
        	JsonObject item = new JsonObject();
        	item.addProperty("DESC_FORMA_PAGAMENTO", "Forma de Pagamento Única");
        	item.addProperty("TIPO_FORMA_PAGAMENTO_VENDA", "B");
        	item.addProperty("ID_FORMA_PAGAMENTO", "1");
        	item.addProperty("FLAG_ATIVO_FORMA_PAGAMENTO", "1");
        	item.addProperty("ID_DISTRIBUIDOR", "05979727000186");
        	jsonArray.add(item);
        }
		
		JsonObject jsonObject = new JsonObject();
		jsonObject.add("", jsonArray);
		
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		
		try (FileWriter fileWriter = new FileWriter(caminhoArquivo)) {
			gson.toJson(jsonObject, fileWriter);
			System.out.println("Arquivo JSON Pagamento gerado com sucesso!");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	
	public static void gerarJsonEndCliente(){
		
		Date data = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		String dataFormatada = formatter.format(data);
		
		String caminhoArquivo = "C:\\Users\\bmcode\\Desktop\\teste\\105_ENDERECO_CLIENTES_"+dataFormatada+".json"; // Especifique o caminho completo do diretório e nome do arquivo
		
		JsonArray jsonArray = new JsonArray();
		
		//Estrutura de Endereço
        for (int i = 0; i < 5; i++) {
        	JsonObject item = new JsonObject();
        	item.addProperty("RUA_END_CLIENTE", "null");
        	item.addProperty("BAIRRO_END_CLIENTE", "CENTRO");
        	item.addProperty("ID_END_CLIENTE", 21);
        	item.addProperty("TEL_END_CLIENTE", "null");
        	item.addProperty("COD_MUNICIPIO_IBGE_CLIENTE", 4105508);
        	item.addProperty("NUMERO_END_CLIENTE", "null");
        	item.addProperty("ID_DISTRIBUIDOR", "07480776000286");
        	item.addProperty("CEP_END_CLIENTE", "87200000");
        	item.addProperty("ID_CLIENTE", 21);
        	item.addProperty("UF_END_CLIENTE", "PR");
        	item.addProperty("MUNICIPIO_END_CLIENTE", "CIANORTE");
        	item.addProperty("COMPLEMENTO_END_CLIENTE", "null");
        	item.addProperty("PAIS_END_CLIENTE", "BRASIL");
        	item.addProperty("EMAIL_END_CLIENTE", "null");
        	jsonArray.add(item);
        }
		
		JsonObject jsonObject = new JsonObject();
		jsonObject.add("", jsonArray);
		
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		
		try (FileWriter fileWriter = new FileWriter(caminhoArquivo)) {
			gson.toJson(jsonObject, fileWriter);
			System.out.println("Arquivo JSON Endereço gerado com sucesso!");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	
	public static void gerarJsonEstoque(){
		
		Date data = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		String dataFormatada = formatter.format(data);
		
		String caminhoArquivo = "C:\\Users\\bmcode\\Desktop\\teste\\203_ESTOQUE_"+dataFormatada+".json"; // Especifique o caminho completo do diretório e nome do arquivo
		
		JsonArray jsonArray = new JsonArray();
		
		//Estrutura de Estoque
        for (int i = 0; i < 5; i++) {
        	JsonObject item = new JsonObject();
        	item.addProperty("VALOR_CUSTO_UNITARIO_ESTOQUE", 616.41);
        	item.addProperty("VALOR_CUSTO_TOTAL_ESTOQUE", 1232.81);
        	item.addProperty("QUANTIDADE_ULTIMA_VENDA", 0);
        	item.addProperty("QUANTIDADE_ULTIMA_COMPRA", "null");
        	item.addProperty("DATA_ULTIMA_COMPRA", "null");
        	item.addProperty("QUANTIDADE_ATUAL_ESTOQUE", 2.0000);
        	item.addProperty("ID_DISTRIBUIDOR", "07480776000286");
        	item.addProperty("DATA_ULTIMA_VENDA", "null");
        	item.addProperty("DATA_PRIMEIRA_COMPRA", "2000-01-01 00:00:00.000");
        	item.addProperty("DATA_REFERENCIA_ESTOQUE", "2016-03-31 00:00:00.000");
        	item.addProperty("SKU_PRODUTO", "102830");
        	item.addProperty("TIPO_ESTOQUE_VENDA", "B");
        	item.addProperty("DATA_PRIMEIRA_VENDA", "null");
        	item.addProperty("MOEDA_ESTOQUE", "REAL");
        	jsonArray.add(item);
        }
		
		JsonObject jsonObject = new JsonObject();
		jsonObject.add("", jsonArray);
		
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		
		try (FileWriter fileWriter = new FileWriter(caminhoArquivo)) {
			gson.toJson(jsonObject, fileWriter);
			System.out.println("Arquivo JSON Estoque gerado com sucesso!");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	
	public static void gerarJsonFaturamento(){
		
		Date data = new Date();
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		String dataFormatada = formatter.format(data);
		
		String caminhoArquivo = "C:\\Users\\bmcode\\Desktop\\teste\\205_FATURAMENTO_"+dataFormatada+".json"; // Especifique o caminho completo do diretório e nome do arquivo
		
		JsonArray jsonArray = new JsonArray();
		
		//Estrutura Faturamento
        for (int i = 0; i < 5; i++) {
        	JsonObject item = new JsonObject();
        	item.addProperty("VALOR_BRUTO_ITEM_NF_FATURA", 3644.83);
        	item.addProperty("MOEDA_FATURAMENTO", "REAL");
        	item.addProperty("VALOR_TOTAL_NF_FATURA", 2121.00);
        	item.addProperty("NUM_NF_FATURA", 115630);
        	item.addProperty("VALOR_PIS_ST_ITEM", 0);
        	item.addProperty("FLAG_ITEM_CANCELADO", "0");
        	item.addProperty("VALOR_COFINS_ITEM", 0.00);
        	item.addProperty("TIPO_FATURAMENTO_VENDA", "V");
        	item.addProperty("NATUREZA_OPERACAO_NF_FATURA", "VENDA MERC - REVENDA");
        	item.addProperty("ID_FV", 1);
        	item.addProperty("QUANTIDADE_CANCELADO", 1.0000);
        	item.addProperty("VALOR_SEGURO_ITEM", 0.00);
        	item.addProperty("ID_FORMA_PAGAMENTO", "1");
        	item.addProperty("SERIE_NF_FATURA", "1");
        	item.addProperty("ITEM_NF_FATURA", 1);
        	item.addProperty("QUANTIDADE_ITEM_NF_FATURA", 1.0000);
        	item.addProperty("ID_END_ENTREGA", 25);
        	item.addProperty("VALOR_IVA", 1219.89);
        	item.addProperty("VALOR_CUSTO_ITEM_NF_FATURA", 2078.92);
        	item.addProperty("FLAG_TIPO_VENDA", "V");
        	item.addProperty("VALOR_LIQUIDO_ITEM_NF_FATURA", 2121.00);
        	item.addProperty("SKU_PRODUTO", "113417");
        	item.addProperty("VALOR_COFINS_ST_ITEM", 0);
        	item.addProperty("ID_END_COBRANCA", 25);
        	item.addProperty("VALOR_DESPESAS_EXTRA_ITEM", 0);
        	item.addProperty("CFOP_NF_FATURA", 5405);
        	item.addProperty("VALOR_TOTAL_ITEM_NF_FATURA", 2121.00);
        	item.addProperty("VALOR_ICMS_ITEM", 0.00);
        	item.addProperty("DATA_NF_FATURA", "2022-11-17 00:00:00.000");
        	item.addProperty("VALOR_ICMS_ST_ITEM", 233.87);
        	item.addProperty("ID_DISTRIBUIDOR", "08295772000100");
        	item.addProperty("VALOR_IPI_ITEM", 0.00);
        	item.addProperty("ID_CLIENTE", 25);
        	item.addProperty("VALOR_DESCONTO_ITEM_NF_FATURA", 1523.83);
        	item.addProperty("VALOR_PIS_ITEM", 0.00);
        	item.addProperty("VALOR_UNITARIO_ITEM_NF_FATURA", 3644.8300000000);
        	item.addProperty("FLAG_FATURAMENTO_TP_ENTREGA", "E");
        	item.addProperty("VALOR_FRETE_NF", 0.00);
        	item.addProperty("OBSERVACAO_NF_FATURA", "null");
        	item.addProperty("DATA_CANCELAMENTO", "null");
        	jsonArray.add(item);
        }
		
		JsonObject jsonObject = new JsonObject();
		jsonObject.add("", jsonArray);
		
		Gson gson = new GsonBuilder().setPrettyPrinting().create();
		
		try (FileWriter fileWriter = new FileWriter(caminhoArquivo)) {
			gson.toJson(jsonObject, fileWriter);
			System.out.println("Arquivo JSON Faturamento gerado com sucesso!");
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}
	
	public void insertMestre() throws Exception{
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		
		jdbc.openSession();
		
		String sqlUpdate = "INSERT INTO AD_TESTELOG(IDLOG, DATA) "
				+"  VALUES ((SELECT NVL(MAX(IDLOG), 0) + 1 FROM AD_TESTELOG), SYSDATE) ";
		
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
