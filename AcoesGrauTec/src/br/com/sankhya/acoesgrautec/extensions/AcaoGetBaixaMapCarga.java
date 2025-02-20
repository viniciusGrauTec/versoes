package br.com.sankhya.acoesgrautec.extensions;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.activiti.engine.impl.util.json.JSONArray;
import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

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

public class AcaoGetBaixaMapCarga implements AcaoRotinaJava, ScheduledAction {
	
	private List<String> selectsParaInsert = new ArrayList<String>();
	private EnviromentUtils util = new EnviromentUtils();
	
	@Override
	public void doAction(ContextoAcao contexto) throws Exception {

		Registro[] linhas = contexto.getLinhas();
		Registro registro = linhas[0];

		String url = (String) registro.getCampo("URL");
		String token = (String) registro.getCampo("TOKEN");
		BigDecimal codEmp = (BigDecimal) registro.getCampo("CODEMP");

		String dataInicio = contexto.getParam("DTINICIO").toString()
				.substring(0, 10);
		String dataFim = contexto.getParam("DTFIM").toString().substring(0, 10);
		
		String matricula = (String) contexto.getParam("MATRICULA");
		
		try {

			// Banco
			List<Object[]> listInfBancoConta = retornarInformacoesBancoConta();
			Map<String, BigDecimal> mapaInfBanco = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfBancoConta) {
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];
				BigDecimal codBcoObj = (BigDecimal) obj[3];

				if (mapaInfBanco.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfBanco.put(codEmpObj + "###" + idExternoObj,
							codBcoObj);
				}
			}

			// Conta
			Map<String, BigDecimal> mapaInfConta = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfBancoConta) {
				BigDecimal codCtabCointObj = (BigDecimal) obj[0];
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];

				if (mapaInfConta.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfConta.put(codEmpObj + "###" + idExternoObj,
							codCtabCointObj);
				}
			}

			// Alunos
			List<Object[]> listInfAlunos = retornarInformacoesAlunos();
			Map<String, BigDecimal> mapaInfAlunos = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfAlunos) {
				BigDecimal codParc = (BigDecimal) obj[0];
				String idExternoObj = (String) obj[1];

				if (mapaInfAlunos.get(idExternoObj) == null) {
					mapaInfAlunos.put(idExternoObj, codParc);
				}
			}

			// Financeiro
			List<Object[]> listInfFinanceiro = retornarInformacoesFinanceiro();
			Map<String, BigDecimal> mapaInfFinanceiro = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				BigDecimal codEmpObj = (BigDecimal) obj[1];
				String idExternoObj = (String) obj[2];

				if (mapaInfFinanceiro.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfFinanceiro.put(codEmpObj + "###" + idExternoObj,
							nuFin);
				}
			}
			
			// Id de Baixa
			List<Object[]> listInfIdBaixa = retornarInformacoesIdBaixa();
			Map<BigDecimal, String> mapaInfIdBaixa = new HashMap<BigDecimal, String>();
			for (Object[] obj : listInfIdBaixa) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				String idBaixa = (String) obj[1];

				if (mapaInfIdBaixa.get(nuFin) == null) {
					mapaInfIdBaixa.put(nuFin, idBaixa);
				}
			}
			
			// Id de Baixa Orig
			List<Object[]> listInfIdBaixaOrig = retornarInformacoesIdBaixaOrig();
			Map<String, BigDecimal> mapaInfIdBaixaOrig = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfIdBaixaOrig) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				String idBaixa = (String) obj[1];
				String idFinOrig = (String) obj[2];
				
				if (mapaInfIdBaixaOrig.get(idBaixa +"###"+ idFinOrig) == null) {
					mapaInfIdBaixaOrig.put(idBaixa +"###"+ idFinOrig, nuFin);
				}
			}

			// NuFin Baixados
			Map<BigDecimal, String> mapaInfFinanceiroBaixado = new HashMap<BigDecimal, String>();
			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				String baixado = (String) obj[3];
				if (mapaInfFinanceiroBaixado.get(nuFin) == null) {
					mapaInfFinanceiroBaixado.put(nuFin, baixado);
				}

			}

			// Valor Desdobramento
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor = new HashMap<BigDecimal, BigDecimal>();
			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				BigDecimal vlrDesdob = (BigDecimal) obj[4];
				if (mapaInfFinanceiroValor.get(nuFin) == null) {
					mapaInfFinanceiroValor.put(nuFin, vlrDesdob);
				}

			}

			// Nro Banco
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco = new HashMap<BigDecimal, BigDecimal>();
			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				BigDecimal nuBco = (BigDecimal) obj[5];
				if (mapaInfFinanceiroBanco.get(nuFin) == null) {
					mapaInfFinanceiroBanco.put(nuFin, nuBco);
				}

			}

			// Tipo de Titulo
			List<Object[]> listInfTipoTitulo = retornarInformacoesTipoTitulo();
			Map<String, BigDecimal> mapaInfTipoTitulo = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfTipoTitulo) {

				BigDecimal codTipTit = (BigDecimal) obj[0];
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];
				BigDecimal qtdParcelas = (BigDecimal) obj[4];

				if (mapaInfTipoTitulo.get(codEmpObj + "###" + idExternoObj + "###" + qtdParcelas) == null) {
					mapaInfTipoTitulo.put(codEmpObj + "###" + idExternoObj + "###" + qtdParcelas,
							codTipTit);
				}
			}

			// Tipo de Titulo Taxa
			Map<String, BigDecimal> mapaInfTipoTituloTaxa = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfTipoTitulo) {
				BigDecimal taxa = (BigDecimal) obj[3];
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];
				BigDecimal qtdParcelas = (BigDecimal) obj[4];

				if (mapaInfTipoTituloTaxa.get(codEmpObj + "###" + idExternoObj + "###" + qtdParcelas) == null) {
					mapaInfTipoTituloTaxa.put(codEmpObj + "###" + idExternoObj + "###" + qtdParcelas,
							taxa);
				}
			}
			
			// Tipo de Titulo Codparc Cart�o
			Map<String, BigDecimal> mapaInfTipoTituloCodparcCartao = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfTipoTitulo) {
				BigDecimal codParcCartao = (BigDecimal) obj[5];
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];
				BigDecimal qtdParcelas = (BigDecimal) obj[4];
				
				if (mapaInfTipoTituloCodparcCartao.get(codEmpObj + "###" + idExternoObj + "###" + qtdParcelas) == null) {
					mapaInfTipoTituloCodparcCartao.put(codEmpObj + "###" + idExternoObj + "###" + qtdParcelas,
							codParcCartao);
				}
			}

			// Menor Movimenta��o Banc�ria Por Conta
			List<Object[]> listInfMenorDataMovBancariaPorConta = retornarInformacoesMenorDataMovBancariaPorConta();
			Map<Long, Date> mapaInfMenorDataMovBancariaPorConta = new HashMap<Long, Date>();
			for (Object[] obj : listInfMenorDataMovBancariaPorConta) {
				Long codCtabCointObj = (Long) obj[0];
				Date dtMinRef = (Date) obj[1];

				if (mapaInfMenorDataMovBancariaPorConta.get(codCtabCointObj) == null) {
					mapaInfMenorDataMovBancariaPorConta.put(codCtabCointObj,
							dtMinRef);
				}
			}

			processDateRange(url, token, codEmp, mapaInfIdBaixaOrig,
					mapaInfIdBaixa,
					mapaInfTipoTituloTaxa,
					mapaInfBanco, mapaInfConta, mapaInfAlunos,
					mapaInfFinanceiro, mapaInfTipoTitulo,
					mapaInfMenorDataMovBancariaPorConta,
					mapaInfFinanceiroBaixado, mapaInfFinanceiroValor,
					mapaInfFinanceiroBanco, mapaInfTipoTituloCodparcCartao,
					dataInicio, dataFim, matricula);
			
			contexto.setMensagemRetorno("Periodo Processado!");
			
		} catch (Exception e) {
			e.printStackTrace();
			contexto.mostraErro(e.getMessage());
		} finally {

			if(selectsParaInsert.size() > 0){

				StringBuilder msgError = new StringBuilder();
				
				System.out.println("Entrou na lista do finally: " + selectsParaInsert.size());
				
				//int idInicial = util.getMaxNumLog();
				
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
				insertLogList(msgError.toString(), codEmp);
				
			}
		
		}

	}
	
	@Override
	public void onTime(ScheduledActionContext arg0) {

		System.out
				.println("/*************** Inicio - JobGetBaixaMap *****************/ ");
		long tempoAnterior = System.currentTimeMillis();
		long tempoInicio = System.currentTimeMillis();

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

		try {

			// Banco
			List<Object[]> listInfBancoConta = retornarInformacoesBancoConta();
			Map<String, BigDecimal> mapaInfBanco = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfBancoConta) {
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];
				BigDecimal codBcoObj = (BigDecimal) obj[3];

				if (mapaInfBanco.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfBanco.put(codEmpObj + "###" + idExternoObj,
							codBcoObj);
				}
			}

			// Conta
			Map<String, BigDecimal> mapaInfConta = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfBancoConta) {
				BigDecimal codCtabCointObj = (BigDecimal) obj[0];
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];

				if (mapaInfConta.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfConta.put(codEmpObj + "###" + idExternoObj,
							codCtabCointObj);
				}
			}

			// Alunos
			List<Object[]> listInfAlunos = retornarInformacoesAlunos();
			Map<String, BigDecimal> mapaInfAlunos = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfAlunos) {
				BigDecimal codParc = (BigDecimal) obj[0];
				String idExternoObj = (String) obj[1];

				if (mapaInfAlunos.get(idExternoObj) == null) {
					mapaInfAlunos.put(idExternoObj, codParc);
				}
			}

			// Financeiro
			List<Object[]> listInfFinanceiro = retornarInformacoesFinanceiro();
			Map<String, BigDecimal> mapaInfFinanceiro = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				BigDecimal codEmpObj = (BigDecimal) obj[1];
				String idExternoObj = (String) obj[2];

				if (mapaInfFinanceiro.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfFinanceiro.put(codEmpObj + "###" + idExternoObj,
							nuFin);
				}
			}

			// Id de Baixa
			List<Object[]> listInfIdBaixa = retornarInformacoesIdBaixa();
			Map<BigDecimal, String> mapaInfIdBaixa = new HashMap<BigDecimal, String>();
			for (Object[] obj : listInfIdBaixa) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				String idBaixa = (String) obj[1];

				if (mapaInfIdBaixa.get(nuFin) == null) {
					mapaInfIdBaixa.put(nuFin, idBaixa);
				}
			}

			// Id de Baixa Orig
			List<Object[]> listInfIdBaixaOrig = retornarInformacoesIdBaixaOrig();
			Map<String, BigDecimal> mapaInfIdBaixaOrig = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfIdBaixaOrig) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				String idBaixa = (String) obj[1];
				String idFinOrig = (String) obj[2];

				if (mapaInfIdBaixaOrig.get(idBaixa + "###" + idFinOrig) == null) {
					mapaInfIdBaixaOrig.put(idBaixa + "###" + idFinOrig, nuFin);
				}
			}
			
			// NuFin Baixados
			Map<BigDecimal, String> mapaInfFinanceiroBaixado = new HashMap<BigDecimal, String>();
			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				String baixado = (String) obj[3];
				if (mapaInfFinanceiroBaixado.get(nuFin) == null) {
					mapaInfFinanceiroBaixado.put(nuFin, baixado);
				}

			}

			// Valor Desdobramento
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor = new HashMap<BigDecimal, BigDecimal>();
			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				BigDecimal vlrDesdob = (BigDecimal) obj[4];
				if (mapaInfFinanceiroValor.get(nuFin) == null) {
					mapaInfFinanceiroValor.put(nuFin, vlrDesdob);
				}

			}

			// Nro Banco
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco = new HashMap<BigDecimal, BigDecimal>();
			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				BigDecimal nuBco = (BigDecimal) obj[5];
				if (mapaInfFinanceiroBanco.get(nuFin) == null) {
					mapaInfFinanceiroBanco.put(nuFin, nuBco);
				}

			}

			// Tipo de Titulo
			List<Object[]> listInfTipoTitulo = retornarInformacoesTipoTitulo();
			Map<String, BigDecimal> mapaInfTipoTitulo = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfTipoTitulo) {

				BigDecimal codTipTit = (BigDecimal) obj[0];
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];
				BigDecimal qtdParcelas = (BigDecimal) obj[4];

				if (mapaInfTipoTitulo.get(codEmpObj + "###" + idExternoObj + "###" + qtdParcelas) == null) {
					mapaInfTipoTitulo.put(codEmpObj + "###" + idExternoObj + "###" + qtdParcelas,
							codTipTit);
				}
			}

			// Tipo de Titulo Taxa
			Map<String, BigDecimal> mapaInfTipoTituloTaxa = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfTipoTitulo) {
				BigDecimal taxa = (BigDecimal) obj[3];
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];
				BigDecimal qtdParcelas = (BigDecimal) obj[4];

				if (mapaInfTipoTituloTaxa.get(codEmpObj + "###" + idExternoObj
						+ "###" + qtdParcelas) == null) {
					mapaInfTipoTituloTaxa.put(codEmpObj + "###" + idExternoObj
							+ "###" + qtdParcelas, taxa);
				}
			}

			// Tipo de Titulo Codparc Cart�o
			Map<String, BigDecimal> mapaInfTipoTituloCodparcCartao = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfTipoTitulo) {
				BigDecimal codParcCartao = (BigDecimal) obj[5];
				Long codEmpObj = (Long) obj[1];
				String idExternoObj = (String) obj[2];
				BigDecimal qtdParcelas = (BigDecimal) obj[4];

				if (mapaInfTipoTituloCodparcCartao.get(codEmpObj + "###"
						+ idExternoObj + "###" + qtdParcelas) == null) {
					mapaInfTipoTituloCodparcCartao
							.put(codEmpObj + "###" + idExternoObj + "###"
									+ qtdParcelas, codParcCartao);
				}
			}

			// Menor Movimenta��o Banc�ria Por Conta
			List<Object[]> listInfMenorDataMovBancariaPorConta = retornarInformacoesMenorDataMovBancariaPorConta();
			Map<Long, Date> mapaInfMenorDataMovBancariaPorConta = new HashMap<Long, Date>();
			for (Object[] obj : listInfMenorDataMovBancariaPorConta) {
				Long codCtabCointObj = (Long) obj[0];
				Date dtMinRef = (Date) obj[1];

				if (mapaInfMenorDataMovBancariaPorConta.get(codCtabCointObj) == null) {
					mapaInfMenorDataMovBancariaPorConta.put(codCtabCointObj,
							dtMinRef);
				}
			}

			jdbc.openSession();

			// Depois rever para ativar todos
			String query = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO";
			
			pstmt = jdbc.getPreparedStatement(query);

			rs = pstmt.executeQuery();

			tempoAnterior = printLogDebug(tempoAnterior,
					"Consulta para capturar o link de integracaoo: AD_LINKSINTEGRACAO");

			while (rs.next()) {
				count++;
				
				System.out.println("Contagem: " + count);

				codEmp = rs.getBigDecimal("CODEMP");

				url = rs.getString("URL");
				token = rs.getString("TOKEN");

				iterarEndpoint(url, token, codEmp, mapaInfIdBaixaOrig,               //erro aqui 
						mapaInfIdBaixa,
						mapaInfTipoTituloTaxa,
						mapaInfBanco, mapaInfConta, mapaInfAlunos,
						mapaInfFinanceiro, mapaInfTipoTitulo,
						mapaInfMenorDataMovBancariaPorConta,
						mapaInfFinanceiroBaixado, mapaInfFinanceiroValor,
						mapaInfFinanceiroBanco, mapaInfTipoTituloCodparcCartao);

				tempoAnterior = printLogDebug(tempoAnterior,
						"onTime - efetuarBaixa da empresa(" + codEmp + ")");

				tempoAnterior = printLogDebug(tempoAnterior,
						"onTime - updateCarga da empresa(" + codEmp + ")");
			}
			
			System.out.println("Chegou ao final da baixa");

			System.out
					.println("/*************** Fim - JobGetBaixaMap *****************/");

			printLogDebug(tempoInicio, "Tempo Total: ");

		} catch (Exception e) {
			e.printStackTrace();
			try {
				insertLogIntegracao(
						"Erro ao integrar Baixas, Mensagem de erro: "
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
			
			if(selectsParaInsert.size() > 0){
				
				StringBuilder msgError = new StringBuilder();
				
				System.out.println("Entrou na lista do finally: " + selectsParaInsert.size());
				
				//int idInicial = util.getMaxNumLog();
				
				int qtdInsert = selectsParaInsert.size();
				
				System.out.println("Lista de selects: " + selectsParaInsert.toString());
				
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
					
					// Verifica��o de depura��o
				    System.out.println("Itera��o: " + i + " de " + qtdInsert);
					
					if (i < qtdInsert) {
						msgError.append(" \nUNION ALL ");
					}
					i++;
				}
				
				System.out.println("Consulta de log: \n" + msgError);
				try {
					insertLogList(msgError.toString(), codEmp);
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				msgError = null;
				this.selectsParaInsert = new ArrayList<String>();
				
			}
		
		}
	}
	
	//alteracao feita aqui 
	public void processDateRange(
	        String url,
	        String token,
	        BigDecimal codemp,
	        Map<String, BigDecimal> mapaInfIdBaixaOrig,
	        Map<BigDecimal, String> mapaInfIdBaixa,
	        Map<String, BigDecimal> mapaInfTipoTituloTaxa,
	        Map<String, BigDecimal> mapaInfBanco,
	        Map<String, BigDecimal> mapaInfConta,
	        Map<String, BigDecimal> mapaInfAlunos,
	        Map<String, BigDecimal> mapaInfFinanceiro,
	        Map<String, BigDecimal> mapaInfTipoTitulo,
	        Map<Long, Date> mapaInfMenorDataMovBancariaPorConta,
	        Map<BigDecimal, String> mapaInfFinanceiroBaixado,
	        Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor,
	        Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
	        Map<String, BigDecimal> mapaInfTipoTituloCodparcCartao,
	        String dataInicio,
	        String dataFim,
	        String matricula) throws Exception {

	    try {
	        LocalDate startDate = LocalDate.parse(dataInicio);
	        LocalDate endDate = LocalDate.parse(dataFim);

	        LocalDate currentDate = startDate;
	        while (!currentDate.isAfter(endDate)) {
	            
	            // Formatar as datas para o formato esperado pela API
	            String dataInicialStr = currentDate.toString() + "T00:00:00";
	            String dataFinalStr = currentDate.toString() + "T23:59:59";
	            
	            // Codificar os parâmetros de data
	            String dataInicialParam = URLEncoder.encode(dataInicialStr, "UTF-8");
	            String dataFinalParam = URLEncoder.encode(dataFinalStr, "UTF-8");

	            // Lista para acumular todos os registros do dia
	            JSONArray todosRegistrosDoDia = new JSONArray();
	            int pagina = 1;
	            boolean temMaisRegistros = true;

	            // Loop de paginação para o dia atual
	            while (temMaisRegistros) {
	                StringBuilder urlBuilder = new StringBuilder(url)
	                    .append("/financeiro/baixas?")
	                    .append("pagina=").append(pagina)
	                    .append("&quantidade=100")
	                    .append("&dataInicial=").append(dataInicialParam)
	                    .append("&dataFinal=").append(dataFinalParam);

	                if (matricula != null && !matricula.trim().isEmpty()) {
	                    String matriculaEncoded = URLEncoder.encode(matricula.trim(), "UTF-8");
	                    urlBuilder.append("&matricula=").append(matriculaEncoded);
	                }

	                String urlCompleta = urlBuilder.toString();
	                System.out.println("URL para baixas (dia: " + currentDate + ", página " + pagina + "): " + urlCompleta);

	                // Fazer a requisição para a página atual
	                String[] response1 = apiGet2(urlCompleta, token);
	                int status = Integer.parseInt(response1[0]);

	                if (status == 200) {
	                    // Processar a resposta JSON
	                    JSONArray paginaAtual = new JSONArray(response1[1]);
	                    
	                    // Adicionar registros ao array acumulado
	                    for (int i = 0; i < paginaAtual.length(); i++) {
	                        todosRegistrosDoDia.put(paginaAtual.getJSONObject(i));
	                    }
	                    
	                    // Verificar se é a última página
	                    if (paginaAtual.length() < 100) {
	                        temMaisRegistros = false;
	                    } else {
	                        pagina++;
	                    }
	                    
	                    System.out.println("Dia " + currentDate + ", página " + pagina + ": " + 
	                                      paginaAtual.length() + " registros. Total acumulado: " + 
	                                      todosRegistrosDoDia.length());
	                } else {
	                    System.err.println("Erro na requisição: Status " + status);
	                    // Se houver erro, interrompemos a paginação para este dia
	                    break;
	                }
	            }
	            
	            // Se temos registros acumulados para este dia, processamos
	            if (todosRegistrosDoDia.length() > 0) {
	                System.out.println("Processando " + todosRegistrosDoDia.length() + 
	                                  " registros de baixas para o dia " + currentDate);
	                
	                // Criar resposta combinada
	                String[] response = new String[] {
	                    "200",
	                    todosRegistrosDoDia.toString()
	                };
	                
	                System.out.println("Dados sendo enviados para efetuarBaixa:");                              //debug 
	                System.out.println("Tamanho do array: " + todosRegistrosDoDia.length());
	                System.out.println("Conteúdo do todosRegistrosDoDia: " + todosRegistrosDoDia.toString());
	                
	                efetuarBaixa(
	                	response,
	                    url,
	                    token,
	                    codemp,
	                    mapaInfIdBaixaOrig,
	                    mapaInfIdBaixa,
	                    mapaInfTipoTituloTaxa,
	                    mapaInfBanco,
	                    mapaInfConta,
	                    mapaInfAlunos,
	                    mapaInfFinanceiro,
	                    mapaInfTipoTitulo,
	                    mapaInfMenorDataMovBancariaPorConta,
	                    mapaInfFinanceiroBaixado,
	                    mapaInfFinanceiroValor,
	                    mapaInfFinanceiroBanco,
	                    mapaInfTipoTituloCodparcCartao
	                );
	            }

	            // Avançar para o próximo dia
	            currentDate = currentDate.plusDays(1);
	        }
	    } catch (Exception e) {
	        System.err.println("Erro ao processar baixas: " + e.getMessage());
	        throw e;
	    }
	}
	
	public void iterarEndpoint(String url, String token, BigDecimal codemp,
			Map<String, BigDecimal> mapaInfIdBaixaOrig,
			Map<BigDecimal, String> mapaInfIdBaixa,
			Map<String, BigDecimal> mapaInfTipoTituloTaxa, 
			Map<String, BigDecimal> mapaInfBanco,
			Map<String, BigDecimal> mapaInfConta,
			Map<String, BigDecimal> mapaInfAlunos,
			Map<String, BigDecimal> mapaInfFinanceiro,
			Map<String, BigDecimal> mapaInfTipoTitulo,
			Map<Long, Date> mapaInfMenorDataMovBancariaPorConta,
			Map<BigDecimal, String> mapaInfFinanceiroBaixado,
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor,
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
			Map<String, BigDecimal> mapaInfTipoTituloCodparcCartao) throws Exception {
		
		Date dataAtual = new Date();

		SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");

		String dataFormatada = formato.format(dataAtual);
		
		//dataFormatada = "2024-11-05";
		
		try {

			String[] response = apiGet2(url
					+ "/financeiro"
					+ "/baixas"
					+ "?quantidade=0"
					+ "&dataInicial=" + dataFormatada + " 00:00:00&dataFinal="
					+ dataFormatada + " 23:59:59", token);

			int status = Integer.parseInt(response[0]);

			System.out.println("Status teste: " + status);

			String responseString = response[1];
			System.out.println("response string baixas: " + responseString);

			efetuarBaixa(response, url, token, codemp, mapaInfIdBaixaOrig,                   //erro aqui 
					mapaInfIdBaixa,
					mapaInfTipoTituloTaxa, mapaInfBanco, mapaInfConta,
					mapaInfAlunos, mapaInfFinanceiro, mapaInfTipoTitulo,
					mapaInfMenorDataMovBancariaPorConta,
					mapaInfFinanceiroBaixado, mapaInfFinanceiroValor,
					mapaInfFinanceiroBanco, mapaInfTipoTituloCodparcCartao);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	
	
	
	
	
	
	
	public void efetuarBaixa(String[] response, String url, String token, BigDecimal codemp,
			Map<String, BigDecimal> mapaInfIdBaixaOrig,
			Map<BigDecimal, String> mapaInfIdBaixa,
			Map<String, BigDecimal> mapaInfTipoTituloTaxa,
			Map<String, BigDecimal> mapaInfBanco,
			Map<String, BigDecimal> mapaInfConta,
			Map<String, BigDecimal> mapaInfAlunos,
			Map<String, BigDecimal> mapaInfFinanceiro,
			Map<String, BigDecimal> mapaInfTipoTitulo,
			Map<Long, Date> mapaInfMenorDataMovBancariaPorConta,
			Map<BigDecimal, String> mapaInfFinanceiroBaixado,
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor,
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
			Map<String, BigDecimal> mapaInfTipoTituloCodparcCartao) throws Exception {

		System.out.println("Entrou no job baixa");
		
		
		boolean movBanc = false;

		SimpleDateFormat formatoOriginal = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat formatoDesejado = new SimpleDateFormat("dd/MM/yyyy");

		Date dataAtual = new Date();
		SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(dataAtual);
		calendar.add(Calendar.DAY_OF_MONTH, -1);

		Date dataUmDiaAtras = calendar.getTime();

		String dataUmDiaFormatada = formato.format(dataUmDiaAtras);
		String dataAtualFormatada = formato.format(dataAtual);

		System.out.println("data um dia atras: " + dataUmDiaFormatada);
		System.out.println("data normal: " + dataAtualFormatada);

		BigDecimal codTipTit = BigDecimal.ZERO;
		BigDecimal codBanco = BigDecimal.ZERO;
		BigDecimal codConta = BigDecimal.ZERO;
		BigDecimal nubco = BigDecimal.ZERO;

		String dataEstorno = "";

		BigDecimal nufin = BigDecimal.ZERO;
		
		String idAluno = "";
		String formaDePagamento = "";
		
		Map<BigDecimal, String> mapIdBaixaAtual = new HashMap<BigDecimal, String>();

		try {

			System.out.println("Dados recebidos em efetuarBaixa:");
			System.out.println("Conteúdo do response[1]: " + response[1]);
			

				JsonParser parser = new JsonParser();
				JsonArray jsonArray = parser.parse(response[1])
						.getAsJsonArray();

				for (JsonElement jsonElement : jsonArray) {
					JsonObject jsonObject = jsonElement.getAsJsonObject();
					
					 System.out.println("JSON recebido: " + jsonObject.toString());
					 
					   // tratamento dos campos obrigatorios
					    String[] camposObrigatorios = {"aluno_id", "titulo_id", "baixa_id", "baixa_valor", "baixa_data"};
					    for (String campo : camposObrigatorios) {
					        if (jsonObject.get(campo) == null || jsonObject.get(campo).isJsonNull()) {
					            throw new Exception("Campo obrigatório ausente ou nulo: " + campo);
					        }
					    }

					System.out.println("Titulo ID: "
							+ jsonObject.get("titulo_id").getAsInt());
					
					System.out.println("Valor da Baixa: "
							+ jsonObject.get("baixa_valor").getAsString());
					
					JsonElement alunoIdElement = jsonObject.get("aluno_id");
					if (alunoIdElement == null || alunoIdElement.isJsonNull()) {
					    throw new Exception("Campo aluno_id é obrigatório mas está nulo");
					}
					idAluno = alunoIdElement.getAsString().trim();
					
					System.out.println("IdAluno: " + idAluno);
					
					BigDecimal codParc = mapaInfAlunos.get(idAluno);
					
					if (codParc != null) {
						
						String tituloId = jsonObject.get("titulo_id").getAsString();
						String baixaId = jsonObject.get("baixa_id").getAsString();

						BigDecimal vlrBaixa = new BigDecimal(jsonObject.get(
								"baixa_valor").getAsString());

						BigDecimal vlrJuros = Optional.ofNullable(jsonObject.get("baixa_juros"))
							    .filter(element -> !element.isJsonNull())
							    .map(JsonElement::getAsString)
							    .map(BigDecimal::new)
							    .orElse(BigDecimal.ZERO);

						BigDecimal vlrMulta = Optional.ofNullable(jsonObject.get("baixa_multa"))
						    .filter(element -> !element.isJsonNull())
						    .map(JsonElement::getAsString)
						    .map(BigDecimal::new)
						    .orElse(BigDecimal.ZERO);

						BigDecimal vlrDesconto = Optional.ofNullable(jsonObject.get("baixa_desconto"))
						    .filter(element -> !element.isJsonNull())
						    .map(JsonElement::getAsString)
						    .map(BigDecimal::new)
						    .orElse(BigDecimal.ZERO);

						BigDecimal vlrOutrosAcrescimos = Optional.ofNullable(jsonObject.get("baixa_outros_acrescimos"))
							.filter(element -> !element.isJsonNull())
							.map(JsonElement::getAsString)
							.map(BigDecimal::new)
							.orElse(BigDecimal.ZERO);

						String dataBaixa = jsonObject.get("baixa_data")
								.getAsString();
						
						System.out.println("Data Baixa: " + dataBaixa);

						Date data = formatoOriginal.parse(dataBaixa);
						
						System.out.println("Date Baixa: " + data);

						String dataBaixaFormatada = formatoDesejado.format(data);

						nufin = mapaInfFinanceiro.get(codemp + "###" + tituloId);

						if (jsonObject.has("baixa_estorno_data")
								&& !jsonObject.get("baixa_estorno_data")
										.isJsonNull()) {

							System.out.println("Entrou no if de estorno");

							dataEstorno = jsonObject.get("baixa_estorno_data")
									.getAsString();
						} else {
							System.out.println("Entrou no else de estorno");
							dataEstorno = null;
						}

						String idExterno = jsonObject.get("local_pagamento_id")
								.getAsString();

						// codBanco = getCodBanco(idExterno, codemp);
						// Buscar pelo mapa
						codBanco = mapaInfBanco.get(codemp + "###" + idExterno);
						
						System.out.println("Banco: " + codBanco);
						
						codConta = mapaInfConta.get(codemp + "###" + idExterno);
						
						System.out.println("Conta: " + codConta);
						
						String nsu_Cartao = "";
						
						if(codConta != null && codBanco != null){
							
							JsonArray formas_de_pagamento = jsonObject
									.getAsJsonArray("formas_de_pagamento");
							
							System.out.println("quantidade de formar de pagamento: " + formas_de_pagamento.size());
							
							BigDecimal taxaCartao = BigDecimal.ZERO;
							
							if (nufin != null && nufin.compareTo(BigDecimal.ZERO) != 0 && formas_de_pagamento.size() == 1) {
								
								BigDecimal codParcCartao = BigDecimal.ZERO;
								
								String dtCredito = "";
								
								for (JsonElement formas_de_pagamentoElement : formas_de_pagamento) {

									JsonObject formas_de_pagamentoObject = formas_de_pagamentoElement
											.getAsJsonObject();

									System.out.println("Forma de pagamento: "
											+ formas_de_pagamentoObject.get(
													"forma_pagamento_id").getAsString());
									
									System.out.println("codemp: " + codemp);
									
									formaDePagamento = formas_de_pagamentoObject.get(
											"forma_pagamento_id").getAsString().trim();

									BigDecimal qtdParcelas = Optional.ofNullable(formas_de_pagamentoObject
										    .get("forma_pagamento_qtdparcelas"))
										    .filter(element -> !element.isJsonNull())    //verifica se é nulo antes de converter
										    .map(JsonElement::getAsBigDecimal)
										    .orElse(BigDecimal.ZERO);
									
									codTipTit = Optional.ofNullable(mapaInfTipoTitulo.get(codemp
											+ "###"
											+ formaDePagamento
											+ "###" + qtdParcelas)).orElse(BigDecimal.ZERO);
									
									System.out.println("Tipo de titulo: " + codTipTit);
									
									taxaCartao = Optional.ofNullable(mapaInfTipoTituloTaxa.get(codemp
											+ "###"
											+ formaDePagamento
											+ "###" + qtdParcelas)).orElse(BigDecimal.ZERO);
									
									System.out.println("Taxa Cart�o: " + taxaCartao);
									
									nsu_Cartao = Optional.ofNullable(formas_de_pagamentoObject
										    .get("forma_pagamento_nsu"))
										    .filter(element -> !element.isJsonNull())     //verifica se é nulo antes de converter
										    .map(JsonElement::getAsString)
										    .orElse("");
									
									codParcCartao = Optional.ofNullable(mapaInfTipoTituloCodparcCartao.get(codemp
											+ "###"
											+ formaDePagamento
											+ "###" + qtdParcelas)).orElse(BigDecimal.ZERO);
									
									dtCredito = Optional.ofNullable(formas_de_pagamentoObject
											.get("forma_pagamento_data_credito"))
											.filter(element -> !element.isJsonNull()) 
							                .map(JsonElement::getAsString)
							                .orElse("");
									
								}
								
								if(taxaCartao.compareTo(BigDecimal.ZERO) != 0){
									vlrBaixa = vlrBaixa.subtract(vlrBaixa.multiply(taxaCartao).divide(BigDecimal.valueOf(100)) );
								}

								System.out.println("estorno: " + dataEstorno);
								System.out.println("Data estorno: "
										+ jsonObject.get("baixa_estorno_data"));
								
								Date dtMinMovConta = mapaInfMenorDataMovBancariaPorConta
										.get(Long.parseLong(codConta.toString()));
								
								System.out.println("dtMinMovConta: " + dtMinMovConta);
								

								if (dataEstorno == null) {
									
									if(dtMinMovConta != null){
										if (data.equals(dtMinMovConta) || data.after(dtMinMovConta)) {
											
											if(codTipTit != null && codTipTit.compareTo(BigDecimal.ZERO) != 0){

													if ("N".equalsIgnoreCase(mapaInfFinanceiroBaixado
															.get(nufin))) {

														System.out.println("Chegou no update");
														if (vlrBaixa
																.compareTo(mapaInfFinanceiroValor
																		.get(nufin)) == 0
															&& (nsu_Cartao == null || nsu_Cartao.isEmpty())) {
															
															System.out
																	.println("Entrou no if do valor");
															/*updateFin(codTipTit, nufin, codBanco,
																	codConta, vlrDesconto,
																	vlrJuros, vlrMulta,
																	vlrOutrosAcrescimos, codemp);*/
															
															updateFinComVlrBaixa(codTipTit, nufin,
																	codBanco, codConta, vlrBaixa,
																	vlrDesconto, vlrJuros,
																	vlrMulta, vlrOutrosAcrescimos, baixaId, codemp);
															
														}else if (nsu_Cartao != null && !nsu_Cartao.isEmpty()){
															
															updateFinCartao(codTipTit, nufin,
																	codBanco, codConta, vlrBaixa,
																	vlrDesconto, vlrJuros, vlrMulta,
																	vlrOutrosAcrescimos, baixaId, codemp, 
																	codParcCartao, dtCredito);
															
														} else {
															System.out
																	.println("Entrou no else do valor");
															updateFinComVlrBaixa(codTipTit, nufin,
																	codBanco, codConta, vlrBaixa,
																	vlrDesconto, vlrJuros,
																	vlrMulta, vlrOutrosAcrescimos, baixaId, codemp);
														}
														
														System.out.println("vlrDesconto: "
																+ vlrDesconto);
														System.out.println("vlrJuros: " + vlrJuros);
														System.out.println("vlrMulta: " + vlrMulta);
														
														if(nsu_Cartao == null || nsu_Cartao.isEmpty()){
															nubco = insertMovBancaria(codConta,
																	vlrBaixa, nufin, dataBaixaFormatada, codemp);

															System.out
																	.println("Passou da mov bancaria: "
																			+ nubco);

															System.out.println("vlrBaixa: " + vlrBaixa);

															updateBaixa(nufin, nubco, vlrBaixa,
																	dataBaixaFormatada, baixaId, codemp);
															
															mapIdBaixaAtual.put(nufin, baixaId);
															
															mapaInfFinanceiroBaixado.put(nufin, "S");
															
															movBanc = true;
														}
														
														/*
														 * insertLogIntegracao(
														 * "Baixa Efetuada Com Sucesso Para o Financeiro: "
														 * + nufin, "Sucesso");
														 */
													} else {
														
														System.out.println("Titulo ja baixado");
														
														String baixaIdExist = Optional.ofNullable(mapaInfIdBaixa.get(nufin)).orElse("");
														
														String baixaIdAtual = Optional.ofNullable(mapIdBaixaAtual.get(nufin)).orElse("");
														
														BigDecimal baixaIdOrig = Optional.ofNullable(mapaInfIdBaixaOrig.get(baixaId + "###" + nufin)).orElse(BigDecimal.ZERO);
														
														System.out
																.println("baixaIdExist: " + baixaIdExist);
														System.out
																.println("baixaIdAtual: " + baixaIdAtual);
														System.out
																.println("baixaIdOrig: " + baixaIdOrig);
														
														if((!baixaIdExist.isEmpty() 
																&& !baixaIdExist.equalsIgnoreCase(baixaId) 
																&& !baixaIdExist.equalsIgnoreCase("N")
																&& baixaIdOrig.compareTo(BigDecimal.ZERO) == 0)
																|| (!baixaIdAtual.isEmpty() && !baixaIdAtual.equalsIgnoreCase(baixaId))){
															
															System.out.println("Baixa Dupla");
															
															BigDecimal nufinDup = insertFin(nufin, vlrBaixa, codTipTit, codemp);
															
															if(nsu_Cartao == null || nsu_Cartao.isEmpty()){
																updateFinComVlrBaixa(codTipTit, nufinDup,
																		codBanco, codConta, vlrBaixa,
																		vlrDesconto, vlrJuros,
																		vlrMulta, vlrOutrosAcrescimos, baixaId, codemp);
																
																nubco = insertMovBancaria(codConta,
																		vlrBaixa, nufinDup, dataBaixaFormatada, codemp);

																movBanc = true;

																updateBaixa(nufinDup, nubco, vlrBaixa,
																		dataBaixaFormatada, baixaId, codemp);
																
																mapaInfFinanceiroBaixado.put(nufinDup, "S");
																
																mapIdBaixaAtual.put(nufinDup, baixaId);
																
																mapaInfIdBaixaOrig.put(baixaId + "###" + nufin, nufinDup);
															}
															
															System.out.println("Fim baixa dupla");
															
														}
														
													}
												
											}else{

												selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Sem \"de para\" de Tipo de Titulo "
														+ "Configurado Para o Metodo de Pagamento: "+formaDePagamento+"' , SYSDATE, 'Aviso', "+codemp+", '' FROM DUAL");
												
											}
											
											
										} else {
											
											selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Baixa Para o Titulo: " +
													  nufin +
													  " N�o Efetuada Pois a Data Minima de Movimenta��o Bancaria "
													  + "Para a Conta " +codConta+
													  " � Superior a Data de Baixa: " +
													  dataBaixaFormatada+"', SYSDATE, 'Aviso', "+codemp+", '"+idAluno+"' FROM DUAL");
											
											
											/*util.inserirLog("Baixa Para o Titulo: " +
													  nufin +
													  " N�o Efetuada Pois a Data Minima de Movimenta��o Bancaria "
													  + "Para a Conta " +codConta+
													  " � Superior a Data de Baixa: " +
													  dataBaixaFormatada, "Aviso", idAluno, codemp);*/
										}
									}else{

										selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Data Minima de Inje��o de Saldo N�o Localizada Para a Conta: "+codConta+"' , SYSDATE, 'Aviso', "+codemp+", '"+idAluno+"' FROM DUAL");
										
									}
									
								}else {
									if ("S".equalsIgnoreCase(mapaInfFinanceiroBaixado
											.get(nufin))) {

										nubco = mapaInfFinanceiroBanco.get(nufin);
										updateFinExtorno(nufin, codemp);
										deleteTgfMbc(nubco, codemp);
										deleteTgfFin(nufin, codemp);
										
										/*
										 * insertLogIntegracao(
										 * "Estorno Efetuado com sucesso",
										 * "Sucesso");
										 */

									}
								}
								
							} else if(nufin != null && nufin.compareTo(BigDecimal.ZERO) != 0 && formas_de_pagamento.size() > 1){
								
								System.out.println("entrou em mais de uma forma de pagamento");
								
								int countBaixa = 0;
								
								for (JsonElement formas_de_pagamentoElement : formas_de_pagamento) {

									JsonObject formas_de_pagamentoObject = formas_de_pagamentoElement
											.getAsJsonObject();
									
									System.out.println("Forma de pagamento: "
											+ formas_de_pagamentoObject.get(
													"forma_pagamento_id"));
									
									System.out.println("codemp: " + codemp);
									
									BigDecimal qtdParcelas = Optional.ofNullable(formas_de_pagamentoObject
											.get("forma_pagamento_qtdparcelas"))
							                .map(JsonElement::getAsBigDecimal)
							                .orElse(BigDecimal.ZERO);
									
									codTipTit = Optional.ofNullable(mapaInfTipoTitulo.get(codemp
											+ "###"
											+ formas_de_pagamentoObject.get(
													"forma_pagamento_id").getAsString().trim()
											+ "###" + qtdParcelas)).orElse(BigDecimal.ZERO);
									
									System.out.println("Tipo de titulo: " + codTipTit);
									
									taxaCartao = Optional.ofNullable(mapaInfTipoTituloTaxa.get(codemp
											+ "###"
											+ formas_de_pagamentoObject.get(
													"forma_pagamento_id").getAsString()
											+ "###" + qtdParcelas)).orElse(BigDecimal.ZERO);
									
									nsu_Cartao = Optional.ofNullable(formas_de_pagamentoObject
											.get("forma_pagamento_nsu"))
							                .map(JsonElement::getAsString)
							                .orElse("");
									
									BigDecimal codParcCartao = Optional.ofNullable(mapaInfTipoTituloCodparcCartao.get(codemp
											+ "###"
											+ formas_de_pagamentoObject.get(
													"forma_pagamento_id").getAsString()
											+ "###" + qtdParcelas)).orElse(BigDecimal.ZERO);
									
									String dtCredito = Optional.ofNullable(formas_de_pagamentoObject
											.get("forma_pagamento_data_credito"))
							                .map(JsonElement::getAsString)
							                .orElse("");
									
									vlrBaixa = formas_de_pagamentoObject.get(
											"forma_pagamento_valor").getAsBigDecimal();
									
									if(taxaCartao.compareTo(BigDecimal.ZERO) != 0){
										vlrBaixa.subtract( vlrBaixa.multiply(taxaCartao).divide(BigDecimal.valueOf(100)) );
									}

									System.out.println("estorno: " + dataEstorno);
									System.out.println("Data estorno: "
											+ jsonObject.get("baixa_estorno_data"));
									
									Date dtMinMovConta = mapaInfMenorDataMovBancariaPorConta
											.get(Long.parseLong(codConta.toString()));
									
									System.out.println("dtMinMovConta: " + dtMinMovConta);
									////
									if(countBaixa == 0){
										System.out.println("contagem 1");
										
										if (dataEstorno == null) {
											
											if(dtMinMovConta != null){
												
												if (data.equals(dtMinMovConta)
														|| data.after(dtMinMovConta)) {

													if (codTipTit != null
															&& codTipTit
																	.compareTo(BigDecimal.ZERO) != 0) {

														if ("N".equalsIgnoreCase(mapaInfFinanceiroBaixado
																.get(nufin))) {

															System.out
																	.println("Chegou no update");
															if (vlrBaixa
																	.compareTo(mapaInfFinanceiroValor
																			.get(nufin)) == 0
																&& (nsu_Cartao == null || nsu_Cartao.isEmpty())) {
																System.out
																		.println("Entrou no if do valor");
//																updateFin(
//																		codTipTit,
//																		nufin,
//																		codBanco,
//																		codConta,
//																		vlrDesconto,
//																		vlrJuros,
//																		vlrMulta,
//																		vlrOutrosAcrescimos,
//																		codemp);
																
																updateFinComVlrBaixa(
																		codTipTit,
																		nufin,
																		codBanco,
																		codConta,
																		vlrBaixa,
																		vlrDesconto,
																		vlrJuros,
																		vlrMulta,
																		vlrOutrosAcrescimos,
																		baixaId, codemp);
																
															} else if (nsu_Cartao != null && !nsu_Cartao.isEmpty()){
																
																updateFinCartao(codTipTit, nufin,
																		codBanco, codConta, vlrBaixa,
																		vlrDesconto, vlrJuros, vlrMulta,
																		vlrOutrosAcrescimos, baixaId, codemp, 
																		codParcCartao, dtCredito);
																
															}else {
																System.out
																		.println("Entrou no else do valor");
																updateFinComVlrBaixa(
																		codTipTit,
																		nufin,
																		codBanco,
																		codConta,
																		vlrBaixa,
																		vlrDesconto,
																		vlrJuros,
																		vlrMulta,
																		vlrOutrosAcrescimos,
																		baixaId, codemp);
															}

															System.out
																	.println("vlrDesconto: "
																			+ vlrDesconto);
															System.out
																	.println("vlrJuros: "
																			+ vlrJuros);
															System.out
																	.println("vlrMulta: "
																			+ vlrMulta);

															
															//updateFin(codTipTit,
															//nufin, codBanco,
															//codConta, vlrDesconto,
															//vlrJuros, vlrMulta);
															
															if(nsu_Cartao == null || nsu_Cartao.isEmpty()){
																nubco = insertMovBancaria(
																		codConta, vlrBaixa,
																		nufin,
																		dataBaixaFormatada,
																		codemp);

																System.out
																		.println("Passou da mov bancaria: "
																				+ nubco);

																System.out
																		.println("vlrBaixa: "
																				+ vlrBaixa);

																updateBaixaParcial(nufin,
																		nubco, vlrBaixa,
																		dataBaixaFormatada,
																		codemp);

																movBanc = true;
																
																mapaInfFinanceiroBaixado.put(nufin, "S");
																
																mapIdBaixaAtual.put(nufin, baixaId);
																
															}
															
															countBaixa++;
															
//															insertLogIntegracao(
//															"Baixa Efetuada Com Sucesso Para o Financeiro: "
//															+ nufin, "Sucesso");
															
														} else {
															System.out
																	.println("Financeiro "
																			+ nufin
																			+ " j� baixado");
														}

													} else {

														selectsParaInsert
																.add("SELECT <#NUMUNICO#>, 'Sem \"de para\" de Tipo de Titulo "
																		+ "Configurado Para o Metodo de Pagamento: "
																		+ formas_de_pagamentoObject
																				.get("forma_pagamento_id")
																		+ "' , SYSDATE, 'Aviso', "
																		+ codemp
																		+ ", '"+idAluno+"' FROM DUAL");

													}

												} else {
														
														selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Baixa Para o Titulo: " +
																  nufin +
																  " N�o Efetuada Pois a Data Minima de Movimenta��o Bancaria "
																  + "Para a Conta " +codConta+
																  " � Superior a Data de Baixa: " +
																  dataBaixaFormatada+"', SYSDATE, 'Aviso', "+codemp+", '"+idAluno+"' FROM DUAL");
														
														
//														util.inserirLog("Baixa Para o Titulo: " +
//																  nufin +
//																  " N�o Efetuada Pois a Data Minima de Movimenta��o Bancaria "
//																  + "Para a Conta " +codConta+
//																  " � Superior a Data de Baixa: " +
//																  dataBaixaFormatada, "Aviso", idAluno, codemp);
													}
												
											}else{

												selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Data Minima de Inje��o de Saldo N�o Localizada Para a Conta: "+codConta+"' , SYSDATE, 'Aviso', "+codemp+", '"+idAluno+"' FROM DUAL");
												
											}
										
										} else {
											if ("S".equalsIgnoreCase(mapaInfFinanceiroBaixado
													.get(nufin))) {

												nubco = mapaInfFinanceiroBanco.get(nufin);
												updateFinExtorno(nufin, codemp);
												deleteTgfMbc(nubco, codemp);
												deleteTgfFin(nufin, codemp);
												
												
//												insertLogIntegracao(
//												"Estorno Efetuado com sucesso",
//												"Sucesso");
												

											}
										}
										
									}else if (codTipTit != null && codTipTit.compareTo(BigDecimal.ZERO) != 0 && countBaixa > 0){
										System.out.println("contagem 2");
										
										if(nsu_Cartao == null || nsu_Cartao.isEmpty()){
											
											BigDecimal nufinDup = insertFin(nufin,
													vlrBaixa, codTipTit, codemp);

											nubco = insertMovBancaria(codConta,
													vlrBaixa, nufinDup,
													dataBaixaFormatada, codemp);

											movBanc = true;
											System.out
													.println("Passou da mov bancaria duplicada: "
															+ nubco);

											System.out.println("vlrBaixa: " + vlrBaixa);

											updateBaixa(nufinDup, nubco, vlrBaixa,
													dataBaixaFormatada, baixaId, codemp);

											mapaInfFinanceiroBaixado.put(nufinDup, "S");

											mapIdBaixaAtual.put(nufinDup, baixaId);
											
										}else{
											
											insertFinCartao(nufin, vlrBaixa, codTipTit, codemp, dtCredito);
											
										}
										

										countBaixa++;

									}///////
									
								}
								
							}else {
								System.out
										.println("N�o foi possivel encontrar financeiro com id externo "
												+ tituloId);
							}
							
						}else{
							
							selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Sem \"de para\" Configurado para o local de pagamento: "+idExterno+"' , SYSDATE, 'Aviso', "+codemp+", '' FROM DUAL");
							
						}
						
						
					}
					
					movBanc = false;
					
					nubco = BigDecimal.ZERO;
					
				}

		} catch (Exception e) {
			e.printStackTrace();

			if (movBanc) {
				updateFinExtorno(nufin, codemp);
				deleteTgfMbc(nubco, codemp);
				System.out.println("Apagou mov bank");
			}
			
			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Mensagem de erro nas Baixas: " + e.getMessage()+"', SYSDATE, 'Erro', "+codemp+", '"+idAluno+"' FROM DUAL");
			
			
			/*try {
				util.inserirLog(
						"Mensagem de erro nas Baixas: " + e.getMessage(),
						"Erro", idAluno, codemp);
			} catch (Exception e1) {
				e1.printStackTrace();
			}*/
		} finally {
			
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

	public void updateFin(BigDecimal codtiptit, BigDecimal nufin,
			BigDecimal codBanco, BigDecimal codConta, BigDecimal vlrDesconto,
			BigDecimal vlrJuros, BigDecimal vlrMulta,
			BigDecimal vlrOutrosAcrescimos, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {

			jdbc.openSession();

			String sqlNota = "UPDATE TGFFIN SET " + "CODTIPTIT = ?, "
					+ "CODBCO = ?, " + "CODCTABCOINT = ?, ";

			sqlNota += "AD_VLRDESCINT = " + vlrDesconto + ", ";
			sqlNota += "VLRINSS = 0, " + "VLRIRF = 0, " + "VLRISS = 0, ";
			sqlNota += "AD_VLRMULTAINT = " + vlrMulta + ", ";
			sqlNota += "AD_VLRJUROSINT = " + vlrJuros + ", AD_OUTACRESCIMOS = "
					+ vlrOutrosAcrescimos;
			sqlNota += ", TIPJURO = null, ";
			sqlNota += "TIPMULTA = null";

			sqlNota += " WHERE nufin = ?";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setBigDecimal(1, codtiptit);
			pstmt.setBigDecimal(2, codBanco);
			pstmt.setBigDecimal(3, codConta);
			pstmt.setBigDecimal(4, nufin);

			pstmt.executeUpdate();

			System.out.println("Passou do update");
		} catch (SQLException e) {
			e.printStackTrace();
			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Atualizar Financeiro Para baixa: "+e.getMessage()+"' , SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public void deleteTgfMbc(BigDecimal nubco, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {

			jdbc.openSession();

			String sqlNota = "DELETE FROM TGFMBC WHERE NUBCO = " + nubco;

			pstmt = jdbc.getPreparedStatement(sqlNota);

			pstmt.executeUpdate();

			System.out.println("Passou do update");
		} catch (SQLException e) {
			e.printStackTrace();
			
			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Deletar Mov. Bancaria: "+e.getMessage()+"' , SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public void updateFinExtorno(BigDecimal nufin, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {

			jdbc.openSession();

			String sqlNota = "UPDATE TGFFIN SET "
					+ "VLRBAIXA = 0, "
					+ "DHBAIXA = NULL, "
					+ "NUBCO = NULL, "
					+ "CODTIPOPERBAIXA = 0, "
					+ "DHTIPOPERBAIXA = (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 0), "
					+ "CODUSUBAIXA = NULL  " + "WHERE NUFIN = " + nufin;

			pstmt = jdbc.getPreparedStatement(sqlNota);

			pstmt.executeUpdate();

			System.out.println("Passou do update");
		} catch (SQLException e) {
			e.printStackTrace();
			
			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Extornar Titulo: "+e.getMessage()+"' , SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public void updateFinComVlrBaixa(BigDecimal codtiptit, BigDecimal nufin,
			BigDecimal codBanco, BigDecimal codConta, BigDecimal vlrBaixa,
			BigDecimal vlrDesconto, BigDecimal vlrJuros, BigDecimal vlrMulta,
			BigDecimal vlrOutrosAcrescimos, String baixaId, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {

			jdbc.openSession();

			String sqlNota = "UPDATE TGFFIN SET " + "CODTIPTIT = ?, "
					+ "CODBCO = ?, " + "CODCTABCOINT = ?, "
					+ "AD_VLRDESCINT = " + vlrDesconto + ", " + "VLRINSS = 0, "
					+ "VLRIRF = 0, " + "VLRISS = 0, " + "AD_VLRJUROSINT = "
					+ vlrJuros + ", " + "AD_VLRMULTAINT = " + vlrMulta + ", "
					+ "TIPJURO = null, AD_VLRORIG = VLRDESDOB, "
					+ "VLRDESDOB = " + vlrBaixa + ", "
					+ "TIPMULTA = null, AD_OUTACRESCIMOS = "
					+ vlrOutrosAcrescimos +", AD_BAIXAID = "+baixaId+" WHERE nufin = ?";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setBigDecimal(1, codtiptit);
			pstmt.setBigDecimal(2, codBanco);
			pstmt.setBigDecimal(3, codConta);
			pstmt.setBigDecimal(4, nufin);

			pstmt.executeUpdate();

			System.out.println("Passou do update");
		} catch (SQLException e) {
			e.printStackTrace();

			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Atualizar Titulo Para Baixa: "+e.getMessage()+"' , SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}
	
	public void updateFinCartao(BigDecimal codtiptit, BigDecimal nufin,
			BigDecimal codBanco, BigDecimal codConta, BigDecimal vlrBaixa,
			BigDecimal vlrDesconto, BigDecimal vlrJuros, BigDecimal vlrMulta,
			BigDecimal vlrOutrosAcrescimos, String baixaId, BigDecimal codemp, 
			BigDecimal codParcCartao, String dtCredito) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		try {
			
			jdbc.openSession();
			
			String sqlNota = "UPDATE TGFFIN SET " + "CODTIPTIT = ?, "
					+ "CODBCO = ?, " 
					+ "CODCTABCOINT = ?, "
					+ "AD_VLRDESCINT = " + vlrDesconto + ", " 
					+ "VLRINSS = 0, "
					+ "VLRIRF = 0, " 
					+ "VLRISS = 0, " 
					+ "AD_VLRJUROSINT = " + vlrJuros + ", " 
					+ "AD_VLRMULTAINT = " + vlrMulta + ", "
					+ "TIPJURO = null, AD_VLRORIG = VLRDESDOB, "
					+ "VLRDESDOB = " + vlrBaixa + ", "
					+ "TIPMULTA = null, AD_OUTACRESCIMOS = " + vlrOutrosAcrescimos +", "
					+ "AD_BAIXAID = "+baixaId+", CODPARC = "+codParcCartao+", "
					+ "AD_BAIXA_CARTAO = 'S', DTVENC = TO_DATE('"+dtCredito+"', 'YYYY-MM-DD') WHERE nufin = ?";
			
			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setBigDecimal(1, codtiptit);
			pstmt.setBigDecimal(2, codBanco);
			pstmt.setBigDecimal(3, codConta);
			pstmt.setBigDecimal(4, nufin);
			
			pstmt.executeUpdate();
			
			System.out.println("Passou do update");
		} catch (SQLException e) {
			e.printStackTrace();
			
			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Atualizar Titulo Para Baixa de Cart�o: "+e.getMessage()+"' , SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public void updateBaixa(BigDecimal nufin, BigDecimal nubco,
			BigDecimal vlrDesdob, String dataBaixaFormatada, 
			String baixaId, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {

			jdbc.openSession();

			String sqlNota = "UPDATE TGFFIN SET "
					+ "VLRBAIXA = "
					+ vlrDesdob
					+ ", "
					+ "DHBAIXA = '"
					+ dataBaixaFormatada
					+ "', "
					+ "NUBCO = "
					+ nubco
					+ ", "
					+ "CODTIPOPERBAIXA = 1400, "
					+ "DHTIPOPERBAIXA = (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 1400), "
					+ "CODUSUBAIXA = 0, AD_BAIXAID = "+baixaId+"  " + "WHERE NUFIN = " + nufin;

			pstmt = jdbc.getPreparedStatement(sqlNota);

			pstmt.executeUpdate();

			System.out.println("Passou do update");
		} catch (SQLException e) {
			e.printStackTrace();

			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Baixar Titulo: "+e.getMessage()+"' , SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}
	
	public void updateBaixaParcial(BigDecimal nufin, BigDecimal nubco,
			BigDecimal vlrDesdob, String dataBaixaFormatada, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		try {
			
			jdbc.openSession();
			
			String sqlNota = "UPDATE TGFFIN SET "
					+ "VLRBAIXA = "
					+ vlrDesdob
					+ ", "
					+ "DHBAIXA = '"
					+ dataBaixaFormatada
					+ "', "
					+ "NUBCO = "
					+ nubco
					+ ", "
					+ "CODTIPOPERBAIXA = 1400, "
					+ "DHTIPOPERBAIXA = (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 1400), "
					+ "CODUSUBAIXA = 0, AD_BAIXAPARCIAL = 'S'  " + "WHERE NUFIN = " + nufin;
			
			pstmt = jdbc.getPreparedStatement(sqlNota);
			
			pstmt.executeUpdate();
			
			System.out.println("Passou do update");
		} catch (SQLException e) {
			e.printStackTrace();

			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Baixar Parcialmente Um Titulo: "+e.getMessage()+"' , SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
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
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			if (jdbc != null) {
				jdbc.closeSession();
			}
		}

	}

	public BigDecimal insertMovBancaria(BigDecimal contaBancaria,
			BigDecimal vlrDesdob, BigDecimal nufin, 
			String dataBaixaFormatada, BigDecimal codemp)
			throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		EnviromentUtils util = new EnviromentUtils();

		BigDecimal nubco = util.getMaxNumMbc();

		try {

			jdbc.openSession();

			String sqlUpdate = "INSERT INTO TGFMBC " + "(NUBCO, " + "CODLANC, "
					+ "DTLANC, " + "CODTIPOPER, " + "DHTIPOPER, "
					+ "DTCONTAB, " + "HISTORICO, " + "CODCTABCOINT, "
					+ "NUMDOC, " + "VLRLANC, " + "TALAO, " + "PREDATA, "
					+ "CONCILIADO, " + "DHCONCILIACAO, " + "ORIGMOV, "
					+ "NUMTRANSF, " + "RECDESP, " + "DTALTER, "
					+ "DTINCLUSAO, " + "CODUSU, " + "VLRMOEDA, " + "SALDO, "
					+ "CODCTABCOCONTRA, " + "NUBCOCP, " + "CODPDV) "
					+ " VALUES ("
					+ nubco
					+ ", " // pk
					+ "1, "
					+ "'"
					+ dataBaixaFormatada
					+ "'"
					+ ", " // dtneg
					+ "1400, " // top baixa
					+ "(SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 1400), " // dhtop
					+ "NULL, "
					+ "(SELECT HISTORICO FROM TGFFIN WHERE NUFIN = "
					+ nufin
					+ "), " // historico
					+ ""
					+ contaBancaria
					+ ", " // conta bancaria (CODCTABCOINT)
					+ "0, " // NUMNOTA
					+ ""
					+ vlrDesdob
					+ ", " // vlrdesdob
					+ "NULL, "
					+ "'"
					+ dataBaixaFormatada
					+ "', " // dtneg2
					+ "'N', "
					+ "NULL, "
					+ "'F', "
					+ "NULL, "
					+ "1, "
					+ "SYSDATE, " + "SYSDATE, " + "0, " // Usuario
					+ "0, " + "" + vlrDesdob + ", " // vlrDesdob
					+ "NULL,  " + "NULL, " + "NULL) ";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.executeUpdate();

		} catch (Exception se) {
			se.printStackTrace();

			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Inserir Mov. Bancaria: "+se.getMessage().replace("'", "\"")+"' , SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			if (jdbc != null) {
				jdbc.closeSession();
			}
		}

		return nubco;

	}

	public BigDecimal getMaxNumMbc() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;

		try {

			updateNumMbc();

			jdbc.openSession();

			// String sqlNota = "SELECT SEQ_TGFFIN_NUFIN.NEXTVAL FROM DUAL";
			String sqlNota = "SELECT MAX(ULTCOD) AS ULTCOD FROM TGFNUM WHERE ARQUIVO = 'TGFMBC'";

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

	public void updateNumMbc() throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {

			jdbc.openSession();

			String sqlUpdate = "UPDATE TGFNUM SET ULTCOD = NVL(ULTCOD, 0) + 1  WHERE ARQUIVO = 'TGFMBC'";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.executeUpdate();

		} catch (Exception se) {
			se.printStackTrace();
		} finally {
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

			String sqlUpdate = "UPDATE AD_ALUNOS SET INTEGRADO_BAIXA = 'S' WHERE ID_EXTERNO = '"
					+ idAluno + "'";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.executeUpdate();

		} catch (Exception se) {
			se.printStackTrace();
		} finally {
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
			System.out.println("Entrou no UPDATE da flag dos alunos baixa");
			jdbc.openSession();

			String sqlUpdate = "UPDATE AD_ALUNOS SET INTEGRADO_BAIXA = 'N'";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.executeUpdate();

		} catch (Exception se) {
			se.printStackTrace();
		} finally {
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
		try {
			jdbc.openSession();

			String sqlUpd = "UPDATE AD_CARGAALUNOS SET INTEGRADO_BAIXA = 'S' WHERE IDCARGA = "
					+ idCarga;

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

	private void resetCarga(BigDecimal codEmp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlUpd = "UPDATE AD_CARGAALUNOS SET INTEGRADO_BAIXA = 'N' WHERE CODEMP = "
					+ codEmp + " AND INTEGRADO_BAIXA = 'S'";

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

	/* Novos */

	public long printLogDebug(long tempoAnterior, String msgRetornoLog) {
		// Log Tempo
		boolean modoDebug = true;
		if (modoDebug) {
			long tempoAgora = System.currentTimeMillis();
			long diffInSeconds = tempoAgora - tempoAnterior;
			tempoAnterior = tempoAgora;
			System.out.println(msgRetornoLog + " : " + diffInSeconds);
		}

		return tempoAnterior;
	}

	public List<Object[]> retornarInformacoesBancoConta() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "	SELECT 	CODCTABCOINT, CODEMP, IDEXTERNO, CODBCO ";
			sql += "		FROM  	ad_infobankbaixa ";
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[4];
				ret[0] = rs.getBigDecimal("CODCTABCOINT");
				ret[1] = rs.getLong("CODEMP");
				ret[2] = rs.getString("IDEXTERNO");
				ret[3] = rs.getBigDecimal("CODBCO");

				listRet.add(ret);
			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw new Exception("Erro Ao Executar Metodo retornarInformacoesBancoConta");
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

	public List<Object[]> retornarInformacoesAlunos() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "	SELECT 	CODPARC, ID_EXTERNO ";
			sql += "		FROM  	AD_ALUNOS ";
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[2];
				ret[0] = rs.getBigDecimal("CODPARC");
				ret[1] = rs.getString("ID_EXTERNO");

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

	public List<Object[]> retornarInformacoesFinanceiro() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "	SELECT 	CODEMP, NUFIN, AD_IDEXTERNO, (CASE WHEN DHBAIXA IS NOT NULL THEN 'S' ELSE 'N' END) BAIXADO, VLRDESDOB, NUBCO ";
			sql += "		FROM  	TGFFIN ";
			sql += "		WHERE  	RECDESP = 1 ";
			sql += "		    AND PROVISAO = 'N' ";
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[6];
				ret[0] = rs.getBigDecimal("NUFIN");
				ret[1] = rs.getBigDecimal("CODEMP");
				ret[2] = rs.getString("AD_IDEXTERNO");
				ret[3] = rs.getString("BAIXADO");
				ret[4] = rs.getBigDecimal("VLRDESDOB");
				ret[5] = rs.getBigDecimal("NUBCO");

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

	public List<Object[]> retornarInformacoesTipoTitulo() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "	SELECT 	CODEMP, CODTIPTIT, IDEXTERNO, TAXACART, NVL(QTD_PARCELAS, 0) AS QTD_PARCELA,"
					+ "(SELECT TIT.CODPARCTEF FROM TGFTIT TIT WHERE TIT.CODTIPTIT = AD_TIPTITINTEGRACAO.CODTIPTIT) AS CODPARCTEF ";
			sql += "		FROM  	AD_TIPTITINTEGRACAO WHERE IDEXTERNO IS NOT NULL";
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[6];
				ret[0] = rs.getBigDecimal("CODTIPTIT");
				ret[1] = rs.getLong("CODEMP");
				ret[2] = rs.getString("IDEXTERNO").trim();
				ret[3] = rs.getBigDecimal("TAXACART");
				ret[4] = rs.getBigDecimal("QTD_PARCELA");
				ret[5] = rs.getBigDecimal("CODPARCTEF");

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

	public List<Object[]> retornarInformacoesMenorDataMovBancariaPorConta()
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "	SELECT 	CODCTABCOINT, MIN(REFERENCIA) DTREF ";
			sql += "		FROM  	TGFSBC ";
			sql += "	    GROUP BY CODCTABCOINT ";
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[2];
				ret[0] = rs.getLong("CODCTABCOINT");
				ret[1] = rs.getDate("DTREF");

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
	
	public void deleteTgfFin(BigDecimal nufin, BigDecimal codemp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();
			
			String sqlNota = "DELETE FROM TGFFIN WHERE NUFIN = " + nufin;
			
			pstmt = jdbc.getPreparedStatement(sqlNota);
			
			pstmt.executeUpdate();
			
			System.out.println("Passou do update");
		} catch (SQLException e) {
			e.printStackTrace();

			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Excluir Titulo: "+e.getMessage()+"' , SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}
	
	public BigDecimal insertFin(BigDecimal nufinOrig, 
			BigDecimal vlrDesdob, BigDecimal codTipTit,
			BigDecimal codemp)
			throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		EnviromentUtils util = new EnviromentUtils();
		
		System.out.println("Chegou no insert do financeiro segundo");
		
		BigDecimal nufin = util.getMaxNumFin(true);

		try {

			jdbc.openSession();

			String sqlUpdate = "INSERT INTO TGFFIN " + "        (NUFIN, "
					+ "         NUNOTA, " + "         NUMNOTA, "
					+ "         ORIGEM, " + "         RECDESP, "
					+ "         CODEMP, " + "         CODCENCUS, "
					+ "         CODNAT, " + "         CODTIPOPER, "
					+ "         DHTIPOPER, " + "         CODTIPOPERBAIXA, "
					+ "         DHTIPOPERBAIXA, " + "         CODPARC, "
					+ "         CODTIPTIT, " + "         VLRDESDOB, "
					+ "         VLRDESC, " + "         VLRBAIXA, "
					+ "         CODBCO, " + "         CODCTABCOINT, "
					+ "         DTNEG, " + "         DHMOV, "
					+ "         DTALTER, " + "         DTVENC, "
					+ "         DTPRAZO, " + "         DTVENCINIC, "
					+ "         TIPJURO, " + "         TIPMULTA, "
					+ "         HISTORICO, " + "         TIPMARCCHEQ, "
					+ "         AUTORIZADO, " + "         BLOQVAR, "
					+ "         INSSRETIDO, " + "         ISSRETIDO, "
					+ "         PROVISAO, " + "         RATEADO, "
					+ "         TIMBLOQUEADA, " + "         IRFRETIDO, "
					+ "         TIMTXADMGERALU, " + "         VLRDESCEMBUT, "
					+ "         VLRINSS, " + "         VLRIRF, "
					+ "         VLRISS, " + "         VLRJURO, "
					+ "         VLRJUROEMBUT, " + "         VLRJUROLIB, "
					+ "         VLRJURONEGOC, " + "         VLRMOEDA, "
					+ "         VLRMOEDABAIXA, " + "         VLRMULTA, "
					+ "         VLRMULTAEMBUT, " + "         VLRMULTALIB, "
					+ "         VLRMULTANEGOC, " + "         VLRPROV, "
					+ "         VLRVARCAMBIAL, " + "         VLRVENDOR, "
					+ "         ALIQICMS, " + "         BASEICMS, "
					+ "         CARTAODESC, " + "         CODMOEDA, "
					+ "         CODPROJ, " + "         CODVEICULO, "
					+ "         CODVEND, " + "         DESPCART, "
					+ "         NUMCONTRATO, " + "         ORDEMCARGA, "
					+ "         CODUSU," + "         AD_IDEXTERNO,"
					+ "         AD_IDALUNO, AD_NUFINORIG, AD_BAIXAPARCIAL) " + "        "
					
					+ " (SELECT "+nufin+", NULL, 0, 'F', recDesp ,codemp ,codCenCus ,codNat ,codTipOper ,(SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = TGFFIN.codTipOper), 0, (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 0), codparc ,"+codTipTit+", "+vlrDesdob+", 0, 0, CODBCO, CODCTABCOINT, DTNEG , SYSDATE, SYSDATE, DTVENC , SYSDATE, DTVENCINIC , 1 , 1 , null , 'I' , 'N' , 'N' , 'N' , 'N' , 'N' , 'N' , 'N' , 'S' , 'S' , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, NULL, AD_IDALUNO, "+nufinOrig+", 'S' FROM TGFFIN WHERE NUFIN = "+nufinOrig+")";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);

			pstmt.executeUpdate();

		} catch (Exception se) {
			se.printStackTrace();

			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Gerar Titulo Parcial: "+se.getMessage()+"' , SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
			/*try {
				util.inserirLog(
						"Erro ao gerar financeiro parcial, Nufin Orig: "+nufin+"\nMensagem de erro: "
								+ se.getMessage(), "Erro", "", codemp);
			} catch (Exception e1) {
				e1.printStackTrace();
			}*/
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
	
	public BigDecimal insertFinCartao(BigDecimal nufinOrig, 
			BigDecimal vlrDesdob, BigDecimal codTipTit,
			BigDecimal codemp, String dtCredito)
					throws Exception {
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		EnviromentUtils util = new EnviromentUtils();
		
		System.out.println("Chegou no insert do financeiro cart�o");
		
		BigDecimal nufin = util.getMaxNumFin(true);
		
		try {
			
			jdbc.openSession();
			
			String sqlUpdate = "INSERT INTO TGFFIN " + "        (NUFIN, "
					+ "         NUNOTA, " + "         NUMNOTA, "
					+ "         ORIGEM, " + "         RECDESP, "
					+ "         CODEMP, " + "         CODCENCUS, "
					+ "         CODNAT, " + "         CODTIPOPER, "
					+ "         DHTIPOPER, " + "         CODTIPOPERBAIXA, "
					+ "         DHTIPOPERBAIXA, " + "         CODPARC, "
					+ "         CODTIPTIT, " + "         VLRDESDOB, "
					+ "         VLRDESC, " + "         VLRBAIXA, "
					+ "         CODBCO, " + "         CODCTABCOINT, "
					+ "         DTNEG, " + "         DHMOV, "
					+ "         DTALTER, " + "         DTVENC, "
					+ "         DTPRAZO, " + "         DTVENCINIC, "
					+ "         TIPJURO, " + "         TIPMULTA, "
					+ "         HISTORICO, " + "         TIPMARCCHEQ, "
					+ "         AUTORIZADO, " + "         BLOQVAR, "
					+ "         INSSRETIDO, " + "         ISSRETIDO, "
					+ "         PROVISAO, " + "         RATEADO, "
					+ "         TIMBLOQUEADA, " + "         IRFRETIDO, "
					+ "         TIMTXADMGERALU, " + "         VLRDESCEMBUT, "
					+ "         VLRINSS, " + "         VLRIRF, "
					+ "         VLRISS, " + "         VLRJURO, "
					+ "         VLRJUROEMBUT, " + "         VLRJUROLIB, "
					+ "         VLRJURONEGOC, " + "         VLRMOEDA, "
					+ "         VLRMOEDABAIXA, " + "         VLRMULTA, "
					+ "         VLRMULTAEMBUT, " + "         VLRMULTALIB, "
					+ "         VLRMULTANEGOC, " + "         VLRPROV, "
					+ "         VLRVARCAMBIAL, " + "         VLRVENDOR, "
					+ "         ALIQICMS, " + "         BASEICMS, "
					+ "         CARTAODESC, " + "         CODMOEDA, "
					+ "         CODPROJ, " + "         CODVEICULO, "
					+ "         CODVEND, " + "         DESPCART, "
					+ "         NUMCONTRATO, " + "         ORDEMCARGA, "
					+ "         CODUSU," + "         AD_IDEXTERNO,"
					+ "         AD_IDALUNO, AD_NUFINORIG, AD_BAIXAPARCIAL, AD_BAIXA_CARTAO, AD_BAIXAID) " + "        "
					
					+ " (SELECT "+nufin+", NULL, 0, 'F', recDesp ,codemp ,codCenCus ,codNat ,codTipOper ,(SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = TGFFIN.codTipOper), 0, (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 0), codparc ,"+codTipTit+", "+vlrDesdob+", 0, 0, CODBCO, CODCTABCOINT, DTNEG , SYSDATE, SYSDATE, TO_DATE('"+dtCredito+"', 'YYYY-MM-DD'), SYSDATE, DTVENCINIC , 1 , 1 , null , 'I' , 'N' , 'N' , 'N' , 'N' , 'N' , 'N' , 'N' , 'S' , 'S' , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, NULL, AD_IDALUNO, "+nufinOrig+", 'S', 'S', AD_BAIXAID FROM TGFFIN WHERE NUFIN = "+nufinOrig+")";
			
			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			
			pstmt.executeUpdate();
			
		} catch (Exception se) {
			se.printStackTrace();
			
			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro Ao Gerar Titulo Parcial: "+se.getMessage()+"' , SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
			/*try {
				util.inserirLog(
						"Erro ao gerar financeiro parcial, Nufin Orig: "+nufin+"\nMensagem de erro: "
								+ se.getMessage(), "Erro", "", codemp);
			} catch (Exception e1) {
				e1.printStackTrace();
			}*/
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
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			if (jdbc != null) {
				jdbc.closeSession();
			}
		}

	}
	
	public List<Object[]> retornarInformacoesIdBaixa() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			
			String sql = "	SELECT 	NUFIN, NVL(AD_BAIXAID, 'N') as BAIXAID";
			sql += "		FROM  	TGFFIN ";
			sql += "		WHERE  	RECDESP = 1 ";
			sql += "		    AND PROVISAO = 'N' "
				+  "            AND DHBAIXA IS NOT NULL"
				+  "            AND AD_IDALUNO IS NOT NULL";
			
			
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			
			while (rs.next()) {
				Object[] ret = new Object[2];
				ret[0] = rs.getBigDecimal("NUFIN");
				ret[1] = rs.getString("BAIXAID");
				
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
	
	
	public List<Object[]> retornarInformacoesIdBaixaOrig() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			
			String sql = "	SELECT 	NUFIN, NVL(AD_BAIXAID, 'N') as BAIXAID, AD_NUFINORIG";
			sql += "		FROM  	TGFFIN ";
			sql += "		WHERE  	RECDESP = 1 ";
			sql += "		    AND PROVISAO = 'N' "
					+  "            AND AD_NUFINORIG IS NOT NULL"
					+  "            AND AD_IDALUNO IS NOT NULL";
			
			
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			
			while (rs.next()) {
				Object[] ret = new Object[3];
				ret[0] = rs.getBigDecimal("NUFIN");
				ret[1] = rs.getString("BAIXAID");
				ret[2] = rs.getString("AD_NUFINORIG");
				
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
	
	public void insertLogList(String listInsert, BigDecimal codemp) throws Exception {
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
