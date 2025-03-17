package br.com.sankhya.acoesgrautec.extensions;

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
import java.util.Collection;
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
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class AcaoGetTitulosCarga implements AcaoRotinaJava, ScheduledAction {
	
	private List<String> selectsParaInsertLog = new ArrayList<String>();
	private EnviromentUtils util = new EnviromentUtils();
	
	@Override
	public void doAction(ContextoAcao contexto) throws Exception {
		
		Registro[] linhas = contexto.getLinhas();
		Registro registro = linhas[0];
		
		String url = (String) registro.getCampo("URL");
		String token = (String) registro.getCampo("TOKEN");
		BigDecimal codEmp = (BigDecimal) registro.getCampo("CODEMP");
		String profissionalizante = Optional.ofNullable(registro.getCampo("PROFISSIONAL")).orElse("N").toString(); 
		String tecnico = (String) Optional.ofNullable(registro.getCampo("TECNICO")).orElse("N");
		
		String tipoEmpresa = "";
		
		if(profissionalizante.equalsIgnoreCase("S")){
			tipoEmpresa = "P";
		}else if(tecnico.equalsIgnoreCase("S")){
			tipoEmpresa = "T";
		}else{
			tipoEmpresa = "N";
		}
		
		
		String dataInicio = contexto.getParam("DTINICIO").toString().substring(0, 10);
		String dataFim = contexto.getParam("DTFIM").toString().substring(0, 10);
		String tituloAberto = (String) contexto.getParam("TITABERTO");
		String matricula = (String) contexto.getParam("Matricula");
		
		try {

			// Alunos
			List<Object[]> listInfAlunos = retornarInformacoesAlunos();
			Map<String, BigDecimal> mapaInfAlunos = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfAlunos) {
				BigDecimal codParc = (BigDecimal) obj[0];
				String idExternoObj = (String) obj[1];
				BigDecimal codemp = (BigDecimal) obj[3];

				if (mapaInfAlunos.get(idExternoObj + "###" + codemp) == null) {
					mapaInfAlunos.put(idExternoObj + "###" + codemp, codParc);
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

			// Nro Banco
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco = new HashMap<BigDecimal, BigDecimal>();
			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				BigDecimal nuBco = (BigDecimal) obj[5];
				if (mapaInfFinanceiroBanco.get(nuFin) == null) {
					mapaInfFinanceiroBanco.put(nuFin, nuBco);
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

			// Parceiros
			List<Object[]> listInfParceiro = retornarInformacoesParceiros();
			Map<String, BigDecimal> mapaInfParceiros = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfParceiro) {
				BigDecimal codParc = (BigDecimal) obj[0];
				String cpf_cnpj = (String) obj[1];

				if (mapaInfParceiros.get(cpf_cnpj) == null) {
					mapaInfParceiros.put(cpf_cnpj, codParc);
				}
			}

			// CenCus
			List<Object[]> listInfCenCus = retornarInformacoesCenCus();
			Map<String, BigDecimal> mapaInfCenCus = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfCenCus) {
				BigDecimal codCenCus = (BigDecimal) obj[0];
				String idExterno = (String) obj[1];
				String flag = (String) obj[2];

				if (mapaInfCenCus.get(idExterno + "###" + flag) == null) {
					mapaInfCenCus.put(idExterno + "###" + flag, codCenCus);
				}
			}
			
			//CenCus por empresa
			List<Object[]> listInfCenCusEmpresa = retornarInformacoesCenCusEmpresa();
			Map<String, BigDecimal> mapaInfCenCusEmp = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfCenCusEmpresa) {
				BigDecimal codCenCus = (BigDecimal) obj[0];
				String idExterno = (String) obj[1];
				BigDecimal codemp = (BigDecimal) obj[2];
				
				if (mapaInfCenCusEmp.get(idExterno + "###" + codemp) == null) {
					mapaInfCenCusEmp.put(idExterno + "###" + codemp, codCenCus);
				}
			}

			// CenCus Pelo Aluno
			Map<String, BigDecimal> mapaInfCenCusAluno = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfAlunos) {
				String idExternoObj = (String) obj[1];
				BigDecimal codCenCus = (BigDecimal) obj[2];

				if (mapaInfCenCusAluno.get(idExternoObj) == null) {
					mapaInfCenCusAluno.put(idExternoObj, codCenCus);
				}
			}

			// Natureza
			List<Object[]> listInfNatureza = retornarInformacoesNatureza();
			Map<String, BigDecimal> mapaInfNatureza = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfNatureza) {
				BigDecimal natureza = (BigDecimal) obj[0];
				String idExternoObj = (String) obj[1];
				String flag = (String) obj[2];

				if (mapaInfNatureza.get(idExternoObj + "###" + flag) == null) {
					mapaInfNatureza.put(idExternoObj + "###" + flag, natureza);
				}
			}
			
			//Natureza por empresa
			List<Object[]> listInfNaturezaEmpresa = retornarInformacoesNaturezaEmpresa();
			Map<String, BigDecimal> mapaInfNaturezaEmp = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfNaturezaEmpresa) {
				BigDecimal natureza = (BigDecimal) obj[0];
				String idExternoObj = (String) obj[1];
				BigDecimal codemp = (BigDecimal) obj[2];
				
				if (mapaInfNaturezaEmp.get(idExternoObj +"###"+ codemp) == null) {
					mapaInfNaturezaEmp.put(idExternoObj +"###"+ codemp, natureza);
				}
			}

			// RecDesp
			List<Object[]> listInfRecDesp = retornarInformacoesRecDesp();
			Map<String, String> mapaInfRecDesp = new HashMap<String, String>();
			for (Object[] obj : listInfRecDesp) {
				String recDesp = (String) obj[0];
				String idExternoObj = (String) obj[1];

				if (mapaInfRecDesp.get(idExternoObj) == null) {
					mapaInfRecDesp.put(idExternoObj, recDesp);
				}
			}

			// Banco
			List<Object[]> listInfBancoConta = retornarInformacoesBancoConta();
			Map<String, BigDecimal> mapaInfBanco = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfBancoConta) {
				Long codEmpObj = (Long) obj[1];
				BigDecimal codBcoObj = (BigDecimal) obj[3];

				if (mapaInfBanco.get(codEmpObj.toString()) == null) {
					mapaInfBanco.put(codEmpObj.toString(), codBcoObj);
				}
			}

			// Conta
			Map<String, BigDecimal> mapaInfConta = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfBancoConta) {
				BigDecimal codCtabCointObj = (BigDecimal) obj[0];
				Long codEmpObj = (Long) obj[1];

				if (mapaInfConta.get(codEmpObj.toString()) == null) {
					mapaInfConta.put(codEmpObj.toString(), codCtabCointObj);
				}
			}
			
			processDateRange(tituloAberto.trim(),tipoEmpresa, mapaInfNaturezaEmp, mapaInfCenCusEmp, 
					mapaInfFinanceiroBaixado, mapaInfFinanceiroBanco, mapaInfFinanceiro, mapaInfRecDesp, mapaInfConta,
					mapaInfBanco, mapaInfNatureza, mapaInfCenCus,
					mapaInfCenCusAluno, mapaInfAlunos, url, token, codEmp,
					dataInicio, dataFim, matricula);
			
			contexto.setMensagemRetorno("Periodo Processado!");
			
		}catch(Exception e){
			e.printStackTrace();
			contexto.mostraErro(e.getMessage());
		}finally{

			if(selectsParaInsertLog.size() > 0){
				
				StringBuilder msgError = new StringBuilder();
				
				System.out.println("Entrou na lista do finally: " + selectsParaInsertLog.size());
				
				//int idInicial = util.getMaxNumLog();
				
				int qtdInsert = selectsParaInsertLog.size();
				
				int i = 1;
				for (String sqlInsert : selectsParaInsertLog) {
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

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal codEmp = BigDecimal.ZERO;
		BigDecimal idCarga = BigDecimal.ZERO;

		String url = "";
		String token = "";
		String matricula = "";
		String tipoEmpresa = "";

		int count = 0;

		System.out.println("Iniciou o financeiro dos alunos Empresas");

		try {
			
			// Alunos
			List<Object[]> listInfAlunos = retornarInformacoesAlunos();
			Map<String, BigDecimal> mapaInfAlunos = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfAlunos) {
				BigDecimal codParc = (BigDecimal) obj[0];
				String idExternoObj = (String) obj[1];
				BigDecimal codemp = (BigDecimal) obj[3];

				if (mapaInfAlunos.get(idExternoObj + "###" + codemp) == null) {
					mapaInfAlunos.put(idExternoObj + "###" + codemp, codParc);
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

			// Nro Banco
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco = new HashMap<BigDecimal, BigDecimal>();
			for (Object[] obj : listInfFinanceiro) {
				BigDecimal nuFin = (BigDecimal) obj[0];
				BigDecimal nuBco = (BigDecimal) obj[5];
				if (mapaInfFinanceiroBanco.get(nuFin) == null) {
					mapaInfFinanceiroBanco.put(nuFin, nuBco);
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

			// Parceiros
			List<Object[]> listInfParceiro = retornarInformacoesParceiros();
			Map<String, BigDecimal> mapaInfParceiros = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfParceiro) {
				BigDecimal codParc = (BigDecimal) obj[0];
				String cpf_cnpj = (String) obj[1];

				if (mapaInfParceiros.get(cpf_cnpj) == null) {
					mapaInfParceiros.put(cpf_cnpj, codParc);
				}
			}

			// CenCus
			List<Object[]> listInfCenCus = retornarInformacoesCenCus();
			Map<String, BigDecimal> mapaInfCenCus = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfCenCus) {
				BigDecimal codCenCus = (BigDecimal) obj[0];
				String idExterno = (String) obj[1];
				String flag = (String) obj[2];

				if (mapaInfCenCus.get(idExterno + "###" + flag) == null) {
					mapaInfCenCus.put(idExterno + "###" + flag, codCenCus);
				}
			}

			// CenCus por empresa
			List<Object[]> listInfCenCusEmpresa = retornarInformacoesCenCusEmpresa();
			Map<String, BigDecimal> mapaInfCenCusEmp = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfCenCusEmpresa) {
				BigDecimal codCenCus = (BigDecimal) obj[0];
				String idExterno = (String) obj[1];
				BigDecimal codemp = (BigDecimal) obj[2];

				if (mapaInfCenCusEmp.get(idExterno + "###" + codemp) == null) {
					mapaInfCenCusEmp.put(idExterno + "###" + codemp, codCenCus);
				}
			}

			// CenCus Pelo Aluno
			Map<String, BigDecimal> mapaInfCenCusAluno = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfAlunos) {
				String idExternoObj = (String) obj[1];
				BigDecimal codCenCus = (BigDecimal) obj[2];

				if (mapaInfCenCusAluno.get(idExternoObj) == null) {
					mapaInfCenCusAluno.put(idExternoObj, codCenCus);
				}
			}

			// Natureza
			List<Object[]> listInfNatureza = retornarInformacoesNatureza();
			Map<String, BigDecimal> mapaInfNatureza = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfNatureza) {
				BigDecimal natureza = (BigDecimal) obj[0];
				String idExternoObj = (String) obj[1];
				String flag = (String) obj[2];

				if (mapaInfNatureza.get(idExternoObj + "###" + flag) == null) {
					mapaInfNatureza.put(idExternoObj + "###" + flag, natureza);
				}
			}

			// Natureza por empresa
			List<Object[]> listInfNaturezaEmpresa = retornarInformacoesNaturezaEmpresa();
			Map<String, BigDecimal> mapaInfNaturezaEmp = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfNaturezaEmpresa) {
				BigDecimal natureza = (BigDecimal) obj[0];
				String idExternoObj = (String) obj[1];
				BigDecimal codemp = (BigDecimal) obj[2];

				if (mapaInfNaturezaEmp.get(idExternoObj + "###" + codemp) == null) {
					mapaInfNaturezaEmp.put(idExternoObj + "###" + codemp,
							natureza);
				}
			}

			// RecDesp
			List<Object[]> listInfRecDesp = retornarInformacoesRecDesp();
			Map<String, String> mapaInfRecDesp = new HashMap<String, String>();
			for (Object[] obj : listInfRecDesp) {
				String recDesp = (String) obj[0];
				String idExternoObj = (String) obj[1];

				if (mapaInfRecDesp.get(idExternoObj) == null) {
					mapaInfRecDesp.put(idExternoObj, recDesp);
				}
			}

			// Banco
			List<Object[]> listInfBancoConta = retornarInformacoesBancoConta();
			Map<String, BigDecimal> mapaInfBanco = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfBancoConta) {
				Long codEmpObj = (Long) obj[1];
				BigDecimal codBcoObj = (BigDecimal) obj[3];

				if (mapaInfBanco.get(codEmpObj.toString()) == null) {
					mapaInfBanco.put(codEmpObj.toString(), codBcoObj);
				}
			}

			// Conta
			Map<String, BigDecimal> mapaInfConta = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfBancoConta) {
				BigDecimal codCtabCointObj = (BigDecimal) obj[0];
				Long codEmpObj = (Long) obj[1];

				if (mapaInfConta.get(codEmpObj.toString()) == null) {
					mapaInfConta.put(codEmpObj.toString(), codCtabCointObj);
				}
			}

			jdbc.openSession();
			
			String query = "SELECT CODEMP, URL, TOKEN, INTEGRACAO,"
					+ "CASE WHEN PROFISSIONAL = 'S' THEN 'P' WHEN TECNICO = 'S' THEN 'T' ELSE 'N' END AS TIPEMP "
					+ "FROM AD_LINKSINTEGRACAO";
			
			pstmt = jdbc.getPreparedStatement(query);

			rs = pstmt.executeQuery();   

			while (rs.next()) {
				count++;
				codEmp = rs.getBigDecimal("CODEMP");
				
				url = rs.getString("URL");
				token = rs.getString("TOKEN");
				tipoEmpresa = rs.getString("TIPEMP");
				String statusIntegracao = rs.getString("INTEGRACAO");
				
				// Verifica se a integração está ativa para esta empresa
				if (!"S".equals(statusIntegracao)) {
					System.out.println("Integração desativada para a empresa " + codEmp + " - pulando processamento");
					continue; // Pula para a próxima iteração do loop
				}

				iterarEndpoint(tipoEmpresa.trim(), mapaInfNaturezaEmp, mapaInfCenCusEmp, 
						mapaInfFinanceiroBaixado, mapaInfFinanceiroBanco, mapaInfFinanceiro, mapaInfRecDesp, mapaInfConta,
						mapaInfBanco, mapaInfNatureza, mapaInfCenCus,
						mapaInfCenCusAluno, mapaInfAlunos, url, token, codEmp);

			}

			System.out
					.println("Finalizou o financeiro dos alunos Empresas");

		} catch (Exception e) {
			e.printStackTrace();
			try {
				
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
			

			if(selectsParaInsertLog.size() > 0){
				
				StringBuilder msgError = new StringBuilder();
				
				System.out.println("Entrou na lista do finally: " + selectsParaInsertLog.size());
				
				//int idInicial = util.getMaxNumLog();
				
				int qtdInsert = selectsParaInsertLog.size();
				
				int i = 1;
				for (String sqlInsert : selectsParaInsertLog) {
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
					insertLogList(msgError.toString(), codEmp);
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				msgError = null;
				this.selectsParaInsertLog = new ArrayList<String>();
				
			}
		
		
		}
	}

	
	//alteracao feita aqui 
//	public void processDateRange1(
//	        String tituloAberto,
//	        String tipoEmpresa,
//	        Map<String, BigDecimal> mapaInfNaturezaEmp,
//	        Map<String, BigDecimal> mapaInfCenCusEmp,
//	        Map<BigDecimal, String> mapaInfFinanceiroBaixado,
//	        Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
//	        Map<String, BigDecimal> mapaInfFinanceiro,
//	        Map<String, String> mapaInfRecDesp,
//	        Map<String, BigDecimal> mapaInfConta,
//	        Map<String, BigDecimal> mapaInfBanco,
//	        Map<String, BigDecimal> mapaInfNatureza,
//	        Map<String, BigDecimal> mapaInfCenCus,
//	        Map<String, BigDecimal> mapaInfCenCusAluno,
//	        Map<String, BigDecimal> mapaInfAlunos,
//	        String url,
//	        String token,
//	        BigDecimal codEmp,
//	        String dataInicio,
//	        String dataFim,
//	        String matricula) throws Exception {
//
//	    try {
//	        // Converter as datas para LocalDate
//	        LocalDate startDate = LocalDate.parse(dataInicio);
//	        LocalDate endDate = LocalDate.parse(dataFim);
//
//	        // Iterar por cada mês no intervalo (ajuste para semana/dia conforme necessário)
//	        LocalDate currentDate = startDate;
//            //while (!currentDate.isAfter(endDate)) {
//
//            // Definir o fim do subperíodo (ex: fim do mês)
//            LocalDate subEndDate = endDate.withDayOfMonth(endDate.lengthOfMonth());
//
//            // Ajustar subEndDate para não ultrapassar a data final original
//            if (subEndDate.isAfter(endDate)) {
//                subEndDate = endDate;
//            }
//
//            // datas com espaços
//            String dataInicialParam = URLEncoder.encode(currentDate.toString() + " 00:00:00", "UTF-8");
//            String dataFinalParam = URLEncoder.encode(subEndDate.toString() + " 23:59:59", "UTF-8");
//            
//
//            // Construir a URL para o subperíodo
//            StringBuilder urlBuilder = new StringBuilder(url)
//                    .append("/financeiro/titulos?")
//                   // .append("quantidade=0")
//                    .append("&dataInicial=").append(dataInicialParam)
//                    .append("&dataFinal=").append(dataFinalParam);
//
//            // Adicionar parâmetros condicionais
//            if (tituloAberto.equalsIgnoreCase("S")) {
//                urlBuilder.append("&situacao=A");
//            }
//            
//            System.out.println("OI TO NA MATRICULA" + matricula);
//            
//            if (matricula != null && !matricula.trim().isEmpty()) {
//            
//                String matriculaEncoded = URLEncoder.encode(matricula.trim(), "UTF-8");
//                urlBuilder.append("&matricula=").append(matriculaEncoded);
//            }
//
//            // Fazer a requisição para o subperíodo
//            String[] response = apiGet(urlBuilder.toString(), token);
//            int status = Integer.parseInt(response[0]);
//            System.out.println("Status para " + currentDate + " a " + subEndDate + ": " + status);
//
//            // Processar a resposta
//            cadastrarFinanceiro(
//                    tipoEmpresa,
//                    mapaInfNaturezaEmp,
//                    mapaInfCenCusEmp,
//                    mapaInfFinanceiroBaixado,
//                    mapaInfFinanceiroBanco,
//                    mapaInfFinanceiro,
//                    mapaInfRecDesp,
//                    mapaInfConta,
//                    mapaInfBanco,
//                    mapaInfNatureza,
//                    mapaInfCenCus,
//                    mapaInfCenCusAluno,
//                    mapaInfAlunos,
//                    response,
//                    url,
//                    token,
//                    codEmp
//            );
//
//            // Avançar para o próximo subperíodo
//            //currentDate = subEndDate.plusDays(1); // Para intervalos diários, use plusDays(1)
//       // }
//
//	    } catch (Exception e) {
//	        System.err.println("Erro ao processar requisições: " + e.getMessage());
//	        throw e;
//	    }
//	}
	
	public void processDateRange(
	        String tituloAberto,
	        String tipoEmpresa,
	        Map<String, BigDecimal> mapaInfNaturezaEmp,
	        Map<String, BigDecimal> mapaInfCenCusEmp,
	        Map<BigDecimal, String> mapaInfFinanceiroBaixado,
	        Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
	        Map<String, BigDecimal> mapaInfFinanceiro,
	        Map<String, String> mapaInfRecDesp,
	        Map<String, BigDecimal> mapaInfConta,
	        Map<String, BigDecimal> mapaInfBanco,
	        Map<String, BigDecimal> mapaInfNatureza,
	        Map<String, BigDecimal> mapaInfCenCus,
	        Map<String, BigDecimal> mapaInfCenCusAluno,
	        Map<String, BigDecimal> mapaInfAlunos,
	        String url,
	        String token,
	        BigDecimal codEmp,
	        String dataInicio,
	        String dataFim,
	        String matricula) throws Exception {

	    try {
	        // Preparar as datas (mantendo o formato simples para minimizar conflitos)
	        String dataInicialCompleta = dataInicio + " 00:00:00";
	        String dataFinalCompleta = dataFim + " 23:59:59";
	        
	        String dataInicialParam = URLEncoder.encode(dataInicialCompleta, "UTF-8");
	        String dataFinalParam = URLEncoder.encode(dataFinalCompleta, "UTF-8");

	        // Lista para armazenar todos os registros
	        JSONArray todosRegistros = new JSONArray();
	        int pagina = 1;
	        boolean temMaisRegistros = true;

	        while (temMaisRegistros) {
	            StringBuilder urlBuilder = new StringBuilder(url.trim())
	                    .append("/financeiro/titulos?")
	                    .append("pagina=").append(pagina)
	                    .append("&quantidade=100")
	                    .append("&dataInicial=").append(dataInicialParam)
	                    .append("&dataFinal=").append(dataFinalParam);

	            if (tituloAberto != null && tituloAberto.equalsIgnoreCase("S")) {
	                urlBuilder.append("&situacao=A");
	            }

	            if (matricula != null && !matricula.trim().isEmpty()) {
	                String matriculaEncoded = URLEncoder.encode(matricula.trim(), "UTF-8");
	                urlBuilder.append("&matricula=").append(matriculaEncoded);
	            }
	            
	            
                //URL Financeiro + pagina atual
	            String urlCompleta = urlBuilder.toString();
	            System.out.println("URL para financeiro (página " + pagina + "): " + urlCompleta);

	            // Fazer a requisição
	            String[] response = apiGet2(urlCompleta, token);
	            int status = Integer.parseInt(response[0]);

	            if (status == 200) {
	                JSONArray paginaAtual = new JSONArray(response[1]);
	                
	                for (int i = 0; i < paginaAtual.length(); i++) {
	                    todosRegistros.put(paginaAtual.getJSONObject(i));
	                }
	                
	                if (paginaAtual.length() < 100) {
	                    temMaisRegistros = false;
	                } else {
	                    pagina++;
	                }
	                
	                System.out.println("Página " + pagina + ": " + paginaAtual.length() + 
	                                  " registros. Total acumulado: " + todosRegistros.length());
	            } else {
	                throw new Exception(String.format(
	                    "Erro na requisição de financeiro. Status: %d. Resposta: %s. URL: %s",
	                    status, response[1], urlCompleta
	                ));
	            }
	        }
	        
	        // Criar resposta combinada
	        String[] respostaCombinada = new String[] {
	            "200",
	            todosRegistros.toString()
	        };
	        
	        System.out.println("Total de registros acumulados: " + todosRegistros.length());
	        
	        cadastrarFinanceiro(
	                tipoEmpresa,
	                mapaInfNaturezaEmp,
	                mapaInfCenCusEmp,
	                mapaInfFinanceiroBaixado,
	                mapaInfFinanceiroBanco,
	                mapaInfFinanceiro,
	                mapaInfRecDesp,
	                mapaInfConta,
	                mapaInfBanco,
	                mapaInfNatureza,
	                mapaInfCenCus,
	                mapaInfCenCusAluno,
	                mapaInfAlunos,
	                respostaCombinada,
	                url,
	                token,
	                codEmp
	        );

	    } catch (Exception e) {
	        System.err.println("Erro ao processar requisição financeira para período " +
	                dataInicio + " até " + dataFim + ": " + e.getMessage());
	        throw e;
	    }
	}
	
	public void iterarEndpoint(String tipoEmpresa,
		    Map<String, BigDecimal> mapaInfNaturezaEmp,
		    Map<String, BigDecimal> mapaInfCenCusEmp,
		    Map<BigDecimal, String> mapaInfFinanceiroBaixado,
		    Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
		    Map<String, BigDecimal> mapaInfFinanceiro,
		    Map<String, String> mapaInfRecDesp,
		    Map<String, BigDecimal> mapaInfConta,
		    Map<String, BigDecimal> mapaInfBanco,
		    Map<String, BigDecimal> mapaInfNatureza,
		    Map<String, BigDecimal> mapaInfCenCus,
		    Map<String, BigDecimal> mapaInfCenCusAluno,
		    Map<String, BigDecimal> mapaInfAlunos, String url, String token,
		    BigDecimal codEmp) throws Exception {

		    System.out.println("=== iterarEndpoint do JOB iniciado ===");
		    System.out.println("tipoEmpresa: " + tipoEmpresa);
		    System.out.println("codEmp: " + codEmp);
		    System.out.println("URL base: " + url);
		    
		    Date dataAtual = new Date();
		    SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");
		    String dataFormatada = formato.format(dataAtual);
		    
		    System.out.println("Data formatada: " + dataFormatada);
		    
		    try {
		        System.out.println("While de iteração");
		        
		        String endpointCompleto = url + "/financeiro" + "/titulos?" 
		            + "quantidade=0" + "&dataInicial=" + dataFormatada 
		            + " 00:00:00&dataFinal=" + dataFormatada + " 23:59:59";
		        
		        System.out.println("Chamando endpoint: " + endpointCompleto);
		        
		        String[] response = apiGet2(endpointCompleto, token);
		        
		        int status = Integer.parseInt(response[0]);
		        System.out.println("Status da resposta: " + status);
		        
		        String responseString = response[1];
		        System.out.println("[API] Resposta recebida, tamanho: " + responseString.length());
		        System.out.println("[API] Primeiros 100 caracteres: " + 
		            (responseString.length() > 100 ? responseString.substring(0, 100) + "..." : responseString));
		        
		        System.out.println("Antes de chamar cadastrarFinanceiro");
		        cadastrarFinanceiro(tipoEmpresa, mapaInfNaturezaEmp,
		            mapaInfCenCusEmp, mapaInfFinanceiroBaixado,
		            mapaInfFinanceiroBanco, mapaInfFinanceiro, mapaInfRecDesp,
		            mapaInfConta, mapaInfBanco, mapaInfNatureza, mapaInfCenCus,
		            mapaInfCenCusAluno, mapaInfAlunos, response, url, token,
		            codEmp);
		        System.out.println("Após chamar cadastrarFinanceiro");
		    }
		    catch (Exception e) {
		        System.out.println("ERRO em iterarEndpoint: " + e.getMessage());
		        e.printStackTrace();
		    }
		    finally {
		        System.out.println("=== iterarEndpoint do JOB finalizado ===");
		    }
		}
	
	
	public void cadastrarFinanceiro(String tipoEmpresa,
	        Map<String, BigDecimal> mapaInfNaturezaEmp,
	        Map<String, BigDecimal> mapaInfCenCusEmp,
	        Map<BigDecimal, String> mapaInfFinanceiroBaixado,
	        Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
	        Map<String, BigDecimal> mapaInfFinanceiro,
	        Map<String, String> mapaInfRecDesp,
	        Map<String, BigDecimal> mapaInfConta,
	        Map<String, BigDecimal> mapaInfBanco,
	        Map<String, BigDecimal> mapaInfNatureza,
	        Map<String, BigDecimal> mapaInfCenCus,
	        Map<String, BigDecimal> mapaInfCenCusAluno,
	        Map<String, BigDecimal> mapaInfAlunos, String[] respostaCombinada,
	        String url, String token, BigDecimal codemp)
	        throws Exception {
	    
	    System.out.println("===== INÍCIO cadastrarFinanceiro =====");
	    System.out.println("tipoEmpresa: " + tipoEmpresa);
	    System.out.println("codemp: " + codemp);
	    
	    EnviromentUtils util = new EnviromentUtils();
	    
	    SimpleDateFormat formatoEntrada = new SimpleDateFormat(
	            "yyyy-MM-dd HH:mm:ss.SSS");
	    SimpleDateFormat formatoOriginal = new SimpleDateFormat("yyyy-MM-dd");
	    SimpleDateFormat formatoDesejado = new SimpleDateFormat("dd/MM/yyyy");

	    System.out.println("Entrou no job");

	    Date dataAtual = new Date();
	    SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");
	    String dataFormatada = formato.format(dataAtual);
	    System.out.println("Data atual formatada: " + dataFormatada);

	    StringBuilder consulta = new StringBuilder();
	    String idAluno = "";
	    
	    try {
	        String responseString = respostaCombinada[1];
	        String responseStatus = respostaCombinada[0];
	        
	        System.out.println("Status da resposta: " + responseStatus);
	        System.out.println("Tamanho da resposta: " + (responseString != null ? responseString.length() : "null"));
	        
	        if (responseStatus.equalsIgnoreCase("200")) {
	            System.out.println("Resposta com status 200 - OK");
	            
	            JsonParser parser = new JsonParser();
	            JsonArray jsonArray = parser.parse(responseString).getAsJsonArray();
	            int count = 0;
	            int total = jsonArray.size();
	            int qtdInsert = 0;

	            System.out.println("Total de registros no JSON: " + total);

	            List<String> selectsParaInsert = new ArrayList<String>();

	            for (JsonElement jsonElement : jsonArray) {
	                System.out.println("\n----- Processando registro " + (count + 1) + " de " + total + " -----");
	                
	                JsonObject jsonObject = jsonElement.getAsJsonObject();

	                String idFin = jsonObject.get("titulo_id").getAsString();
	                System.out.println("ID Financeiro: " + idFin);

	                BigDecimal vlrDesdob = new BigDecimal(jsonObject.get(
	                        "titulo_valor").getAsDouble());
	                System.out.println("Valor: " + vlrDesdob);

	                String dtVenc = jsonObject.get("titulo_vencimento").getAsString();
	                System.out.println("Data vencimento original: " + dtVenc);

	                String idCurso = jsonObject.get("curso_id").isJsonNull() 
	                                ? "" 
	                                : jsonObject.get("curso_id").getAsString();
	                System.out.println("ID Curso: " + (idCurso.isEmpty() ? "vazio" : idCurso));

	                String taxaId = jsonObject.get("taxa_id").getAsString();
	                System.out.println("Taxa ID: " + taxaId);

	                String dtPedidoOrig = jsonObject.get("data_atualizacao").getAsString();
	                System.out.println("Data pedido original: " + dtPedidoOrig);

	                Date dataPedido = formatoEntrada.parse(dtPedidoOrig);
	                String dtPedido = formatoDesejado.format(dataPedido);
	                System.out.println("Data Pedido formatada: " + dtPedido);

	                Date data = formatoOriginal.parse(dtVenc);
	                String dataVencFormatada = formatoDesejado.format(data);
	                System.out.println("Data vencimento formatada: " + dataVencFormatada);

	                idAluno = jsonObject.get("aluno_id").getAsString();
	                System.out.println("ID Aluno: " + idAluno);
	                
	                final String idAlunoF = idAluno;

	                BigDecimal codparc = Optional.ofNullable(
	                        mapaInfAlunos.get(idAluno + "###" + codemp)).orElse(BigDecimal.ZERO);
	                System.out.println("Código parceiro: " + codparc + (codparc.compareTo(BigDecimal.ZERO) == 0 ? " (NÃO ENCONTRADO)" : ""));

	                String situacao_titulo = jsonObject.get("titulo_situacao").getAsString();
	                System.out.println("Situação título: " + situacao_titulo);
	                
	                if(!situacao_titulo.equalsIgnoreCase("X")){
	                    System.out.println("Processando título não cancelado");
	                    
	                    if (vlrDesdob.compareTo(new BigDecimal("5")) > 0
	                            && codparc.compareTo(BigDecimal.ZERO) != 0) {
	                        System.out.println("Valor > 5 e parceiro encontrado, continuando processamento");
	                        
	                        String chaveCenCus = taxaId + "###" + tipoEmpresa;
	                        System.out.println("Chave para busca Centro Custo: " + chaveCenCus);
	                        
	                        BigDecimal codCenCus = Optional.ofNullable(mapaInfCenCus.get(chaveCenCus))
	                            .orElseGet(() -> {
	                                System.out.println("Centro Custo não encontrado pela taxa, buscando pelo aluno");
	                                return Optional.ofNullable(mapaInfCenCusAluno.get(idAlunoF))
	                                    .orElse(BigDecimal.ZERO);
	                            });

	                        System.out.println("CodCenCus: " + codCenCus + (codCenCus.compareTo(BigDecimal.ZERO) == 0 ? " (NÃO ENCONTRADO)" : ""));
	                        
	                        if (validarDataLimite(dtPedido)) {
	                            System.out.println("Data limite validada com sucesso");
	                            
	                            if(codCenCus != null && codCenCus.compareTo(BigDecimal.ZERO) != 0){
	                                System.out.println("Centro Custo válido");
	                                
	                                if (codparc.compareTo(BigDecimal.ZERO) != 0) {
	                                    System.out.println("Entrou no parceiro: " + codparc);

	                                    String chaveFinanceiro = codemp + "###" + idFin;
	                                    System.out.println("Chave para busca Financeiro: " + chaveFinanceiro);
	                                    
	                                    BigDecimal validarNufin = Optional.ofNullable(
	                                            mapaInfFinanceiro.get(chaveFinanceiro)).orElse(
	                                            BigDecimal.ZERO);
	                                    System.out.println("NUFIN: " + validarNufin + (validarNufin.compareTo(BigDecimal.ZERO) == 0 ? " (NÃO ENCONTRADO - OK)" : " (JÁ EXISTE)"));

	                                    if (validarNufin.compareTo(BigDecimal.ZERO) == 0) {
	                                        System.out.println("Entrou no financeiro - título ainda não cadastrado");

	                                        BigDecimal codConta = mapaInfConta.get(codemp.toString());
	                                        System.out.println("Código Conta: " + codConta);

	                                        BigDecimal codBanco = mapaInfBanco.get(codemp.toString());
	                                        System.out.println("Código Banco: " + codBanco);

	                                        String recDesp = mapaInfRecDesp.get(taxaId);
	                                        System.out.println("Rec/Desp: " + recDesp);
	                                        
	                                        String chaveNatureza = taxaId + "###" + tipoEmpresa;
	                                        System.out.println("Chave para busca Natureza: " + chaveNatureza);
	                                        
	                                        BigDecimal natureza = Optional.ofNullable(mapaInfNatureza.get(chaveNatureza))
	                                                .orElse(BigDecimal.ZERO);
	                                        System.out.println("Natureza: " + natureza + (natureza.compareTo(BigDecimal.ZERO) == 0 ? " (NÃO ENCONTRADA)" : ""));
	                                        
	                                        if (recDesp != null && !recDesp.isEmpty() && natureza.compareTo(BigDecimal.ZERO) != 0) {
	                                            System.out.println("RecDesp e Natureza válidos, montando SQL");

	                                            String sqlInsert = " SELECT <#NUFIN#>, NULL, 0, 'F', "
	                                                    + recDesp
	                                                    + ", "
	                                                    + codemp
	                                                    + " , "
	                                                    + codCenCus
	                                                    + " , "
	                                                    + natureza
	                                                    + " ,  "
	                                                    + BigDecimal.valueOf(1300)
	                                                    + " ,  (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = "
	                                                    + BigDecimal.valueOf(1300)
	                                                    + "), 0, (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 0), "
	                                                    + codparc
	                                                    + " , "
	                                                    + BigDecimal.valueOf(4)
	                                                    + " , "
	                                                    + vlrDesdob
	                                                    + " , 0, 0, "
	                                                    + codBanco
	                                                    + ", "
	                                                    + codConta
	                                                    + ", '"
	                                                    + dtPedido
	                                                    + "' , SYSDATE, SYSDATE, '"
	                                                    + dataVencFormatada
	                                                    + "', SYSDATE, '"
	                                                    + dataVencFormatada
	                                                    + "' , 1 , 1 , null , 'I' , 'N' , 'N' , 'N' , 'N' , 'N' , 'N' , 'N' , 'S' , 'S' , 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, '"
	                                                    + idFin
	                                                    + "', '"
	                                                    + idAluno
	                                                    + "' FROM DUAL ";

	                                            System.out.println("SQL Insert para TGFFIN criado com sucesso");
	                                            
	                                            consulta.append(sqlInsert);
	                                            selectsParaInsert.add(sqlInsert);
	                                            qtdInsert++;
	                                            System.out.println("Quantidade de inserts acumulados: " + qtdInsert);

	                                            if (count < total - 1) {
	                                                consulta.append("\nUNION ALL");
	                                            }

	                                            System.out.println("Financeiro cadastrado com sucesso no array de inserts");
	                                        } else {
	                                            System.out.println("ATENÇÃO: RecDesp ou Natureza inválidos - adicionando log");
	                                            selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Sem \"de para\" para a Taxa ID: "+ taxaId+"', SYSDATE, 'Aviso', "+codemp+", '' FROM DUAL");
	                                        }
	                                    } else {
	                                        System.out.println("AVISO: Financeiro " + idFin + " já cadastrado para o parceiro: " + codparc);
	                                    }
	                                } else {
	                                    System.out.println("AVISO: Aluno com Id: " + idAluno + " não encontrado");
	                                }
	                            } else {
	                                System.out.println("ATENÇÃO: Centro de Custo inválido - adicionando log");
	                                selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Sem \"de para\" para a Taxa ID: "+ taxaId+"', SYSDATE, 'Aviso', "+codemp+", '' FROM DUAL");
	                            }
	                        } else {
	                            System.out.println("ATENÇÃO: Data pedido inválida - adicionando log");
	                            selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Data pedido inferior a data limite, Data Pedido: "+dtPedido+"', SYSDATE, 'Aviso', "+codemp+", '' FROM DUAL");
	                        }
	                    } else {
	                        System.out.println("AVISO: Valor <= 5 ou parceiro não encontrado - ignorando registro");
	                    }
	                } else if(situacao_titulo.equalsIgnoreCase("X")){
	                    System.out.println("Título cancelado - processando exclusão");
	                    
	                    BigDecimal validarNufin = Optional.ofNullable(
	                            mapaInfFinanceiro.get(codemp + "###" + idFin)).orElse(
	                            BigDecimal.ZERO);
	                    System.out.println("NUFIN para exclusão: " + validarNufin + (validarNufin.compareTo(BigDecimal.ZERO) == 0 ? " (NÃO ENCONTRADO)" : ""));
	                    
	                    if(validarNufin.compareTo(BigDecimal.ZERO) != 0){
	                        System.out.println("Financeiro encontrado para exclusão");
	                        
	                        if ("S".equalsIgnoreCase(mapaInfFinanceiroBaixado.get(validarNufin))) {
	                            System.out.println("Financeiro baixado - processando estorno");

	                            BigDecimal nubco = mapaInfFinanceiroBanco.get(validarNufin);
	                            System.out.println("NUBCO: " + nubco);
	                            
	                            System.out.println("Executando updateFinExtorno para NUFIN: " + validarNufin);
	                            updateFinExtorno(validarNufin, codemp);
	                            
	                            System.out.println("Executando deleteTgfMbc para NUBCO: " + nubco);
	                            deleteTgfMbc(nubco, codemp);
	                            
	                            System.out.println("Executando deleteTgfFin para NUFIN: " + validarNufin);
	                            deleteTgfFin(validarNufin, codemp);
	                        } else {
	                            System.out.println("Financeiro não baixado - excluindo diretamente");
	                            deleteTgfFin(validarNufin, codemp);
	                        }
	                    } else {
	                        System.out.println("Financeiro não encontrado para exclusão - ignorando");
	                    }
	                }
	                
	                count++;
	                System.out.println("----- Registro " + count + " processado -----");
	            }

	            System.out.println("\nTotal de registros processados: " + count);
	            System.out.println("Total de inserts a realizar: " + qtdInsert);
	            System.out.println("Consulta Antes tratamento: " + consulta);

	            // Apenas se encontrar registro elegivel
	            if (qtdInsert > 0) {
	                System.out.println("Iniciando processamento de inserts");
	                
	                // Capturar o tgfnum
	                BigDecimal nuFinInicial = util.getMaxNumFin(false);
	                System.out.println("NUFIN inicial obtido: " + nuFinInicial);

	                // Atualizar o nufin adicionando a quantidade de lista
	                System.out.println("Atualizando NUFIN com incremento de: " + qtdInsert);
	                util.updateNumFinByQtd(qtdInsert);
	                
	                // remontar a lista para inserir
	                StringBuilder sqlInsertFin = new StringBuilder();
	                int i = 1;
	                for (String sqlInsert : selectsParaInsert) {
	                    String sql = sqlInsert;
	                    int nuFin = nuFinInicial.intValue() + i;
	                    sql = sql.replace("<#NUFIN#>", String.valueOf(nuFin));
	                    sqlInsertFin.append(sql);
	                    System.out.println("SQL #" + i + " - NUFIN: " + nuFin);

	                    if (i < qtdInsert) {
	                        sqlInsertFin.append(" \nUNION ALL ");
	                    }
	                    i++;
	                }

	                System.out.println("SQL de insert pronto para execução");
	                
	                // gravar o financeiro
	                System.out.println("Executando insertFinByList");
	                insertFinByList(sqlInsertFin, codemp);
	                System.out.println("InsertFinByList concluído com sucesso");
	            } else {
	                System.out.println("Nenhum insert a realizar");
	            }
	        } else {
	            System.out.println("ERRO: Status da API diferente de 200: " + responseStatus);
	            selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Api Retornou Status Diferente de 200: "+responseStatus+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
	        }
	    } catch (Exception e) {
	        System.out.println("ERRO GRAVE no cadastrarFinanceiro: " + e.getMessage());
	        e.printStackTrace();
	        selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Erro ao integrar financeiro, Mensagem de erro: "
	                            + e.getMessage()+"', SYSDATE, 'Erro', "+codemp+", '"+idAluno+"' FROM DUAL");
	    } finally {
	        System.out.println("===== FIM cadastrarFinanceiro =====");
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
		 

	public void insertFinByList(StringBuilder listInsert, BigDecimal codemp) throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		EnviromentUtils util = new EnviromentUtils();
		
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
					+ "         AD_IDALUNO) " + listInsert.toString();

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.executeUpdate();

		} catch (Exception se) {
			se.printStackTrace();
			selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Erro ao integrar financeiro, Mensagem de erro: "
								+ se.getMessage().replace("'", "\"")+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");

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

	public BigDecimal insertFin(BigDecimal codemp, BigDecimal codCenCus,
			BigDecimal codNat, BigDecimal codTipOper, BigDecimal codparc,
			BigDecimal codTipTit, BigDecimal vlrDesdbo, String dtVenc,
			String dtPedido, String idExterno, String idAluno,
			BigDecimal codConta, BigDecimal codBanco, String recDesp)
			throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		BigDecimal nufin = getMaxNumFin();

		System.out.println("Teste financeiro: " + nufin);

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
					+ "         AD_IDALUNO) " + "        VALUES (?, "
					+ "               NULL, " + "               0, "
					+ "               'F', " + "               "
					+ recDesp
					+ ", "
					+ "               "
					+ codemp
					+ " , " // AS CODEMP
					+ "               "
					+ codCenCus
					+ " , " // AS CODCENCUS
					+ "               "
					+ codNat
					+ " , " // AS CODNAT
					+ "               "
					+ codTipOper
					+ " , " // AS CODTIPOPER
					+ "               (SELECT MAX(DHALTER) "
					+ "                  FROM TGFTOP "
					+ "                 WHERE CODTIPOPER = "
					+ codTipOper
					+ "), "
					+ "               0, "
					+ "               (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 0), "
					+ "               "
					+ codparc
					+ " , " // AS CODPARC
					+ "               "
					+ codTipTit
					+ " , " // AS CODTIPTIT
					+ "               "
					+ vlrDesdbo
					+ " , " // AS VLRDESDOB
					+ "               0, "
					+ "               0, "
					+ "               "
					+ codBanco
					+ ", " // AS CODBCO
					+ "               "
					+ codConta
					+ ", " // AS CODCTABCOINT
					+ "               '"
					+ dtPedido
					+ "' , " // AS DTNEG
					+ "               SYSDATE, "
					+ "               SYSDATE, "
					+ "               '"
					+ dtVenc
					+ "' , " // AS DTVENC
					+ "               SYSDATE, " // AS PRAZO
					+ "               '"
					+ dtVenc
					+ "' , " // AS DTVENCINIC
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
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0, "
					+ "               0,"
					+ "               '"
					+ idExterno
					+ "',"
					+ "     '"
					+ idAluno + "')";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.setBigDecimal(1, nufin);
			/*
			 * pstmt.setBigDecimal(2, codemp); pstmt.setBigDecimal(3,
			 * codCenCus); pstmt.setBigDecimal(4, codNat);
			 * pstmt.setBigDecimal(5, codTipOper); pstmt.setBigDecimal(6,
			 * codTipOper); pstmt.setBigDecimal(7, codparc);
			 * pstmt.setBigDecimal(8, codTipTit); pstmt.setBigDecimal(9,
			 * vlrDesdbo); pstmt.setString(10, dtPedido); pstmt.setString(11,
			 * dtVenc); pstmt.setString(12, dtVenc); pstmt.setString(13,
			 * idExterno); pstmt.setString(14, idAluno);
			 */

			pstmt.executeUpdate();

		} catch (Exception se) {
			se.printStackTrace();
			try {
				
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

			//updateNumFin();

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
			//selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Credor com informa��es invalidas ou nulas', SYSDATE, 'Aviso', "+codemp+", '' FROM DUAL");
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

			String sqlNota = "SELECT CODNAT FROM AD_NATACAD WHERE IDEXTERNO = '"
					+ idExterno
					+ "'"
					+ " union "
					+ "SELECT 0 FROM DUAL WHERE NOT EXISTS (SELECT CODNAT FROM AD_NATACAD WHERE IDEXTERNO = '"
					+ idExterno + "')";

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

		if (id.compareTo(BigDecimal.ZERO) == 0) {
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

			String sqlNota = "SELECT CODCENCUS FROM AD_NATACAD WHERE IDEXTERNO = "
					+ idTaxa;

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

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TGFFIN WHERE AD_IDEXTERNO = ? AND CODEMP = "
					+ codemp;

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

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM DUAL WHERE TO_DATE('"
					+ data
					+ "') < ADD_MONTHS(SYSDATE, 12 * -(SELECT NVL(INTEIRO,0) FROM TSIPAR WHERE CHAVE = 'LIMINFANO')) ";

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
					+ "FROM AD_NATACAD where idexterno = "
					+ idExterno
					+ " "
					+ "union SELECT '0' FROM DUAL "
					+ "WHERE NOT EXISTS (SELECT SUBSTR(CODNAT, 0, 1) NAT FROM AD_NATACAD where idexterno = "
					+ idExterno + ")";

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

		if (id > 1) {
			return true;
		} else {
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
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			if (jdbc != null) {
				jdbc.closeSession();
			}
		}

	}

	public void updateNumFinByQtd(int qtdAdd) throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {

			jdbc.openSession();

			String sqlUpdate = "UPDATE TGFNUM SET ULTCOD = (NVL(ULTCOD, 0) + "
					+ qtdAdd + ")  WHERE ARQUIVO = 'TGFFIN'";

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

			String sqlUpdate = "UPDATE AD_ALUNOS SET INTEGRADO = 'S' WHERE ID_EXTERNO = '"
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
			System.out.println("Entrou no UPDATE da flag dos alunos");
			jdbc.openSession();

			String sqlUpdate = "UPDATE AD_ALUNOS SET INTEGRADO = 'N'";

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

	public void insertLogIntegracao(String descricao, String status, 
			String idMatricula, BigDecimal codemp)
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

	private void updateCarga(BigDecimal idCarga) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlUpd = "UPDATE AD_CARGAALUNOS SET INTEGRADO_FIN = 'S' WHERE IDCARGA = "
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

			String sqlUpd = "UPDATE AD_CARGAALUNOS SET INTEGRADO_FIN = 'N' WHERE CODEMP = "
					+ codEmp + " AND INTEGRADO_FIN = 'S'";

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

	private List<Object[]> retornarInformacoesParceiros() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();

		try {
			jdbc.openSession();
			String sql = "	SELECT 	CODPARC, CGC_CPF";
			sql += "		FROM  	TGFPAR ";

			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();

			while (rs.next()) {
				Object[] ret = new Object[2];
				ret[0] = rs.getBigDecimal("CODPARC");
				ret[1] = rs.getString("CGC_CPF");

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

	public List<Object[]> retornarInformacoesAlunos() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try { 
			jdbc.openSession();
			String sql = "	SELECT 	CODPARC, ID_EXTERNO, CODCENCUS, CODEMP ";
			sql += "		FROM  	AD_ALUNOS ";
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[4];
				ret[0] = rs.getBigDecimal("CODPARC");
				ret[1] = rs.getString("ID_EXTERNO");
				ret[2] = rs.getBigDecimal("CODCENCUS");
				ret[3] = rs.getBigDecimal("CODEMP");

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
	
	
	//usando JapeWrapper com  DynamicVO
//	public List<Object[]> retornarInformacoesAlunos() throws Exception {
//	    List<Object[]> listRet = new ArrayList<>();
//	    
//	    JapeWrapper dao = JapeFactory.dao("AD_ALUNOS");
//	    Collection<DynamicVO> alunos = dao.find(null, null, "CODPARC", "ID_EXTERNO", "CODCENCUS", "CODEMP");
//	    
//	    for (DynamicVO alunoVO : alunos) {
//	        Object[] ret = new Object[4];
//	        ret[0] = alunoVO.asBigDecimal("CODPARC");
//	        ret[1] = alunoVO.asString("ID_EXTERNO");
//	        ret[2] = alunoVO.asBigDecimal("CODCENCUS");
//	        ret[3] = alunoVO.asBigDecimal("CODEMP");
//	        listRet.add(ret);
//	    }
//	    
//	    return listRet;
//	}
	
	
//	public List<Object[]> retornarInformacoesCenCus() throws Exception {
//	    List<Object[]> listRet = new ArrayList<>();
//
//	    JapeWrapper dao = JapeFactory.dao("AD_NATACAD");
//	    Collection<DynamicVO> registros = dao.find(null, null, "CODCENCUS", "IDEXTERNO", "PROFISSIONAL", "TECNICO");
//
//	    for (DynamicVO registro : registros) {
//	        Object[] ret = new Object[3];
//	        ret[0] = registro.asBigDecimal("CODCENCUS");
//	        ret[1] = registro.asString("IDEXTERNO");
//
//	        String flag = "N";
//	        if ("S".equals(registro.asString("PROFISSIONAL"))) {
//	            flag = "P";
//	        } else if ("S".equals(registro.asString("TECNICO"))) {
//	            flag = "T";
//	        }
//	        ret[2] = flag;
//
//	        listRet.add(ret);
//	    }
//
//	    return listRet;
//	}


	public List<Object[]> retornarInformacoesCenCus() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "SELECT CODCENCUS, IDEXTERNO, CASE WHEN PROFISSIONAL = 'S' THEN 'P' WHEN TECNICO = 'S' THEN 'T' ELSE 'N' END AS FLAG "
					+ " FROM AD_NATACAD";

			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[3];
				ret[0] = rs.getBigDecimal("CODCENCUS");
				ret[1] = rs.getString("IDEXTERNO");
				ret[2] = rs.getString("FLAG");

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
	
	public List<Object[]> retornarInformacoesCenCusEmpresa() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "SELECT CODCENCUS, IDEXTERNO, CODEMP FROM AD_NATACAD WHERE CODEMP IS NOT NULL";
			
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[3];
				ret[0] = rs.getBigDecimal("CODCENCUS");
				ret[1] = rs.getString("IDEXTERNO");
				ret[2] = rs.getBigDecimal("CODEMP");
				
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

	public List<Object[]> retornarInformacoesNatureza() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "SELECT CODNAT, IDEXTERNO, CASE WHEN PROFISSIONAL = 'S' THEN 'P' WHEN TECNICO = 'S' THEN 'T' ELSE 'N' END AS FLAG FROM AD_NATACAD WHERE CODEMP IS NULL";

			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[3];
				ret[0] = rs.getBigDecimal("CODNAT");
				ret[1] = rs.getString("IDEXTERNO");
				ret[2] = rs.getString("FLAG");

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
	
	public List<Object[]> retornarInformacoesNaturezaEmpresa() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "SELECT CODNAT, IDEXTERNO, CODEMP FROM AD_NATACAD WHERE CODEMP IS NOT NULL";
			
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[3];
				ret[0] = rs.getBigDecimal("CODNAT");
				ret[1] = rs.getString("IDEXTERNO");
				ret[2] = rs.getBigDecimal("CODEMP");
				
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

	public List<Object[]> retornarInformacoesBancoConta() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "	SELECT 	CODCTABCOINT, CODEMP, IDEXTERNO, CODBCO ";
			sql += "		FROM  	ad_infobankbaixa WHERE IDEXTERNO IS NULL";
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[4];
				ret[0] = rs.getBigDecimal("CODCTABCOINT");
				ret[1] = rs.getLong("CODEMP");
				ret[2] = rs.getLong("IDEXTERNO");
				ret[3] = rs.getBigDecimal("CODBCO");

				listRet.add(ret);
			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw new Exception(
					"Erro Ao Executar Metodo retornarInformacoesBancoConta");
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

	public List<Object[]> retornarInformacoesRecDesp() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		List<Object[]> listRet = new ArrayList<>();
		try {
			jdbc.openSession();
			String sql = "SELECT CASE WHEN SUBSTR(CODNAT, 0, 1) > 1 THEN '-1' ELSE '1' END AS NAT, idexterno "
					+ "FROM AD_NATACAD where idexterno IS NOT NULL "
					+ " union SELECT '0', '0' FROM DUAL "
					+ " WHERE NOT EXISTS (SELECT SUBSTR(CODNAT, 0, 1) NAT FROM AD_NATACAD where idexterno IS NOT NULL)";

			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[2];
				ret[0] = rs.getString("NAT");
				ret[1] = rs.getString("idexterno");

				listRet.add(ret);
			}

		} catch (SQLException e) {
			e.printStackTrace();
			throw new Exception(
					"Erro Ao Executar Metodo retornarInformacoesBancoConta");
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
			selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Erro ao Extornar Titulo "+nufin+": "+e.getMessage()+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
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
			selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Erro ao Excluir Movimenta��o Bancaria "+nubco+": "+e.getMessage()+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
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
			selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Erro ao Excluir Titulo "+nufin+": "+e.getMessage()+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
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
			selectsParaInsertLog.add("SELECT <#NUMUNICO#>, 'Erro ao Inserir Lista de Financeiros Titulo: "+se.getMessage()+"', SYSDATE, 'Erro', "+codemp+", '' FROM DUAL");
			
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}
}
