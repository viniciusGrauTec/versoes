package br.com.sankhya.acoesgrautec.jobs;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class JobGetBaixaMap implements ScheduledAction {

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

			// Carregar os mapas das listas - Apenas para consultas únicas por
			// execução para evitar múltiplos acessos

			// Banco
			List<Object[]> listInfBancoConta = retornarInformacoesBancoConta();
			Map<String, BigDecimal> mapaInfBanco = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfBancoConta) {
				Long codEmpObj = (Long) obj[1];
				Long idExternoObj = (Long) obj[2];
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
				Long idExternoObj = (Long) obj[2];

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
				Long codEmpObj = (Long) obj[1];
				Long idExternoObj = (Long) obj[2];

				if (mapaInfFinanceiro.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfFinanceiro.put(codEmpObj + "###" + idExternoObj,
							nuFin);
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
				Long idExternoObj = (Long) obj[2];

				if (mapaInfTipoTitulo.get(codEmpObj + "###" + idExternoObj) == null) {
					mapaInfTipoTitulo.put(codEmpObj + "###" + idExternoObj,
							codTipTit);
				}
			}

			// Menor Movimentação Bancária Por Conta
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
			String query3 = "SELECT LINK.CODEMP, URL, TOKEN, IDCARGA, MATRICULA FROM AD_LINKSINTEGRACAO LINK INNER JOIN AD_CARGAALUNOS CARGA ON CARGA.CODEMP = LINK.CODEMP WHERE LINK.CODEMP = 3 ";
			String query4 = "SELECT LINK.CODEMP, URL, TOKEN, IDCARGA, MATRICULA FROM AD_LINKSINTEGRACAO LINK INNER JOIN AD_CARGAALUNOS CARGA ON CARGA.CODEMP = LINK.CODEMP WHERE LINK.CODEMP = 4 AND NVL(CARGA.INTEGRADO_BAIXA, 'N') = 'N'";

			pstmt = jdbc.getPreparedStatement(query3);

			rs = pstmt.executeQuery();

			tempoAnterior = printLogDebug(tempoAnterior,
					"Consulta para capturar o link de integração: AD_LINKSINTEGRACAO");

			while (rs.next()) {
				count++;
				
				System.out.println("Contagem: " + count);

				codEmp = rs.getBigDecimal("CODEMP");
				idCarga = rs.getBigDecimal("IDCARGA");

				url = rs.getString("URL");
				token = rs.getString("TOKEN");
				matricula = rs.getString("MATRICULA");

				efetuarBaixa(url, token, codEmp, matricula, mapaInfBanco,
						mapaInfConta, mapaInfAlunos, mapaInfFinanceiro,
						mapaInfTipoTitulo, mapaInfMenorDataMovBancariaPorConta,
						mapaInfFinanceiroBaixado, mapaInfFinanceiroValor,
						mapaInfFinanceiroBanco, tempoAnterior);

				tempoAnterior = printLogDebug(tempoAnterior,
						"onTime - efetuarBaixa da empresa(" + codEmp + ")");

				//updateCarga(idCarga);

				tempoAnterior = printLogDebug(tempoAnterior,
						"onTime - updateCarga da empresa(" + codEmp + ")");
			}
			System.out.println("Chegou ao final da baixa");
			if (count == 0) {
				//resetCarga(codEmp);
				tempoAnterior = printLogDebug(tempoAnterior, "resetCarga");
			}

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
		}
	}

	public void efetuarBaixa(String url, String token, BigDecimal codemp,
			String matricula, Map<String, BigDecimal> mapaInfBanco,
			Map<String, BigDecimal> mapaInfConta,
			Map<String, BigDecimal> mapaInfAlunos,
			Map<String, BigDecimal> mapaInfFinanceiro,
			Map<String, BigDecimal> mapaInfTipoTitulo,
			Map<Long, Date> mapaInfMenorDataMovBancariaPorConta,
			Map<BigDecimal, String> mapaInfFinanceiroBaixado,
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroValor,
			Map<BigDecimal, BigDecimal> mapaInfFinanceiroBanco,
			long tempoAnterior) throws Exception {

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

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		String dataEstorno = "";

		BigDecimal nufin = BigDecimal.ZERO;

		try {

			jdbc.openSession();

			BigDecimal codParc = mapaInfAlunos.get(matricula);
			
			if (codParc != null) {
				String[] response = apiGet(url
						+ "/financeiro"
						+ "/baixas"
						// + "?quantidade=1"
						// + "?dataInicial="+dataAtualFormatada+" 00:00:00"
						+ "?matricula="
						+ matricula
						// +
						// "&vencimentoInicial=2024-08-04 00:00:00&vencimentoFinal=2024-08-05 23:59:59"
						+ "&dataInicial=" + dataUmDiaFormatada
						+ " 00:00:00&dataFinal=" + dataAtualFormatada
						+ " 23:59:59", token);

				System.out.println("Teste: " + response[1]);

				JsonParser parser = new JsonParser();
				JsonArray jsonArray = parser.parse(response[1])
						.getAsJsonArray();

				tempoAnterior = printLogDebug(tempoAnterior,
						"efetuarBaixa - API financeiro/baixas");

				for (JsonElement jsonElement : jsonArray) {
					JsonObject jsonObject = jsonElement.getAsJsonObject();

					System.out.println("Titulo ID: "
							+ jsonObject.get("titulo_id").getAsInt());
					System.out.println("Valor da Baixa: "
							+ jsonObject.get("baixa_valor").getAsString());

					String tituloId = jsonObject.get("titulo_id").getAsString();
					BigDecimal vlrBaixa = new BigDecimal(jsonObject.get(
							"baixa_valor").getAsString());

					BigDecimal vlrJuros = new BigDecimal(jsonObject.get(
							"baixa_juros").getAsString());

					BigDecimal vlrMulta = new BigDecimal(jsonObject.get(
							"baixa_multa").getAsString());

					BigDecimal vlrDesconto = new BigDecimal(jsonObject.get(
							"baixa_desconto").getAsString());

					BigDecimal vlrOutrosAcrescimos = new BigDecimal(jsonObject
							.get("baixa_outros_acrescimos").getAsString());

					String dataBaixa = jsonObject.get("baixa_data")
							.getAsString();

					Date data = formatoOriginal.parse(dataBaixa);

					String dataBaixaFormatada = formatoDesejado.format(data);

					nufin = mapaInfFinanceiro.get(codemp + "###" + tituloId);

					tempoAnterior = printLogDebug(tempoAnterior,
							"efetuarBaixa - nufin by tituloId");

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
					tempoAnterior = printLogDebug(tempoAnterior,
							"efetuarBaixa - codBanco");

					codConta = mapaInfConta.get(codemp + "###" + idExterno);
					tempoAnterior = printLogDebug(tempoAnterior,
							"efetuarBaixa - codConta");

					JsonArray formas_de_pagamento = jsonObject
							.getAsJsonArray("formas_de_pagamento");

					for (JsonElement formas_de_pagamentoElement : formas_de_pagamento) {

						JsonObject formas_de_pagamentoObject = formas_de_pagamentoElement
								.getAsJsonObject();

						System.out.println("Forma de pagamento: "
								+ formas_de_pagamentoObject.get(
										"forma_pagamento_id").getAsString());

						codTipTit = mapaInfBanco.get(codemp
								+ "###"
								+ formas_de_pagamentoObject.get(
										"forma_pagamento_id").getAsString());
					}

					tempoAnterior = printLogDebug(tempoAnterior,
							"efetuarBaixa - getAsJsonArray(formas_de_pagamento)");

					System.out.println("estorno: " + dataEstorno);
					System.out.println("Data estorno: "
							+ jsonObject.get("baixa_estorno_data"));

					if (nufin.compareTo(BigDecimal.ZERO) != 0) {

						Date dtMinMovConta = mapaInfMenorDataMovBancariaPorConta
								.get(codConta);

						if (data.after(dtMinMovConta)) {
							if (dataEstorno == null) {

								if ("N".equalsIgnoreCase(mapaInfFinanceiroBaixado
										.get(nufin))) {

									System.out.println("Chegou no update");
									if (vlrBaixa
											.compareTo(mapaInfFinanceiroValor
													.get(nufin)) == 0) {
										System.out
												.println("Entrou no if do valor");
										/*updateFin(codTipTit, nufin, codBanco,
												codConta, vlrDesconto,
												vlrJuros, vlrMulta,
												vlrOutrosAcrescimos);*/
									} else {
										System.out
												.println("Entrou no else do valor");
										/*updateFinComVlrBaixa(codTipTit, nufin,
												codBanco, codConta, vlrBaixa,
												vlrDesconto, vlrJuros,
												vlrMulta, vlrOutrosAcrescimos);*/
									}

									System.out.println("vlrDesconto: "
											+ vlrDesconto);
									System.out.println("vlrJuros: " + vlrJuros);
									System.out.println("vlrMulta: " + vlrMulta);

									/*
									 * updateFin(codTipTit, nufin, codBanco,
									 * codConta, vlrDesconto, vlrJuros,
									 * vlrMulta);
									 */

									/*nubco = insertMovBancaria(codConta,
											vlrBaixa, nufin, dataBaixaFormatada);*/

									System.out
											.println("Passou da mov bancaria: "
													+ nubco);

									System.out.println("vlrBaixa: " + vlrBaixa);

									/*updateBaixa(nufin, nubco, vlrBaixa,
											dataBaixaFormatada);*/

									movBanc = true;

									/*
									 * insertLogIntegracao(
									 * "Baixa Efetuada Com Sucesso Para o Financeiro: "
									 * + nufin, "Sucesso");
									 */
								} else {
									System.out.println("Financeiro " + nufin
											+ " já baixado");
								}
							} else {
								if ("S".equalsIgnoreCase(mapaInfFinanceiroBaixado
										.get(nufin))) {

									/*nubco = mapaInfFinanceiroBanco.get(nufin);
									updateFinExtorno(nufin);
									deleteTgfMbc(nubco);*/
									
									/*
									 * insertLogIntegracao(
									 * "Estorno Efetuado com sucesso",
									 * "Sucesso");
									 */

								}
							}
						} else {
							/*
							 * insertLogIntegracao("Baixa Para o Titulo " +
							 * nufin +
							 * " Não Efetuada Pois a Data Minima de Movimentação Bancaria "
							 * + "Para a Conta " +codConta+
							 * " é Superior a Data de Baixa: " +
							 * dataBaixaFormatada, "Aviso");
							 */
						}
						tempoAnterior = printLogDebug(tempoAnterior,
								"efetuarBaixa - if nufin.compareTo(BigDecimal.ZERO) != 0");
					} else {
						System.out
								.println("Não foi possivel encontrar financeiro com id externo "
										+ tituloId);
					}

				}
			}

		} catch (Exception e) {
			e.printStackTrace();

			if (movBanc) {
				updateFinExtorno(nufin);
				deleteTgfMbc(nubco);
				System.out.println("Apagou mov bank");
			}

			try {
				insertLogIntegracao(
						"Mensagem de erro nas Baixas: " + e.getMessage(),
						"Erro");
			} catch (Exception e1) {
				e1.printStackTrace();
			}
			tempoAnterior = printLogDebug(tempoAnterior, "efetuarBaixa - catch");
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

		}

	}

	public String[] apiGet(String ur, String token) throws Exception {

		BufferedReader reader;
		String line;
		StringBuilder responseContent = new StringBuilder();
		// String key = preferenciaSenha();

		// Preparando a requisição
		URL obj = new URL(ur);
		HttpURLConnection https = (HttpURLConnection) obj.openConnection();

		System.out.println("Entrou na API");
		System.out.println("URL: " + ur);
		System.out.println("https: " + https);
		System.out.println("token: " + token);

		https.setRequestMethod("GET");
		// https.setConnectTimeout(50000);
		https.setRequestProperty("User-Agent",
				"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)");
		https.setRequestProperty("Content-Type",
				"application/json; charset=UTF-8");
		https.setRequestProperty("Authorization", "Bearer " + token);
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

	public void updateFin(BigDecimal codtiptit, BigDecimal nufin,
			BigDecimal codBanco, BigDecimal codConta, BigDecimal vlrDesconto,
			BigDecimal vlrJuros, BigDecimal vlrMulta,
			BigDecimal vlrOutrosAcrescimos) throws Exception {
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
			throw e;
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public void deleteTgfMbc(BigDecimal nubco) throws Exception {
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
			throw e;
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public void updateFinExtorno(BigDecimal nufin) throws Exception {
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
			throw e;
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
			BigDecimal vlrOutrosAcrescimos) throws Exception {
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
					+ vlrOutrosAcrescimos + " WHERE nufin = ?";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setBigDecimal(1, codtiptit);
			pstmt.setBigDecimal(2, codBanco);
			pstmt.setBigDecimal(3, codConta);
			pstmt.setBigDecimal(4, nufin);

			pstmt.executeUpdate();

			System.out.println("Passou do update");
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public void updateBaixa(BigDecimal nufin, BigDecimal nubco,
			BigDecimal vlrDesdob, String dataBaixaFormatada) throws Exception {
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
					+ "CODUSUBAIXA = 0  " + "WHERE NUFIN = " + nufin;

			pstmt = jdbc.getPreparedStatement(sqlNota);

			pstmt.executeUpdate();

			System.out.println("Passou do update");
		} catch (SQLException e) {
			e.printStackTrace();
			throw e;
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
			BigDecimal vlrDesdob, BigDecimal nufin, String dataBaixaFormatada)
			throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		BigDecimal nubco = getMaxNumMbc();

		try {

			jdbc.openSession();

			String sqlUpdate = "INSERT INTO TGFMBC " + "(NUBCO, " + "CODLANC, "
					+ "DTLANC, " + "CODTIPOPER, " + "DHTIPOPER, "
					+ "DTCONTAB, " + "HISTORICO, " + "CODCTABCOINT, "
					+ "NUMDOC, " + "VLRLANC, " + "TALAO, " + "PREDATA, "
					+ "CONCILIADO, " + "DHCONCILIACAO, " + "ORIGMOV, "
					+ "NUMTRANSF, " + "RECDESP, " + "DTALTER, "
					+ "DTINCLUSAO, " + "CODUSU, " + "VLRMOEDA, " + "SALDO, "
					+ "CODCTABCOCONTRA, " + "NUBCOCP, " + "CODPDV ) "
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
				ret[2] = rs.getLong("IDEXTERNO");
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
				ret[1] = rs.getLong("CODEMP");
				ret[2] = rs.getLong("AD_IDEXTERNO");
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
			String sql = "	SELECT 	CODEMP, CODTIPTIT, IDEXTERNO ";
			sql += "		FROM  	AD_TIPTITINTEGRACAO ";
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				Object[] ret = new Object[3];
				ret[0] = rs.getBigDecimal("CODTIPTIT");
				ret[1] = rs.getLong("CODEMP");
				ret[2] = rs.getLong("IDEXTERNO");

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

}
