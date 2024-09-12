package br.com.sankhya.acoesgrautec.jobs;

import br.com.sankhya.acoesgrautec.services.SkwServicoCompras;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.core.JapeSession.SessionHandle;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;

import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

public class JobGetBaixaFornecedores implements ScheduledAction {
	public void onTime(ScheduledActionContext arg0) {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal codEmp = BigDecimal.ZERO;

		String url = "";
		String token = "";
		try {
			jdbc.openSession();

			String query3 = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO WHERE CODEMP = 3";
			String query4 = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO WHERE CODEMP = 4";

			pstmt = jdbc.getPreparedStatement(query4);

			rs = pstmt.executeQuery();
			while (rs.next()) {
				codEmp = rs.getBigDecimal("CODEMP");

				url = rs.getString("URL");
				token = rs.getString("TOKEN");

				efetuarBaixa(url, token, codEmp);
			}
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

	public void efetuarBaixa(String url, String token, BigDecimal codemp)
			throws Exception {
		System.out.println("Entrou no job baixa");

		boolean movBanc = false;

		SimpleDateFormat formatoOriginal = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat formatoDesejado = new SimpleDateFormat("dd/MM/yyyy");

		Date dataAtual = new Date();

		String dataAtualFormatada = formatoOriginal.format(dataAtual);

		BigDecimal codTipTit = BigDecimal.ZERO;
		BigDecimal codBanco = BigDecimal.ZERO;
		BigDecimal codConta = BigDecimal.ZERO;
		BigDecimal nubco = BigDecimal.ZERO;
		
		BigDecimal nufin = BigDecimal.ZERO;
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		String dataEstorno = "";

		SkwServicoCompras sc = null;

		JapeSession.SessionHandle hnd = null;

		hnd = JapeSession.open();

		System.out.println("Entrou aqui JOBBaixas");

		String domain = "http://127.0.0.1:8501";
		
		int count = 0;
		
		try {
			jdbc.openSession();

			//String sqlP = "SELECT AD_ID_EXTERNO_FORN, CODPARC FROM TGFPAR WHERE AD_ID_EXTERNO_FORN IS NOT NULL AND CODEMP = 3 and rownum < 4";
			String sqlP = "SELECT NUFIN, AD_IDEXTERNO, AD_PROCESSADO FROM TGFFIN WHERE CODPARC IN (SELECT CODPARC FROM AD_IDFORNACAD WHERE CODEMP = "+codemp+") AND AD_IDEXTERNO IS NOT NULL AND DHBAIXA IS NULL AND NVL(AD_PROCESSADO, 'N') = 'N' AND CODEMP = "+codemp+" AND ROWNUM <= 400";

			pstmt = jdbc.getPreparedStatement(sqlP);

			rs = pstmt.executeQuery();
			
			while (rs.next()) {
				count++;
				//String fornecedor = rs.getString("AD_ID_EXTERNO_FORN");
				String idExternoTitulo = rs.getString("AD_IDEXTERNO");

				String[] response = apiGet(url + "/financeiro" + "/clientes"
						+ "/titulos-pagar-baixa" + "?titulo=" + idExternoTitulo/*+"&vencimentoInicial=2024-01-01&vencimentoFinal=2024-03-31"*/,

				token);

				System.out.println("Teste: " + response[1]);
				
				String response2 = response[1];
				
				if(!response2.equals("[]")){
					JsonParser parser = new JsonParser();
					JsonArray jsonArray = parser.parse(response[1])
							.getAsJsonArray();

					for (JsonElement jsonElement : jsonArray) {

						JsonObject jsonObject = jsonElement.getAsJsonObject();

						String tituloId = jsonObject.get("titulo_id").getAsString();

						BigDecimal vlrBaixa = new BigDecimal(jsonObject.get(
								"baixa_valor").getAsString());

						BigDecimal vlrJuros = new BigDecimal(jsonObject.get(
								"baixa_juros").getAsString());

						BigDecimal vlrMulta = new BigDecimal(jsonObject.get(
								"baixa_multa").getAsString());

						BigDecimal vlrDesconto = new BigDecimal(jsonObject.get(
								"baixa_desconto").getAsString());
						
						BigDecimal vlrOutrosAcrescimos = new BigDecimal(jsonObject.get(
								"baixa_outros_acrescimos").getAsString());

						String dataBaixa = jsonObject.get("baixa_data")
								.getAsString();

						String baixaId = jsonObject.get("baixa_id").getAsString();

						Date data = formatoOriginal.parse(dataBaixa);

						String dataBaixaFormatada = formatoDesejado.format(data);

						nufin = getNufin(tituloId);
						if (jsonObject.has("baixa_estorno_data")) {
							if (!jsonObject.get("baixa_estorno_data").isJsonNull()) {
								System.out.println("Entrou no if de estorno");

								dataEstorno = jsonObject.get("baixa_estorno_data")
										.getAsString();
							}else{
								dataEstorno = null;
							}
						}
						
						String idExterno = jsonObject.get("local_pagamento_id")
								.getAsString();
						codBanco = /*new BigDecimal("999");*/getCodBanco(idExterno, codemp);
						codConta = /*new BigDecimal("44");*/getCodConta(idExterno, codemp);

						String[] response1 = apiGet(url + "/financeiro"
								+ "/clientes"
								+ "/titulos-pagar-baixa-forma-pagamento"
								+ "?baixa=" + baixaId, token);

						JsonParser parser2 = new JsonParser();
						JsonArray jsonArray2 = parser2.parse(response1[1])
								.getAsJsonArray();

						JsonObject jsonObject2 = jsonArray2.get(0)
								.getAsJsonObject();

						String baixaPagamentoVlr = jsonObject2.get(
								"forma_pagamento_valor").getAsString();

						String baixaFormaPagamentoId = jsonObject2.get(
								"forma_pagamento_id").getAsString();

						codTipTit = getTipTit(baixaFormaPagamentoId, codemp);

						System.out.println("estorno: " + dataEstorno);
						System.out.println("Data estorno: "
								+ jsonObject.get("baixa_estorno_data"));
						if (nufin.compareTo(BigDecimal.ZERO) != 0) {
							
							System.out.println("Achou nufin pra baixar: " + nufin);
							
							if (validarDataMinMovBancaria(codConta,
									dataBaixaFormatada)) {
								
								if (dataEstorno == null) {
									if ((validarBaixa(nufin))
											&& (nufin.compareTo(BigDecimal
													.valueOf(113328L)) != 0)) {
										
										System.out.println("Chegou no update");
										
										if (vlrBaixa.compareTo(getVlrDesdob(nufin)) == 0) {
											System.out
													.println("Entrou no if do valor");
											updateFin(codTipTit, nufin, codBanco,
													codConta, vlrDesconto,
													vlrJuros, vlrMulta, vlrOutrosAcrescimos);
										} else {
											System.out
													.println("Entrou no else do valor");
											updateFinComVlrBaixa(codTipTit, nufin,
													codBanco, codConta, vlrBaixa,vlrDesconto,
													vlrJuros, vlrMulta, vlrOutrosAcrescimos);
										}
										System.out.println("vlrDesconto: "
												+ vlrDesconto);
										System.out.println("vlrJuros: " + vlrJuros);
										System.out.println("vlrMulta: " + vlrMulta);

										nubco = insertMovBancaria(codConta,
												vlrBaixa, nufin, dataBaixaFormatada);

										movBanc = true;

										System.out
												.println("Passou da mov bancaria: "
														+ nubco);

										System.out.println("vlrBaixa: " + vlrBaixa);

										updateBaixa(nufin, nubco, vlrBaixa,
												dataBaixaFormatada);

										/*insertLogIntegracao(
												"Baixa Efetuada Com Sucesso Para o Financeiro: "
														+ nufin, "Sucesso");*/
									} else {
										System.out.println("Financeiro " + nufin
												+ " já baixado");
									}
								} else if (!validarBaixa(nufin)) {
									nubco = getNubco(nufin);
									updateFinExtorno(nufin);
									deleteTgfMbc(nubco);
									deleteTgfFin(nufin);
									/*insertLogIntegracao(
											"Estorno Efetuado com sucesso",
											"Sucesso");*/
								}
							} else {
								/*insertLogIntegracao(
										"Baixa Para o Titulo "
												+ nufin
												+ " Não Efetuada Pois a Data Minima de Movimentação Bancaria "
												+ "Para a Conta " + codConta
												+ " é Superior a Data de Baixa: "
												+ dataBaixaFormatada, "Aviso");*/
							}
						} else {
							System.out
									.println("Não foi possivel encontrar financeiro com id externo "
											+ tituloId);
						}
					}
				}
				
				updateFlagTituloProcessado(idExternoTitulo);
				
			}
			System.out.println("Chegou ao final da baixa");
			
			if(count == 0){
				updateResetarTitulo(codemp);
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
		
		System.out.println("response teste: " + response);
		
		https.disconnect();

		return new String[] { Integer.toString(status), response };

	}

	public BigDecimal getNufin(String idTitulo) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "SELECT NUFIN FROM TGFFIN WHERE AD_IDEXTERNO = ?";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setString(1, idTitulo.trim());

			rs = pstmt.executeQuery();
			if (rs.next()) {
				id = rs.getBigDecimal("NUFIN");
				if (id == null) {
					id = BigDecimal.ZERO;
				}
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
		return id;
	}

	public BigDecimal getNubco(BigDecimal nufin) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "SELECT NUBCO FROM TGFFIN WHERE NUFIN = " + nufin;

			pstmt = jdbc.getPreparedStatement(sqlNota);

			rs = pstmt.executeQuery();
			if (rs.next()) {
				id = rs.getBigDecimal("NUBCO");
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
		return id;
	}

	public boolean validarBaixa(BigDecimal nufin) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int count = 0;
		try {
			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TGFFIN WHERE NUFIN = ? AND DHBAIXA IS NOT NULL AND VLRBAIXA IS NOT NULL AND CODUSUBAIXA IS NOT NULL";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setBigDecimal(1, nufin);

			rs = pstmt.executeQuery();
			if (rs.next()) {
				count = rs.getInt("COUNT");
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
		if (count > 0) {
			return false;
		}
		return true;
	}

	public boolean validarDataMinMovBancaria(BigDecimal codConta,
			String dataBaixa) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int count = 0;
		try {
			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM (SELECT MIN(REFERENCIA) DTREF \t    FROM TGFSBC \t   WHERE CODCTABCOINT = "
					+

					codConta
					+ ") "
					+ "\tWHERE DTREF > TO_DATE('"
					+ dataBaixa
					+ "', 'DD/MM/YYYY')";

			pstmt = jdbc.getPreparedStatement(sqlNota);

			rs = pstmt.executeQuery();
			if (rs.next()) {
				count = rs.getInt("COUNT");
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
		if (count > 0) {
			return false;
		}
		return true;
	}

	public BigDecimal getTipTit(String idExterno, BigDecimal codEmp)
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "SELECT CODTIPTIT FROM AD_TIPTITINTEGRACAO WHERE CODEMP = ? AND IDEXTERNO = ?";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setBigDecimal(1, codEmp);
			pstmt.setString(2, idExterno.trim());

			rs = pstmt.executeQuery();
			if (rs.next()) {
				id = rs.getBigDecimal("CODTIPTIT");
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
		return id;
	}

	public BigDecimal getCodBanco(String idExterno, BigDecimal codEmp)
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "select CODBCO from ad_infobankbaixa WHERE CODEMP = ? AND IDEXTERNO = ?";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setBigDecimal(1, codEmp);
			pstmt.setString(2, idExterno.trim());

			rs = pstmt.executeQuery();
			if (rs.next()) {
				id = rs.getBigDecimal("CODBCO");
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
		return id;
	}

	public BigDecimal getCodConta(String idExterno, BigDecimal codEmp)
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "select CODCTABCOINT from ad_infobankbaixa WHERE CODEMP = ? AND IDEXTERNO = ?";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setBigDecimal(1, codEmp);
			pstmt.setString(2, idExterno.trim());

			rs = pstmt.executeQuery();
			if (rs.next()) {
				id = rs.getBigDecimal("CODCTABCOINT");
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
		return id;
	}

	public BigDecimal getVlrDesdob(BigDecimal nufin) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal vlrDesdob = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "select VLRDESDOB FROM TGFFIN WHERE NUFIN = "
					+ nufin;

			pstmt = jdbc.getPreparedStatement(sqlNota);

			rs = pstmt.executeQuery();
			if (rs.next()) {
				vlrDesdob = rs.getBigDecimal("VLRDESDOB");
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
		return vlrDesdob;
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
					
						sqlNota += "AD_VLRDESCINT = "+vlrDesconto+", ";
						sqlNota += "VLRINSS = 0, " + "VLRIRF = 0, " + "VLRISS = 0, ";
						sqlNota += "AD_VLRMULTAINT = "+vlrMulta+", ";
						sqlNota += "AD_VLRJUROSINT = "+vlrJuros+", AD_OUTACRESCIMOS = " + vlrOutrosAcrescimos;
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
	
	public void deleteTgfFin(BigDecimal nufin) throws Exception {
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

			String sqlNota = "UPDATE TGFFIN SET VLRBAIXA = 0, DHBAIXA = NULL, NUBCO = NULL, CODTIPOPERBAIXA = 0, DHTIPOPERBAIXA = (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 0), CODUSUBAIXA = NULL  WHERE NUFIN = "
					+

					nufin;

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
			BigDecimal outrosAcrescimos)
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlNota = "UPDATE TGFFIN SET CODTIPTIT = ?, CODBCO = ?, CODCTABCOINT = ?, AD_VLRDESCINT = "+vlrDesconto+", VLRINSS = 0, VLRIRF = 0, VLRISS = 0, AD_VLRJUROSINT = "+vlrJuros+", AD_VLRMULTAINT = "+vlrMulta+", TIPJURO = null, AD_VLRORIG = VLRDESDOB, VLRDESDOB = "
					+ vlrBaixa + "," + "TIPMULTA = null, AD_OUTACRESCIMOS = "+outrosAcrescimos+" WHERE nufin = ?";

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

			String sqlNota = "UPDATE TGFFIN SET VLRBAIXA = "
					+

					vlrDesdob
					+ ", "
					+ "DHBAIXA = '"
					+ dataBaixaFormatada
					+ "', "
					+ "NUBCO = "
					+ nubco
					+ ", "
					+ "CODTIPOPERBAIXA = 1500, "
					+ "DHTIPOPERBAIXA = (SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 1500), "
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

		jdbc.openSession();

		String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, STATUS)VALUES (((SELECT NVL(MAX(NUMUNICO), 0) + 1 FROM AD_LOGINTEGRACAO)), ?, SYSDATE, ?)";

		pstmt = jdbc.getPreparedStatement(sqlUpdate);
		pstmt.setString(1, descricao);
		pstmt.setString(2, status);
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

	public BigDecimal insertMovBancaria(BigDecimal contaBancaria,
			BigDecimal vlrDesdob, BigDecimal nufin, String dataBaixaFormatada)
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		BigDecimal nubco = getMaxNumMbc();

		jdbc.openSession();

		String sqlUpdate = "INSERT INTO TGFMBC (NUBCO, CODLANC, DTLANC, CODTIPOPER, DHTIPOPER, DTCONTAB, HISTORICO, CODCTABCOINT, NUMDOC, VLRLANC, TALAO, PREDATA, CONCILIADO, DHCONCILIACAO, ORIGMOV, NUMTRANSF, RECDESP, DTALTER, DTINCLUSAO, CODUSU, VLRMOEDA, SALDO, CODCTABCOCONTRA, NUBCOCP, CODPDV )  VALUES ("
				+

				nubco
				+ ", "
				+ "1, "
				+ "'"
				+ dataBaixaFormatada
				+ "'"
				+ ", "
				+ "1500, "
				+ "(SELECT MAX(DHALTER) FROM TGFTOP WHERE CODTIPOPER = 1500), "
				+ "NULL, "
				+ "(SELECT HISTORICO FROM TGFFIN WHERE NUFIN = "
				+ nufin
				+ "), "
				+

				contaBancaria
				+ ", "
				+ "0, "
				+

				vlrDesdob
				+ ", "
				+ "NULL, "
				+ "'"
				+ dataBaixaFormatada
				+ "', "
				+ "'N', "
				+ "NULL, "
				+ "'F', "
				+ "NULL, "
				+ "1, "
				+ "SYSDATE, "
				+ "SYSDATE, "
				+ "0, "
				+ "0, "
				+ vlrDesdob
				+ ", "
				+ "NULL,  "
				+ "NULL, " + "NULL) ";

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

		jdbc.openSession();

		String sqlUpdate = "UPDATE TGFNUM SET ULTCOD = NVL(ULTCOD, 0) + 1  WHERE ARQUIVO = 'TGFMBC'";

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
	

	public void updateFlagTituloProcessado(String idTitulo) throws Exception {
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		try {
			
			jdbc.openSession();
			
			String sqlUpdate = "UPDATE TGFFIN SET AD_PROCESSADO = 'S' WHERE AD_IDEXTERNO = '" + idTitulo + "'";
			
			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.executeUpdate();
			
		} catch (Exception se) {
			se.printStackTrace();
		}finally{
			if (pstmt != null) {
				pstmt.close();
			}
			if (jdbc != null) {
				jdbc.closeSession();
			}
		}
		
	}
	
	
	public void updateResetarTitulo(BigDecimal codEmp) throws Exception {
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		try {
			
			jdbc.openSession();
			
			String sqlUpdate = "UPDATE TGFFIN SET AD_PROCESSADO = 'N' WHERE AD_IDEXTERNO IS NOT NULL AND DHBAIXA IS NULL AND CODEMP = " + codEmp + " AND AD_IDALUNO IS NULL";
			
			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.executeUpdate();
			
		} catch (Exception se) {
			se.printStackTrace();
		}finally{
			if (pstmt != null) {
				pstmt.close();
			}
			if (jdbc != null) {
				jdbc.closeSession();
			}
		}
		
	}
	

}
