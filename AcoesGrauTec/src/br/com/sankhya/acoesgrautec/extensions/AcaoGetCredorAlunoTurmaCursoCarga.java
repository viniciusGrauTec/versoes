package br.com.sankhya.acoesgrautec.extensions;

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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintStream;
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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.activiti.engine.impl.util.json.JSONArray;
import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

public class AcaoGetCredorAlunoTurmaCursoCarga
  implements AcaoRotinaJava, ScheduledAction
 {
	
	private List<String> selectsParaInsert = new ArrayList<String>();
	private EnviromentUtils util = new EnviromentUtils();
	
	@Override
	public void doAction(ContextoAcao contexto) throws Exception {
		
		Registro[] linhas = contexto.getLinhas();
		Registro registro = linhas[0];
		
		String url = (String) registro.getCampo("URL");
		String token = (String) registro.getCampo("TOKEN");
		BigDecimal codEmp = (BigDecimal) registro.getCampo("CODEMP");
		
		String dataInicio = contexto.getParam("DTINICIO").toString().substring(0, 10);
		String dataFim = contexto.getParam("DTFIM").toString().substring(0, 10);
		
		String matricula = (String) contexto.getParam("Matricula");
		
		try {

			// Alunos
			List<Object[]> listInfAlunos = retornarInformacoesAlunos(); 
			Map<String, BigDecimal> mapaInfAlunos = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfAlunos) {
				BigDecimal codParc = (BigDecimal) obj[0];
				String idExternoObj = (String) obj[1];
				BigDecimal codemp = (BigDecimal) obj[2];

				if (mapaInfAlunos.get(idExternoObj + "###" + codemp) == null) {
					mapaInfAlunos.put(idExternoObj + "###" + codemp, codParc);
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
			
			processDateRange(mapaInfAlunos, mapaInfParceiros, url, token,
					codEmp, dataInicio, dataFim, matricula);
			
			contexto.setMensagemRetorno("Periodo Processado!");
			
		}catch(Exception e){
			e.printStackTrace();
			contexto.mostraErro(e.getMessage());
		}finally{
			if(selectsParaInsert.size() > 0){
				
				StringBuilder msgError = new StringBuilder();
				
				System.out.println("Entrou na lista do finally: " + selectsParaInsert.size());
				
				//BigDecimal idInicial = util.getMaxNumLog();
				
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
				insertLogList(msgError.toString());
				
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

		System.out.println("Iniciou cadastro dos alunos");
		
		String threadName = Thread.currentThread().getName();
	    System.out.println("=== INÍCIO DO JOB === Thread: " + threadName + " - Hora: " + new Date());

		try {
			
			System.out.println("Iniciando carregamento de informações - Thread: " + threadName);

			// Alunos
			List<Object[]> listInfAlunos = retornarInformacoesAlunos();
			Map<String, BigDecimal> mapaInfAlunos = new HashMap<String, BigDecimal>();
			for (Object[] obj : listInfAlunos) {
				BigDecimal codParc = (BigDecimal) obj[0];
				String idExternoObj = (String) obj[1];
				BigDecimal codemp = (BigDecimal) obj[2];

				if (mapaInfAlunos.get(idExternoObj + "###" + codemp) == null) {
					mapaInfAlunos.put(idExternoObj + "###" + codemp, codParc);
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

			jdbc.openSession();
			
			String queryEmp = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO";

			pstmt = jdbc.getPreparedStatement(queryEmp);

			rs = pstmt.executeQuery();
			
			
			System.out.println("Iniciando processamento das empresas - Thread: " + threadName);
			
			
			while (rs.next()) {
				System.out.println("While principal");

				codEmp = rs.getBigDecimal("CODEMP");

				url = rs.getString("URL");
				token = rs.getString("TOKEN");
				System.out.println("Processando empresa " + codEmp + " na thread " + threadName);
				
				iterarEndpoint(mapaInfAlunos, mapaInfParceiros, url, token,                                
						codEmp);
				
				System.out.println("Finalizou processamento da empresa " + codEmp + " na thread " + threadName);
				
			}

			System.out.println("Finalizou o cadastro dos alunos");

		} catch (Exception e) {
			System.out.println("Erro no job - Thread: " + threadName);
			e.printStackTrace();
			try {
				insertLogIntegracao(
						"Erro ao integrar Alunos e/ou Credor, Mensagem de erro: "
								+ e.getMessage(), "Erro", "", "");
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		} finally {
			System.out.println("=== FIM DO JOB === Thread: " + threadName + " - Hora: " + new Date());
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
				
				//BigDecimal idInicial = util.getMaxNumLog();
				
				int qtdInsert = selectsParaInsert.size();
				
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

					if (i < qtdInsert) {
						msgError.append(" \nUNION ALL ");
					}
					i++;
				}
				
				System.out.println("Consulta de log: \n" + msgError);
				
				try {
					insertLogList(msgError.toString());
				} catch (Exception e) {
					e.printStackTrace();
				}
				
				msgError = null;
				this.selectsParaInsert = new ArrayList<String>();
				
			}
		
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
			String sql = "	SELECT 	CODPARC, ID_EXTERNO, CODEMP ";
			sql += "		FROM  	AD_ALUNOS ";
			pstmt = jdbc.getPreparedStatement(sql);
			rs = pstmt.executeQuery();
			while (rs.next()) {  //erro aqui 313
				Object[] ret = new Object[3];
				ret[0] = rs.getBigDecimal("CODPARC");
				ret[1] = rs.getString("ID_EXTERNO");
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

	
	//iterarendpoint das requisicoes
	//novo método iterarendpoint com URL encoder
	public void processDateRange(
	        Map<String, BigDecimal> mapaInfAlunos,
	        Map<String, BigDecimal> mapaInfParceiros,
	        String url,
	        String token,
	        BigDecimal codEmp,
	        String dataInicio,
	        String dataFim,
	        String matricula) throws Exception {

	    try {
	        // Preparar as datas
	        String dataInicialCompleta = dataInicio + " 00:00:00";
	        String dataFinalCompleta = dataFim + " 23:59:59";

	        // Codificar os parâmetros
	        String dataInicialEncoded = URLEncoder.encode(dataInicialCompleta, "UTF-8");
	        String dataFinalEncoded = URLEncoder.encode(dataFinalCompleta, "UTF-8");

	        // Lista para armazenar todos os registros
	        JSONArray todosRegistros = new JSONArray();
	        int pagina = 1;
	        boolean temMaisRegistros = true;

	        while (temMaisRegistros) {
	            // Construir a URL para a página atual
	            StringBuilder urlBuilder = new StringBuilder();
	            urlBuilder.append(url.trim())
	                    .append("/alunos")
	                    .append("?pagina=").append(pagina)
	                    .append("&quantidade=100")
	                    .append("&dataInicial=").append(dataInicialEncoded)
	                    .append("&dataFinal=").append(dataFinalEncoded);

	            // Adicionar parâmetro de matrícula se estiver presente
	            if (matricula != null && !matricula.isEmpty()) {
	                String matriculaEncoded = URLEncoder.encode(matricula, "UTF-8");
	                urlBuilder.append("&matricula=").append(matriculaEncoded);
	            }

	            String urlCompleta = urlBuilder.toString();
	            System.out.println("URL para alunos (página " + pagina + "): " + urlCompleta);

	            // Fazer a requisição
	            String[] response = apiGet2(urlCompleta, token);
	            int status = Integer.parseInt(response[0]);

	            if (status == 200) {
	                JSONArray paginaAtual = new JSONArray(response[1]);
	                
	                // Adicionar registros ao array acumulado
	                for (int i = 0; i < paginaAtual.length(); i++) {
	                    todosRegistros.put(paginaAtual.getJSONObject(i));
	                }
	                
	                // Verificar se é a última página
	                if (paginaAtual.length() <100) {
	                    temMaisRegistros = false;
	                } else {
	                    pagina++;
	                }
	                
	                System.out.println("Página " + pagina + ": " + paginaAtual.length() + 
	                                  " registros. Total acumulado: " + todosRegistros.length());
	            } else {
	                throw new Exception(String.format(
	                    "Erro na requisição de alunos. Status: %d. Resposta: %s. URL: %s",
	                    status, response[1], urlCompleta
	                ));
	            }
	        }
	        
	        // Criar uma resposta combinada com todos os registros
	        String dadosCombinados = todosRegistros.toString();
	        System.out.println("Total de registros de alunos acumulados: " + todosRegistros.length());
	        
	        // Processar todos os registros acumulados
	        cadastrarCredorAlunoCursoTurma(
	                mapaInfAlunos,
	                mapaInfParceiros,
	                dadosCombinados,
	                codEmp
	        );

	    } catch (Exception e) {
	        System.err.println("Erro ao processar período " + dataInicio +
	                " até " + dataFim + ": " + e.getMessage());
	        throw e;
	    }
	}
	
	public void iterarEndpoint(Map<String, BigDecimal> mapaInfAlunos,
			Map<String, BigDecimal> mapaInfParceiros, String url, String token,
			BigDecimal codEmp) throws Exception {
		
		Date dataAtual = new Date();

		SimpleDateFormat formato = new SimpleDateFormat("yyyy-MM-dd");

		String dataFormatada = formato.format(dataAtual);
		
		try {

			System.out.println("While de iteração");

			String[] response = apiGet2(url + "/alunos" + "?dataInicial="
					+ dataFormatada + " 00:00:00&dataFinal=" + dataFormatada
					+ " 23:59:59" + "&quantidade=0", token);

			int status = Integer.parseInt(response[0]);
			System.out.println("Status teste: " + status);

			String responseString = response[1];
			System.out.println("response string: " + responseString);

			cadastrarCredorAlunoCursoTurma(mapaInfAlunos, mapaInfParceiros,
					responseString, codEmp);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	

	private void updateAcaoAgendada() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlUpd = "UPDATE TSIAAG SET ATIVO = 'N' WHERE NUAAG = 35";

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

	private void updateCarga(BigDecimal idCarga) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlUpd = "UPDATE AD_CARGAALUNOS SET INTEGRADO = 'S' WHERE IDCARGA = "
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

	private void insertUltPagina(int pagina) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlUpd = "INSERT INTO AD_BLOCOPAGINACAO (IDPAGINA, PAGINAATUAL) VALUES ((SELECT NVL(MAX(IDPAGINA), 0) + 1 FROM AD_BLOCOPAGINACAO), "
					+ pagina + ")";

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

	public int getPagina() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int pagina = 0;
		try {

			jdbc.openSession();

			String sqlSlt = "SELECT NVL((SELECT PAGINAATUAL FROM AD_BLOCOPAGINACAO WHERE IDPAGINA = (SELECT MAX(IDPAGINA) FROM AD_BLOCOPAGINACAO)), 0) AS PAGINA FROM DUAL";

			pstmt = jdbc.getPreparedStatement(sqlSlt);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				pagina = rs.getInt("PAGINA");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			if (rs != null) {
				rs.close();
			}
			jdbc.closeSession();
		}

		return pagina;
	}

	public void cadastrarCredorAlunoCursoTurma(
			Map<String, BigDecimal> mapaInfAlunos,
			Map<String, BigDecimal> mapaInfParceiros, String dadosCombinados,
			BigDecimal codEmp) {
		System.out.println("Cadastro principal");
		
		EnviromentUtils util = new EnviromentUtils();
		
		try {
			JsonParser parser = new JsonParser();
			JsonArray jsonArray = parser.parse(dadosCombinados).getAsJsonArray();
			for (JsonElement jsonElement : jsonArray) {
				JsonObject jsonObject = jsonElement.getAsJsonObject();

				String credorNome = jsonObject.get("credor_nome").isJsonNull() ? null
						: jsonObject.get("credor_nome").getAsString();
				
				String credorCpf = jsonObject.get("credor_cpf").isJsonNull() ? null
						: jsonObject.get("credor_cpf").getAsString();
				
				String credorEndereco = jsonObject.get("credor_endereco")
						.isJsonNull() ? null : jsonObject
						.get("credor_endereco").getAsString();
				
				String credorCep = jsonObject.get("credor_endereco_cep")
						.isJsonNull() ? null : jsonObject.get(
						"credor_endereco_cep").getAsString();
				
				String credorBairro = jsonObject.get("credor_endereco_bairro")
						.isJsonNull() ? null : jsonObject.get(
						"credor_endereco_bairro").getAsString();
				
				String credorCidade = jsonObject.get("credor_endereco_cidade")
						.isJsonNull() ? null : jsonObject.get(
						"credor_endereco_cidade").getAsString();
				
				String credorUf = jsonObject.get("credor_endereco_uf")
						.isJsonNull() ? null : jsonObject.get(
						"credor_endereco_uf").getAsString();
				
				String credorResidencial = jsonObject.get(
						"credor_telefone_residencial").isJsonNull() ? null
						: jsonObject.get("credor_telefone_residencial")
								.getAsString();
				
				String credorCelular = jsonObject
						.get("credor_telefone_celular").isJsonNull() ? null
						: jsonObject.get("credor_telefone_celular")
								.getAsString();
				
				String credorComercial = jsonObject.get(
						"credor_telefone_comercial").isJsonNull() ? null
						: jsonObject.get("credor_telefone_comercial")
								.getAsString();

				String alunoId = jsonObject.get("aluno_id").isJsonNull() ? null
						: jsonObject.get("aluno_id").getAsString();
				
				String alunoNome = jsonObject.get("aluno_nome").isJsonNull() ? null
						: jsonObject.get("aluno_nome").getAsString();
				
				String alunoNomeSocial = jsonObject.get("aluno_nome_social")
						.isJsonNull() ? null : jsonObject.get(
						"aluno_nome_social").getAsString();
				
				String alunoEndereco = jsonObject.get("aluno_endereco")
						.isJsonNull() ? null : jsonObject.get("aluno_endereco")
						.getAsString();
				
				String alunoCep = jsonObject.get("aluno_endereco_cep")
						.isJsonNull() ? null : jsonObject.get(
						"aluno_endereco_cep").getAsString();
				
				String alunoBairro = jsonObject.get("aluno_endereco_bairro")
						.isJsonNull() ? null : jsonObject.get(
						"aluno_endereco_bairro").getAsString();
				
				String alunoCidade = jsonObject.get("aluno_endereco_cidade")
						.isJsonNull() ? null : jsonObject.get(
						"aluno_endereco_cidade").getAsString();
				
				String alunoUf = jsonObject.get("aluno_endereco_uf")
						.isJsonNull() ? null : jsonObject.get(
						"aluno_endereco_uf").getAsString();
				
				String alunoSexo = jsonObject.get("aluno_sexo").isJsonNull() ? null
						: jsonObject.get("aluno_sexo").getAsString();
				
				String alunoDataNascimento = jsonObject.get(
						"aluno_data_nascimento").isJsonNull() ? null
						: jsonObject.get("aluno_data_nascimento").getAsString();
				
				String alunoRg = jsonObject.get("aluno_rg").isJsonNull() ? null
						: jsonObject.get("aluno_rg").getAsString();
				
				String alunoCpf = jsonObject.get("aluno_cpf").isJsonNull() ? null
						: jsonObject.get("aluno_cpf").getAsString();
				
				String alunoCelular = jsonObject.get("aluno_telefone_celular")
						.isJsonNull() ? null : jsonObject.get(
						"aluno_telefone_celular").getAsString();
				
				String alunoResidencial = jsonObject.get(
						"aluno_telefone_residencial").isJsonNull() ? null
						: jsonObject.get("aluno_telefone_residencial")
								.getAsString();
				
				String alunoEmail = jsonObject.get("aluno_email").isJsonNull() ? null
						: jsonObject.get("aluno_email").getAsString();

				String cursoDescricao = jsonObject.getAsJsonArray("cursos")
						.get(0).getAsJsonObject().get("curso_descricao")
						.isJsonNull() ? null : jsonObject
						.getAsJsonArray("cursos").get(0).getAsJsonObject()
						.get("curso_descricao").getAsString();

				String cursoId = jsonObject.getAsJsonArray("cursos").get(0)
						.getAsJsonObject().get("curso_id").isJsonNull() ? null
						: jsonObject.getAsJsonArray("cursos").get(0)
								.getAsJsonObject().get("curso_id")
								.getAsString();
				
				/*if(cursoId.equals("00001")){
					cursoId = "00023";
				}*/

				String turmaId = jsonObject.getAsJsonArray("cursos").get(0)
						.getAsJsonObject().get("turma_id").isJsonNull() ? null
						: jsonObject.getAsJsonArray("cursos").get(0)
								.getAsJsonObject().get("turma_id")
								.getAsString();

				String alunoSituacao = jsonObject.getAsJsonArray("cursos")
						.get(0).getAsJsonObject().get("situacao_descricao")
						.isJsonNull() ? null : jsonObject
						.getAsJsonArray("cursos").get(0).getAsJsonObject()
						.get("situacao_descricao").getAsString();
				
				String alunoSituacaoId = jsonObject.getAsJsonArray("cursos")
						.get(0).getAsJsonObject().get("situacao_id")
						.isJsonNull() ? null : jsonObject
						.getAsJsonArray("cursos").get(0).getAsJsonObject()
						.get("situacao_id").getAsString();

				boolean credor = mapaInfParceiros.get(credorCpf.trim()) == null ? false: true; // getIfCredorExist(credorCpf);
				System.out.println("credorCpf: " + credorCpf.trim());
				boolean aluno = mapaInfAlunos.get(alunoId.trim() + "###" + codEmp) == null ? false : true;//getIfAlunoExist(alunoId);

				if (credorCpf != null && credorCidade != null) {
					/*
					 * if ((alunoSituacaoId.equalsIgnoreCase("LFI")) ||
					 * (alunoSituacaoId.equalsIgnoreCase("LFR")) ||
					 * (alunoSituacaoId.equalsIgnoreCase("MT")) ||
					 * (alunoSituacaoId.equalsIgnoreCase("TF")) ||
					 * (alunoSituacaoId.equalsIgnoreCase("NC")) ||
					 * (alunoSituacaoId.equalsIgnoreCase("NF")) ||
					 * (alunoSituacaoId.equalsIgnoreCase("CAC")) ||
					 * (alunoSituacaoId.equalsIgnoreCase("FO"))) {
					 */
					System.out.println("entrou na validacaoo de cadastro");
					if (!credor) {
						BigDecimal credotAtual = insertCredor(credorNome,
								credorCpf, credorEndereco, credorCep,
								credorBairro, credorCidade, credorUf,
								credorResidencial, credorCelular,
								credorComercial, alunoNome, codEmp);
						/*
						 * insertLogIntegracao("Credor Cadastrado: ", "Sucesso",
						 * credorNome, "");*/
						 
						 insertCursoTurma(cursoDescricao, cursoId, turmaId,
						 credorNome, alunoNome, codEmp);
						 

						System.out.println("ID EXTERNO: " + cursoId);
						/*
						 * insertLogIntegracao("Curso e turma cadastrado",
						 * "Sucesso", credorNome, alunoNome);
						 */
						System.out.println("Teste validacao aluno: " + aluno
								+ ": alunoId");
						
						if (!aluno) {
							insertAluno(credotAtual, alunoId, alunoNome,
									alunoNomeSocial, alunoEndereco, alunoCep,
									alunoBairro, alunoCidade, alunoUf,
									alunoSexo, alunoDataNascimento, alunoRg,
									alunoCpf, alunoCelular, alunoResidencial,
									alunoEmail, alunoSituacao, alunoSituacaoId,
									credorNome, codEmp, cursoDescricao, turmaId);
							System.out.println("Entrou no cad aluno");
							/*
							 * insertLogIntegracao("Aluno Cadastro: ",
							 * "Sucesso", "", alunoNome);
							 */
						}

						/*
						 * insertLogIntegracao(
						 * "Finalizando chamada do endpoint de Alunos",
						 * "Sucesso", "", "");
						 */
					} else {
						/*
						 * insertLogIntegracao("Credor j� cadastrado", "Aviso",
						 * credorNome, "");
						 */

						BigDecimal credorCadastrado = mapaInfParceiros
								.get(credorCpf);// getCredorCadastrado(credorCpf);

						insertCursoTurma(cursoDescricao, cursoId, turmaId,
								credorNome, alunoNome, codEmp);
						/*
						 * insertLogIntegracao("Curso e turma cadastrado",
						 * "Sucesso", "", "");
						 */
						System.out.println("Teste validacao aluno: " + aluno
								+ ": alunoId: " + alunoId);
						if (!aluno) {
							System.out.println("Entrou no cad aluno");
							insertAluno(credorCadastrado, alunoId, alunoNome,
									alunoNomeSocial, alunoEndereco, alunoCep,
									alunoBairro, alunoCidade, alunoUf,
									alunoSexo, alunoDataNascimento, alunoRg,
									alunoCpf, alunoCelular, alunoResidencial,
									alunoEmail, alunoSituacao, alunoSituacaoId,
									credorNome, codEmp, cursoDescricao, turmaId);
						
						} else {
							updateAluno(alunoSituacaoId, alunoSituacao, alunoId);
						}

						/*
						 * insertLogIntegracao(
						 * "Finalizando chamada do endpoint de Alunos",
						 * "Sucesso", credorNome, alunoNome);
						 */
					}
					/*
					 * } else { System.out
					 * .println("Aluno com situacao diferente do esperado"); }
					 */

				}else{
					selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Credor com informacoes invalidas ou nulas', SYSDATE, 'Aviso', "+codEmp+", '"+alunoId+"' FROM DUAL");
					
					//util.inserirLog("Credor com informacoes invalidas ou nulas", "Aviso", alunoId, codEmp);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			try {
				selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro no chamado do endpoint: " + e.getMessage() +"', SYSDATE, 'Erro', "+codEmp+", NULL FROM DUAL");
				
				/*util.inserirLog(
						"Erro no chamado do endpoint: " + e.getMessage(),
						"Erro", "", codEmp);*/
			} catch (Exception e1) {
				e1.printStackTrace();
			}
		}
	}

	public boolean getIfCredorExist(String credorCpf) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int credor = 0;
		try {
			// updateTgfNumParc();

			jdbc.openSession();

			String sqlSlt = "SELECT COUNT(0) AS CREDOR FROM TGFPAR WHERE CGC_CPF = ?";

			pstmt = jdbc.getPreparedStatement(sqlSlt);
			pstmt.setString(1, credorCpf);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				credor = rs.getInt("CREDOR");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			if (rs != null) {
				rs.close();
			}
			jdbc.closeSession();
		}
		if (credor > 0) {
			return true;
		}
		return false;
	}

	public boolean getIfAlunoExist(String alunoCpf) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int aluno = 0;
		try {
			// updateTgfNumParc();

			jdbc.openSession();

			String sqlSlt = "SELECT COUNT(0) AS ALUNO FROM AD_ALUNOS WHERE ID_EXTERNO = ?";

			pstmt = jdbc.getPreparedStatement(sqlSlt);
			pstmt.setString(1, alunoCpf);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				aluno = rs.getInt("ALUNO");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			if (rs != null) {
				rs.close();
			}
			jdbc.closeSession();
		}
		if (aluno > 0) {
			return true;
		}
		return false;
	}

	public BigDecimal getCredorCadastrado(String credorCpf) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal credorCadastrado = BigDecimal.ZERO;
		try {
			// updateTgfNumParc();

			jdbc.openSession();

			String sqlSlt = "SELECT CODPARC FROM TGFPAR WHERE CGC_CPF = ?";

			pstmt = jdbc.getPreparedStatement(sqlSlt);
			pstmt.setString(1, credorCpf);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				credorCadastrado = rs.getBigDecimal("CODPARC");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			if (rs != null) {
				rs.close();
			}
			jdbc.closeSession();
		}
		return credorCadastrado;
	}

	public void updateTgfNumParc() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlUpd = "UPDATE TGFNUM SET ULTCOD = ULTCOD + 1  WHERE ARQUIVO = 'TGFPAR'";

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

	public void updateAluno(String idSituacao, String situacao, String idAluno)
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlUpd = "UPDATE AD_ALUNOS SET SITUACAO_ID = '" + idSituacao
					+ "', SITUACAO = '" + situacao + "' WHERE ID_EXTERNO = '"
					+ idAluno + "'";

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

	public BigDecimal getMaxNumParc() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		BigDecimal bd = BigDecimal.ZERO;
		try {
			updateTgfNumParc();

			jdbc.openSession();

			String sqlUpd = "SELECT MAX (ULTCOD) AS ULTCOD FROM TGFNUM WHERE ARQUIVO = 'TGFPAR'";

			pstmt = jdbc.getPreparedStatement(sqlUpd);
			rs = pstmt.executeQuery();
			while (rs.next()) {
				bd = rs.getBigDecimal("ULTCOD");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			if (rs != null) {
				rs.close();
			}
			jdbc.closeSession();
		}
		return bd;
	}

	public BigDecimal insertCredor(String credorNome, String credorCpf,
			String credorEndereco, String credorCep, String credorBairro,
			String credorCidade, String credorUf, String credorResidencial,
			String credorCelular, String credorComercial, String alunoNome,
			BigDecimal codemp)
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		EnviromentUtils util = new EnviromentUtils();
		
		BigDecimal atualCodparc = util.getMaxNumParc();

		String tipPessoa = "";

		BigDecimal countBai = BigDecimal.ZERO;
		BigDecimal codEnd = BigDecimal.ZERO;
		BigDecimal codBai = BigDecimal.ZERO;
		if (credorCpf.length() == 11) {
			tipPessoa = "F";
		} else if (credorCpf.length() == 14) {
			tipPessoa = "J";
		}
		if ((credorBairro != null)
				&& (validarCadastroBairro(credorBairro, credorNome, alunoNome))) {
			codBai = insertBairro(credorBairro, credorNome, alunoNome);
			countBai = countBai.add(BigDecimal.ONE);
		}
		try {
			jdbc.openSession();

			String sqlP = "INSERT INTO TGFPAR(CODPARC, NOMEPARC, RAZAOSOCIAL ,TIPPESSOA, AD_ENDCREDOR, CODBAI, CODCID, "
					+ " CEP,TELEFONE, CGC_CPF, DTCAD, DTALTER, AD_FLAGALUNO) \t\tVALUES(?, ?, ?, ?, ?, NVL((select max(codbai) from tsibai where TRANSLATE( \t\t\t    upper(nomebai), \t\t\t    '������������������������������������', \t\t\t    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC' \t\t\t  ) like TRANSLATE( \t\t\t    upper(?), \t\t\t    '������������������������������������', \t\t\t    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC' \t\t\t  )), 0), (SELECT max(codcid) FROM tsicid WHERE TRANSLATE(              UPPER(descricaocorreio),               '������������������������������������',               'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')               LIKE TRANSLATE(UPPER(?),               '������������������������������������',               'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')               OR SUBSTR(UPPER(descricaocorreio),               1, INSTR(UPPER(descricaocorreio), ' ') - 1)               LIKE TRANSLATE(UPPER(?),               '������������������������������������',               'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')),  ?, ?, ?, SYSDATE, SYSDATE, 'S')";

			pstmt = jdbc.getPreparedStatement(sqlP);
			pstmt.setBigDecimal(1, atualCodparc);
			pstmt.setString(2, credorNome.toUpperCase());
			pstmt.setString(3, credorNome.toUpperCase());
			pstmt.setString(4, tipPessoa);

			pstmt.setString(5, credorEndereco);

			pstmt.setString(6, credorBairro);

			pstmt.setString(7, credorCidade.trim());
			pstmt.setString(8, credorCidade.trim());

			pstmt.setString(9, credorCep);
			pstmt.setString(10, credorCelular);
			pstmt.setString(11, credorCpf);

			pstmt.executeUpdate();
		} catch (SQLException e) {
			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro ao cadastrar credor: " + e.getMessage() +"', SYSDATE, 'Erro', "+codemp+", '"+credorNome+"' FROM DUAL");
			/*util.inserirLog("Erro ao cadastrar credor: " + e.getMessage(),
					"Erro", credorNome, codemp);*/
			e.printStackTrace();
			atualCodparc = null;
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
		return atualCodparc;
	}

	public void insertCursoTurma(String cursoDescricao, String cursoId,
			String turmaId, String credorNome, String alunoNome,
			BigDecimal codEmp) throws Exception {
		
		if ((cursoDescricao != null)
				&& (validarCadastroCurso(cursoDescricao, credorNome, alunoNome))) {
			// insertCurso(cursoDescricao, cursoId, credorNome, alunoNome);
		} else {
			//updateCurso(cursoDescricao, cursoId, credorNome, alunoNome);
		}
		
		if ((cursoDescricao != null)
				&& (validarCadastroCursoProj(cursoDescricao, credorNome,
						alunoNome, codEmp))) {
			insertCursoProj(cursoDescricao, credorNome, alunoNome, codEmp,
					cursoId);
		}
		
		if ((turmaId != null)
				&& (validarCadastroTurma(turmaId, credorNome, alunoNome,
						cursoDescricao, codEmp))) {
			insertCadastroTurma(turmaId, codEmp, cursoDescricao);
		}
	}

	public boolean validarCadastroCurso(String curso, String credorNome,
			String alunoNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal count = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TSICUS  WHERE TRANSLATE(UPPER (DESCRCENCUS), '������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') LIKE TRANSLATE (UPPER ('%"
					+ curso.trim()
					+ "%'), "
					+ "'������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			rs = pstmt.executeQuery();

			if (rs.next()) {
				count = rs.getBigDecimal("COUNT");
			}

		} catch (SQLException e) {
			e.printStackTrace();
			insertLogIntegracao(
					"Erro ao validar se curso j� cadastrado: " + e.getMessage(),
					"Erro", credorNome, alunoNome);
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
		if (count.compareTo(BigDecimal.ZERO) == 0) {
			return true;
		}
		return false;
	}

	public boolean validarCadastroCursoProj(String curso, String credorNome,
			String alunoNome, BigDecimal codEmp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal count = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TCSPRJ  WHERE TRANSLATE(UPPER (IDENTIFICACAO), '������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') LIKE TRANSLATE (UPPER ('"
					+ curso.trim()
					+ "'), "
					+ "'������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') AND CODPROJPAI = (SELECT MAX(CODPROJ) FROM TCSPRJ WHERE CODEMP = "
					+ codEmp + ")";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			rs = pstmt.executeQuery();
			if (rs.next()) {
				count = rs.getBigDecimal("COUNT");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			insertLogIntegracao(
					"Erro ao validar se curso j� cadastrado como projeto: "
							+ e.getMessage(), "Erro", credorNome, alunoNome);
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
		if (count.compareTo(BigDecimal.ZERO) == 0) {
			return true;
		}
		return false;
	}

	public boolean validarCadastroTurma(String turma, String credorNome,
			String alunoNome, String cursoNome, BigDecimal codEmp)
			throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal count = BigDecimal.ZERO;
		
		
		try {
			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TCSPRJ  WHERE TRANSLATE(UPPER (IDENTIFICACAO), '������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') LIKE TRANSLATE (UPPER ('%"
					+
					turma.trim()
					+ "%'), "
					+ "'������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') AND CODPROJPAI = (SELECT CODPROJ FROM TCSPRJ WHERE CODPROJPAI = (SELECT MAX(CODPROJ) FROM TCSPRJ WHERE CODEMP = "
					+ codEmp + ") AND TRANSLATE(UPPER (IDENTIFICACAO), '������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') LIKE TRANSLATE (UPPER ('"
					+
					cursoNome.trim()
					+ "'), "
					+ "'������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC'))";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			rs = pstmt.executeQuery();
			if (rs.next()) {
				count = rs.getBigDecimal("COUNT");
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
		if (count.compareTo(BigDecimal.ZERO) == 0) {
			return true;
		}
		return false;
	}

	public void insertCadastroTurma(String turmaId, BigDecimal codEmp,
			String cursoDescricao) throws Exception {
		
		EnviromentUtils util = new EnviromentUtils();
		
		String ativo = "S";
		String analitico = "N";
		
		if (turmaId.length() >= 20) {
			turmaId = turmaId.substring(0, 20).toUpperCase();
		} else {
			turmaId = turmaId.toUpperCase();
		}
		
		int grau = 3;
		
		try {
			
			util.insertCurso(cursoDescricao.trim(), codEmp, turmaId.trim(), ativo, analitico, grau);

		} catch (Exception se) {
			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro cadastrando turma: " + se.getMessage() +" Nome Turma: "+turmaId+"', SYSDATE, 'Erro', "+codEmp+", null FROM DUAL");
			
			/*util.inserirLog("Erro cadastrando turma: " + se.getMessage() + " Nome Turma: " + turmaId,
					"Erro", "", codEmp);*/
			se.printStackTrace();
		} finally {
			
		}
	}

	public void insertCurso(String cursoDescricao, String cursoId,
			String credorNome, String alunoNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		String ativo = "S";
		String analitico = "S";

		int codCenCusPai = 10101000;
		int grau = 4;

		System.out.println("ID_EXTERNO: " + cursoId);
		try {
			jdbc.openSession();

			String sqlUpdate = "INSERT INTO TSICUS(CODCENCUS, CODCENCUSPAI, DESCRCENCUS, ATIVO, ANALITICO, GRAU, AD_IDEXTERNO)VALUES ((SELECT MAX(CODCENCUS) + 1 FROM TSICUS WHERE CODCENCUSPAI = '10101000'), ? ,? , ?, ?, ?, ?)";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);

			pstmt.setInt(1, codCenCusPai);
			pstmt.setString(2, cursoDescricao);
			pstmt.setString(3, ativo);
			pstmt.setString(4, analitico);
			pstmt.setInt(5, grau);
			pstmt.setString(6, cursoId);
			pstmt.executeUpdate();
		} catch (Exception se) {
			insertLogIntegracao("Erro cadastrando curso: " + se.getMessage(),
					"Erro", credorNome, alunoNome);
			se.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public void updateCurso(String cursoDescricao, String cursoId,
			String credorNome, String alunoNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		System.out.println("ID_EXTERNO: " + cursoId);
		try {
			jdbc.openSession();

			String sqlUpdate = "UPDATE TSICUS SET AD_IDEXTERNO = '"
					+ cursoId
					+ "' WHERE CODCENCUS = (SELECT CODCENCUS FROM TSICUS  WHERE TRANSLATE(UPPER (DESCRCENCUS), '������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') LIKE TRANSLATE (UPPER ('%"
					+ cursoDescricao.trim()
					+ "'),  '������������������������������������', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC'))";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);

			pstmt.executeUpdate();
		} catch (Exception se) {
			insertLogIntegracao("Erro cadastrando curso: " + se.getMessage(),
					"Erro", credorNome, alunoNome);
			se.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
	}

	public BigDecimal getMaxCodProjPaiProj(BigDecimal codEmp) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal codProjPai = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlSelect = "select codproj from TCSPRJ where codemp = "
					+ codEmp;

			pstmt = jdbc.getPreparedStatement(sqlSelect);

			rs = pstmt.executeQuery();
			if (rs.next()) {
				codProjPai = rs.getBigDecimal("codproj");
			}
		} catch (Exception se) {
			insertLogIntegracao(
					"Erro cadastrando curso como projeto: " + se.getMessage(),
					"Erro", "", "");
			se.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
		return codProjPai;
	}

	public BigDecimal insertCursoProj(String cursoDescricao, String credorNome,
			String alunoNome, BigDecimal codEmp, String cursoId)
			throws Exception {
		
		
		EnviromentUtils util = new EnviromentUtils();
		
		BigDecimal codProjPai = getMaxCodProjPaiProj(codEmp);

		String ativo = "S";
		String analitico = "S";
		int grau = 2;

		String cursoAbrev = "";
		if (cursoDescricao.length() >= 20) {
			cursoAbrev = cursoDescricao.substring(0, 20).toUpperCase();
		} else {
			cursoAbrev = cursoDescricao.toUpperCase();
		}
		try {
			
			util.insertProjeto(codProjPai, cursoDescricao.toUpperCase().trim(), 
					cursoAbrev.toUpperCase().trim(), ativo, analitico, grau, cursoId);
			
		} catch (Exception se) {
			selectsParaInsert.add("SELECT <#NUMUNICO#>, 'Erro cadastrando curso como projeto: " + se.getMessage() +" Nome Curso: "+cursoDescricao+"', SYSDATE, 'Erro', "+codEmp+", null FROM DUAL");
			
			/*util.inserirLog(
					"Erro cadastrando curso como projeto: " + se.getMessage() +" Nome Curso: " + cursoDescricao,
					"Erro", "", codEmp);*/
			se.printStackTrace();
		} finally {
			
		}
		return null;
	}

	public void insertAluno(BigDecimal credotAtual, String alunoId,
	        String alunoNome, String alunoNomeSocial, String alunoEndereco,
	        String alunoCep, String alunoBairro, String alunoCidade,
	        String alunoUf, String alunoSexo, String alunoDataNascimento,
	        String alunoRg, String alunoCpf, String alunoCelular,
	        String alunoResidencial, String alunoEmail, String alunoSituacao,
	        String alunoSituacaoId, String credorNome, BigDecimal codEmp,
	        String descrCurso, String turmaId) throws Exception {

	    System.out.println("\n=== DEBUG INSERT ALUNO ===");
	    System.out.println("turmaId: " + turmaId);
	    System.out.println("descrCurso original: " + descrCurso);

	    if (credotAtual == null) {
	        throw new IllegalArgumentException("credotAtual não pode ser nulo");
	    }

	    if (descrCurso == null || descrCurso.trim().isEmpty()) {
	        throw new IllegalArgumentException("descrCurso não pode ser nulo ou vazio");
	    }

	    EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	    JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	    PreparedStatement pstmt = null;
	    PreparedStatement verifyStmt = null;
	    ResultSet rs = null;
	    BigDecimal codCenCus = null;

	    EnviromentUtils util = new EnviromentUtils();

	    if (alunoNomeSocial == null) {
	        alunoNomeSocial = " ";
	    }

	    try {
	        jdbc.openSession();

	        // Abordagem mais simples usando UPPER e removendo acentos
	        String verifyQuery = "SELECT codcencus, descrcencus FROM tsicus";
	        verifyStmt = jdbc.getPreparedStatement(verifyQuery);
	        rs = verifyStmt.executeQuery();
	        
	        // Normaliza o nome do curso (remove acentos, maiúsculas e espaços extras)
	        String normalizedDescrCurso = descrCurso
	            .toUpperCase()
	            .replaceAll("\\s+", " ")
	            .trim()
	            .replace("Á", "A")
	            .replace("À", "A")
	            .replace("Ã", "A")
	            .replace("Â", "A")
	            .replace("É", "E")
	            .replace("Ê", "E")
	            .replace("Í", "I")
	            .replace("Ó", "O")
	            .replace("Ô", "O")
	            .replace("Õ", "O")
	            .replace("Ú", "U")
	            .replace("Ç", "C");
	            
	        System.out.println("[DEBUG] descrCurso normalizado: " + normalizedDescrCurso);
	        
	        boolean cursoEncontrado = false;
	        System.out.println("[DEBUG] Buscando curso no banco. Cursos disponíveis:");
	        
	        while (rs.next()) {
	            String descricaoBanco = rs.getString("descrcencus");
	            String descricaoBancoNormalizada = descricaoBanco
	                .toUpperCase()
	                .replaceAll("\\s+", " ")
	                .trim()
	                .replace("Á", "A")
	                .replace("À", "A")
	                .replace("Ã", "A")
	                .replace("Â", "A")
	                .replace("É", "E")
	                .replace("Ê", "E")
	                .replace("Í", "I")
	                .replace("Ó", "O")
	                .replace("Ô", "O")
	                .replace("Õ", "O")
	                .replace("Ú", "U")
	                .replace("Ç", "C");
	                
	            System.out.println(" - Original: [" + descricaoBanco + "] Normalizado: [" + descricaoBancoNormalizada + "]");
	            
	            if (normalizedDescrCurso.equals(descricaoBancoNormalizada)) {
	                codCenCus = rs.getBigDecimal("codcencus");
	                System.out.println("[DEBUG] Curso encontrado! Código: " + codCenCus + " para: [" + descricaoBanco + "]");
	                cursoEncontrado = true;
	                break;
	            }
	        }
	        
	        if (!cursoEncontrado) {
	            // Tentar uma busca menos restritiva
	            rs.close();
	            verifyStmt.close();
	            
	            verifyQuery = "SELECT codcencus, descrcencus FROM tsicus WHERE UPPER(descrcencus) LIKE ?";
	            verifyStmt = jdbc.getPreparedStatement(verifyQuery);
	            verifyStmt.setString(1, "%" + normalizedDescrCurso.replace("CURSO TECNICO EM", "%") + "%");
	            rs = verifyStmt.executeQuery();
	            
	            System.out.println("[DEBUG] Fazendo busca menos restritiva com: " + 
	                normalizedDescrCurso.replace("CURSO TECNICO EM", "%"));
	            
	            if (rs.next()) {
	                codCenCus = rs.getBigDecimal("codcencus");
	                System.out.println("[DEBUG] Curso encontrado na busca menos restritiva! Código: " + 
	                    codCenCus + " para: [" + rs.getString("descrcencus") + "]");
	                cursoEncontrado = true;
	            } else {
	                throw new SQLException("Nenhum curso encontrado para a descrição: " + descrCurso);
	            }
	        }

	        String sqlP = "INSERT INTO AD_ALUNOS (CODPARC, ID_EXTERNO, NOME, NOME_SOCIAL, ENDERECO, CEP, BAIRRO, CIDADE, UF, SEXO, DATA_NASCIMENTO, RG, CPF, TELEFONE_CELULAR, TELEFONE_RESIDENCIAL, EMAIL, SITUACAO, SITUACAO_ID, CODEMP, CODCENCUS, TURMA) "
	                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT TO_CHAR(TO_DATE(?, 'yyyy-MM-dd'), 'dd/MM/yyyy') FROM dual), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

	        System.out.println("[DEBUG] Query SQL: " + sqlP);
	        System.out.println("[DEBUG] Valor de turmaId antes do INSERT: " + turmaId);

	        pstmt = jdbc.getPreparedStatement(sqlP);

	        pstmt.setBigDecimal(1, credotAtual);
	        pstmt.setString(2, alunoId);
	        pstmt.setString(3, alunoNome);
	        pstmt.setString(4, alunoNomeSocial);
	        pstmt.setString(5, alunoEndereco);
	        pstmt.setString(6, alunoCep);
	        pstmt.setString(7, alunoBairro);
	        pstmt.setString(8, alunoCidade);
	        pstmt.setString(9, alunoUf);
	        pstmt.setString(10, alunoSexo);
	        pstmt.setString(11, alunoDataNascimento);
	        pstmt.setString(12, alunoRg);
	        pstmt.setString(13, alunoCpf);
	        pstmt.setString(14, alunoCelular);
	        pstmt.setString(15, alunoResidencial);
	        pstmt.setString(16, alunoEmail);
	        pstmt.setString(17, alunoSituacao);
	        pstmt.setString(18, alunoSituacaoId);
	        pstmt.setBigDecimal(19, codEmp);
	        pstmt.setBigDecimal(20, codCenCus); // Usa o código do curso obtido na verificação
	        pstmt.setString(21, turmaId);

	        int rowsAffected = pstmt.executeUpdate();

	        System.out.println("[DEBUG] Número de linhas afetadas pelo INSERT: " + rowsAffected);
	        System.out.println("[DEBUG] Aluno inserido com sucesso na turma: " + turmaId);

	    } catch (SQLException e) {
	        String errorDetails = String.format(
	            "Erro ao inserir aluno. CODPARC: %s, ID_EXTERNO: %s, NOME: %s, Curso: %s, Erro: %s",
	            credotAtual, alunoId, alunoNome, descrCurso, e.getMessage()
	        );
	        selectsParaInsert.add("SELECT <#NUMUNICO#>, '" + errorDetails + "', SYSDATE, 'Erro', " + codEmp + ", '" + alunoId + "' FROM DUAL");

	        System.err.println("[DEBUG] Erro ao inserir aluno na turma: " + turmaId);
	        System.err.println("[DEBUG] Mensagem de erro: " + e.getMessage());
	        e.printStackTrace();
	        throw e;
	    } finally {
	        if (rs != null) {
	            rs.close();
	        }
	        if (verifyStmt != null) {
	            verifyStmt.close();
	        }
	        if (pstmt != null) {
	            pstmt.close();
	        }
	        jdbc.closeSession();
	    }
	}

	public boolean validarCadastroEndereco(String endereco, String credorNome,
			String alunoNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal count = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TSIEND WHERE TRANSLATE(  \t    upper(NOMEEND),   \t    '������������������������������������',   \t    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC'   \t  ) like TRANSLATE(    \t    upper((SELECT LTRIM(SUBSTR(?, INSTR(?, ' ') + 1))  FROM DUAL)),   \t    '������������������������������������',   \t    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setString(1, endereco);
			pstmt.setString(2, endereco);

			rs = pstmt.executeQuery();
			if (rs.next()) {
				count = rs.getBigDecimal("COUNT");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			insertLogIntegracao("Erro ao validar se endere�o j� cadastrado: "
					+ e.getMessage(), "Erro", credorNome, alunoNome);
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
		if (count.compareTo(BigDecimal.ZERO) == 0) {
			return true;
		}
		return false;
	}

	public boolean validarCadastroBairro(String bairro, String credorNome,
			String alunoNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal count = BigDecimal.ZERO;
		try {
			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TSIBAI WHERE TRANSLATE(  \t    upper(NOMEBAI),   \t    '������������������������������������',   \t    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC'   \t  ) like TRANSLATE(    \t    upper(?),   \t    '������������������������������������',   \t    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setString(1, bairro);

			rs = pstmt.executeQuery();
			if (rs.next()) {
				count = rs.getBigDecimal("COUNT");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			insertLogIntegracao(
					"Erro ao validar se bairro j� cadastrado: "
							+ e.getMessage(), "Erro", credorNome, alunoNome);
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
		if (count.compareTo(BigDecimal.ZERO) == 0) {
			return true;
		}
		return false;
	}

	public BigDecimal insertBairro(String bairro, String credorNome,
			String alunoNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		BigDecimal codbai = getMaxNumBai();

		jdbc.openSession();

		String sqlUpdate = "INSERT INTO TSIBAI (CODBAI, NOMEBAI, DTALTER)VALUES (?, ?, SYSDATE) ";

		pstmt = jdbc.getPreparedStatement(sqlUpdate);
		pstmt.setBigDecimal(1, codbai);
		pstmt.setString(2, bairro.toUpperCase());
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
			insertLogIntegracao("Erro ao bairro endere�o: " + se.getMessage(),
					"Erro", credorNome, alunoNome);
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
		return codbai;
	}

	public BigDecimal getMaxNumBai() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;
		try {
			updateNumBai();

			jdbc.openSession();

			String sqlNota = "SELECT MAX(ULTCOD) AS ID FROM TGFNUM WHERE ARQUIVO = 'TSIBAI'";

			pstmt = jdbc.getPreparedStatement(sqlNota);

			rs = pstmt.executeQuery();
			if (rs.next()) {
				id = rs.getBigDecimal("ID");
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

	public void updateNumBai() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String sqlUpdate = "UPDATE TGFNUM SET ULTCOD = ULTCOD + 1  WHERE ARQUIVO = 'TSIBAI'";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
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

	public void insertLogIntegracao(String descricao, String status,
			String credorNome, String alunoNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			jdbc.openSession();

			String descricaoCompleta = null;
			if ((credorNome.equals("")) && (alunoNome.equals(""))) {
				descricaoCompleta = descricao;
			} else if ((credorNome.equals("")) && (!alunoNome.isEmpty())) {
				descricaoCompleta = descricao + " " + " Aluno:" + alunoNome;
			} else if ((!credorNome.isEmpty()) && (alunoNome.equals(""))) {
				descricaoCompleta = descricao + " " + " Credor:" + credorNome;
			} else if ((!credorNome.isEmpty()) && (!alunoNome.isEmpty())) {
				descricaoCompleta = descricao + " " + " Credor:" + credorNome
						+ " " + " Aluno:" + alunoNome;
			}
			String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, STATUS)VALUES (((SELECT NVL(MAX(NUMUNICO), 0) + 1 FROM AD_LOGINTEGRACAO)), ?, SYSDATE, ?)";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.setString(1, descricaoCompleta);
			pstmt.setString(2, status);
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

	
}