package br.com.sankhya.acoesgrautec.jobs;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import br.com.sankhya.acoesgrautec.services.SkwServicoFinanceiro;
import br.com.sankhya.acoesgrautec.util.EnviromentUtils;
import br.com.sankhya.acoesgrautec.util.LogCatcher;
import br.com.sankhya.acoesgrautec.util.LogConfiguration;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.core.JapeSession;
import br.com.sankhya.jape.core.JapeSession.SessionHandle;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.jape.vo.DynamicVO;
import br.com.sankhya.jape.wrapper.JapeFactory;
import br.com.sankhya.jape.wrapper.JapeWrapper;
import br.com.sankhya.modelcore.MGEModelException;
import br.com.sankhya.modelcore.util.DynamicEntityNames;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.modelcore.util.SWRepositoryUtils;

public class JobGetCredorAlunoTurmaCurso implements ScheduledAction {

	@Override
	public void onTime(ScheduledActionContext arg0) {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal codEmp = BigDecimal.ZERO;

		String url = "";
		String token = "";

		System.out.println("teste");

		try {

			jdbc.openSession();

			String query = "SELECT CODEMP, URL, TOKEN FROM AD_LINKSINTEGRACAO WHERE CODEMP = 3";

			pstmt = jdbc.getPreparedStatement(query);

			rs = pstmt.executeQuery();

			while (rs.next()) {

				codEmp = rs.getBigDecimal("CODEMP");

				url = rs.getString("URL");
				token = rs.getString("TOKEN");

				iterarEndpoint(url, token, codEmp);

			}

		} catch (Exception e) {
			e.printStackTrace();
			try {
				insertLogIntegracao(
						"Erro ao integrar Alunos e/ou Credor, Mensagem de erro: "
								+ e.getMessage(), "Erro", "", "");
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

	public void iterarEndpoint(String url, String token, BigDecimal codEmp)
			throws Exception {

		int pagina = 1;
		int quantidade = 10;
		System.out.println("teste2");
		try {
			while (true) {

				String[] response = apiGet(url + "/alunos" + "?pagina="
						+ pagina + "&quantidade=" + quantidade, token);
				//
				int status = Integer.parseInt(response[0]);
				System.out.println("Status teste: " + status);
				System.out.println("pagina: " + pagina);
				//
				// // String[] response =
				// //
				// apiGet("https://api.acadweb.com.br/testegrautecanhagabausankhya/alunos?matricula=ENF230348");
				// // + "dataFinal=2023-04-24 06:36:00?" + "quantidade=10");
				// // String[] response =
				// //
				// apiGet("https://api.acadweb.com.br/testegrautecanhagabausankhya/alunos?pagina=4972&quantidade=1");
				//
				String responseString = response[1];
				System.out.println("response string: " + responseString);
				//
				if (responseString.equals("[]") || pagina == 2) {
					System.out.println("Entrou no if da quebra");
					break; // Sai do loop se a resposta estiver vazia
				}
				//
				cadastrarCredorAlunoCursoTurma(responseString);
				// pagina++;
				//
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void cadastrarCredorAlunoCursoTurma(String responseString) {
		try {
			JsonParser parser = new JsonParser();
			JsonArray jsonArray = parser.parse(responseString).getAsJsonArray();

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

				boolean credor = getIfCredorExist(credorCpf);
				boolean aluno = getIfAlunoExist(alunoCpf);

				if (alunoSituacaoId.equalsIgnoreCase("LFI")
						|| alunoSituacaoId.equalsIgnoreCase("LFR")
						|| alunoSituacaoId.equalsIgnoreCase("MT")
						|| alunoSituacaoId.equalsIgnoreCase("TF")
						|| alunoSituacaoId.equalsIgnoreCase("NC")) {

					System.out.println("entrou na validação de cadastro");

					if (credor == false) {
						BigDecimal credotAtual = insertCredor(credorNome,
								credorCpf, credorEndereco, credorCep,
								credorBairro, credorCidade, credorUf,
								credorResidencial, credorCelular,
								credorComercial, alunoNome);
						insertLogIntegracao("Credor Cadastrado: ", "Sucesso",
								credorNome, "");

						insertCursoTurma(cursoDescricao, cursoId, turmaId,
								credorNome, alunoNome);

						System.out.println("ID EXTERNO: " + cursoId);
						insertLogIntegracao("Curso e turma cadastrado",
								"Sucesso", credorNome, alunoNome);

						if (aluno == false) {

							insertAluno(credotAtual, alunoId, alunoNome,
									alunoNomeSocial, alunoEndereco, alunoCep,
									alunoBairro, alunoCidade, alunoUf,
									alunoSexo, alunoDataNascimento, alunoRg,
									alunoCpf, alunoCelular, alunoResidencial,
									alunoEmail, alunoSituacao, alunoSituacaoId,
									credorNome);
							insertLogIntegracao("Aluno Cadastro: ", "Sucesso",
									"", alunoNome);
						}

						insertLogIntegracao(
								"Finalizando chamada do endpoint de Alunos",
								"Sucesso", "", "");
					} else {
						insertLogIntegracao("Credor já cadastrado", "Aviso",
								credorNome, "");

						BigDecimal credorCadastrado = getCredorCadastrado(credorCpf);

						insertCursoTurma(cursoDescricao, cursoId, turmaId,
								credorNome, alunoNome);
						insertLogIntegracao("Curso e turma cadastrado",
								"Sucesso", "", "");

						if (aluno == false) {

							insertAluno(credorCadastrado, alunoId, alunoNome,
									alunoNomeSocial, alunoEndereco, alunoCep,
									alunoBairro, alunoCidade, alunoUf,
									alunoSexo, alunoDataNascimento, alunoRg,
									alunoCpf, alunoCelular, alunoResidencial,
									alunoEmail, alunoSituacao, alunoSituacaoId,
									credorNome);
							insertLogIntegracao("Aluno já Cadastro ", "Aviso",
									"", alunoNome);
						}

						insertLogIntegracao(
								"Finalizando chamada do endpoint de Alunos",
								"Sucesso", credorNome, alunoNome);
					}
				} else {
					System.out
							.println("Aluno com situação diferente do esperado");
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
			try {
				insertLogIntegracao(
						"Erro no chamado do endpoint: " + e.getMessage(),
						"Erro", "", "");
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

			updateTgfNumParc();

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
			jdbc.closeSession();
		}

		if (credor > 0) {
			return true;
		} else {
			return false;
		}

	}

	public boolean getIfAlunoExist(String alunoCpf) throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		int aluno = 0;

		try {

			updateTgfNumParc();

			jdbc.openSession();

			String sqlSlt = "SELECT COUNT(0) AS ALUNO FROM AD_ALUNOS WHERE CPF = ?";

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
			jdbc.closeSession();
		}

		if (aluno > 0) {
			return true;
		} else {
			return false;
		}

	}

	public BigDecimal getCredorCadastrado(String credorCpf) throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal credorCadastrado = BigDecimal.ZERO;

		try {

			updateTgfNumParc();

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
			jdbc.closeSession();
		}

		return bd;

	}

	public BigDecimal insertCredor(String credorNome, String credorCpf,
			String credorEndereco, String credorCep, String credorBairro,
			String credorCidade, String credorUf, String credorResidencial,
			String credorCelular, String credorComercial, String alunoNome)
			throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		BigDecimal atualCodparc = getMaxNumParc();

		String tipPessoa = "";
		BigDecimal numEnd = BigDecimal.ZERO;

		BigDecimal countEnd = BigDecimal.ZERO;
		BigDecimal countBai = BigDecimal.ZERO;
		BigDecimal codEnd = BigDecimal.ZERO;
		BigDecimal codBai = BigDecimal.ZERO;

		if (credorCpf.length() == 11) {
			tipPessoa = "F";
		} else if (credorCpf.length() == 14) {
			tipPessoa = "J";
		}

		// if (credorEndereco != null) {
		// if (validarCadastroEndereco(credorEndereco, credorNome)) {
		// codEnd = insertEndereco(credorEndereco, credorNome);
		// countEnd = countEnd.add(BigDecimal.ONE);
		// }
		// }

		if (credorBairro != null) {
			if (validarCadastroBairro(credorBairro, credorNome, alunoNome)) {
				codBai = insertBairro(credorBairro, credorNome, alunoNome);
				countBai = countBai.add(BigDecimal.ONE);
			}
		}

		try {
			jdbc.openSession();

			String sqlP = "INSERT INTO TGFPAR(CODPARC, NOMEPARC, RAZAOSOCIAL ,TIPPESSOA, AD_ENDCREDOR, CODBAI, CODCID, CEP,"
					+ "TELEFONE, CGC_CPF, DTCAD, DTALTER) "
					+ "		VALUES(?, ?, ?, ?, ?, NVL((select codbai from tsibai where TRANSLATE( "
					+ "			    upper(nomebai), "
					+ "			    'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', "
					+ "			    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC' "
					+ "			  ) like TRANSLATE( "
					+ "			    upper(?), "
					+ "			    'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', "
					+ "			    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC' "
					+ "			  )), 0), (SELECT codcid FROM tsicid WHERE TRANSLATE("
					+ "              UPPER(descricaocorreio), "
					+ "              'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', "
					+ "              'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') "
					+ "              LIKE TRANSLATE(UPPER(?), "
					+ "              'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', "
					+ "              'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') "
					+ "              OR SUBSTR(UPPER(descricaocorreio), "
					+ "              1, INSTR(UPPER(descricaocorreio), ' ') - 1) "
					+ "              LIKE TRANSLATE(UPPER(?), "
					+ "              'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', "
					+ "              'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')), "
					+ " ?, ?, ?, SYSDATE, SYSDATE)";

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
			insertLogIntegracao("Erro ao cadastrar credor: " + e.getMessage(),
					"Erro", credorNome, "");
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}

		return atualCodparc;
	}

	public void insertCursoTurma(String cursoDescricao, String cursoId,
			String turmaId, String credorNome, String alunoNome)
			throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		BigDecimal curso = BigDecimal.ZERO;
		System.out.println("curso: " + curso);
		BigDecimal cursoProj = BigDecimal.ZERO;
		System.out.println("cursoProj: " + cursoProj);
		BigDecimal turma = BigDecimal.ZERO;
		System.out.println("turma: " + turma);

		if (cursoDescricao != null) {
			if (validarCadastroCurso(cursoDescricao, credorNome, alunoNome)) {
				curso = insertCurso(cursoDescricao, cursoId, credorNome,
						alunoNome);
				System.out.println("curso2: " + curso);
			}
		}

		if (cursoDescricao != null) {
			if (validarCadastroCursoProj(cursoDescricao, credorNome, alunoNome)) {
				curso = insertCursoProj(cursoDescricao, credorNome, alunoNome);
				System.out.println("curso2: " + curso);
			}
		}

		if (turmaId != null) {
			if (validarCadastroTurma(turmaId, credorNome, alunoNome)) {
				cursoProj = getCursoProj(cursoDescricao, credorNome, alunoNome);
				System.out.println("cursoProj: " + cursoProj);
				if (cursoProj != null) {
					insertCadastroTurma(cursoProj, turmaId);
				}
			}
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

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TSICUS  "
					+ "WHERE TRANSLATE(UPPER (DESCRCENCUS), 'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', "
					+ "'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') "
					+ "LIKE TRANSLATE (UPPER ('%"
					+ curso
					+ "%'), "
					+ "'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			rs = pstmt.executeQuery();

			if (rs.next()) {

				count = rs.getBigDecimal("COUNT");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			insertLogIntegracao(
					"Erro ao validar se curso já cadastrado: " + e.getMessage(),
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
		} else {
			return false;
		}

	}

	public BigDecimal getCursoProj(String curso, String credorNome,
			String alunoNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal cursoProj = BigDecimal.ZERO;

		try {

			jdbc.openSession();

			String sqlNota = "SELECT CODPROJ FROM TCSPRJ  "
					+ "WHERE TRANSLATE(UPPER (IDENTIFICACAO), 'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', "
					+ "'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') "
					+ "LIKE TRANSLATE (UPPER ('%"
					+ curso
					+ "%'), "
					+ "'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			rs = pstmt.executeQuery();

			if (rs.next()) {

				cursoProj = rs.getBigDecimal("CODPROJ");
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

		return cursoProj;
	}

	public boolean validarCadastroCursoProj(String curso, String credorNome,
			String alunoNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal count = BigDecimal.ZERO;

		try {

			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TCSPRJ  "
					+ "WHERE TRANSLATE(UPPER (IDENTIFICACAO), 'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', "
					+ "'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') "
					+ "LIKE TRANSLATE (UPPER ('%"
					+ curso
					+ "%'), "
					+ "'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			rs = pstmt.executeQuery();

			if (rs.next()) {

				count = rs.getBigDecimal("COUNT");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			insertLogIntegracao(
					"Erro ao validar se curso já cadastrado como projeto: "
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
		} else {
			return false;
		}
	}

	public boolean validarCadastroTurma(String turma, String credorNome,
			String alunoNome) throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal count = BigDecimal.ZERO;

		try {

			jdbc.openSession();

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TCSPRJ  "
					+ "WHERE TRANSLATE(UPPER (IDENTIFICACAO), 'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', "
					+ "'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC') "
					+ "LIKE TRANSLATE (UPPER ('%"
					+ turma
					+ "%'), "
					+ "'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', 'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')";

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
		} else {
			return false;
		}

	}

	public void insertCadastroTurma(BigDecimal cursoProj, String turmaId)
			throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		String ativo = "S";
		String analitico = "N";

		int grau = 3;

		jdbc.openSession();

		String sqlUpdate = "INSERT INTO TCSPRJ"
				+ "(CODPROJ, CODPROJPAI ,IDENTIFICACAO, ABREVIATURA, ATIVO, ANALITICO, GRAU)"
				+ "VALUES ((SELECT (MAX(CASE  WHEN CODPROJPAI IS NULL THEN "
				+ "(SELECT (MAX(CODPROJ)) FROM TCSPRJ WHERE CODPROJ = "
				+ "? )ELSE CODPROJPAI END) + 1) AS NOVATURMA FROM TCSPRJ), ?, ? , ?, ?, ?, ?)";

		pstmt = jdbc.getPreparedStatement(sqlUpdate);

		pstmt.setBigDecimal(1, cursoProj);
		pstmt.setBigDecimal(2, cursoProj);
		pstmt.setString(3, turmaId);
		if (turmaId.length() >= 20) {
			pstmt.setString(4, turmaId.substring(0, 20));
		} else {

			pstmt.setString(4, turmaId);
		}
		pstmt.setString(5, ativo);
		pstmt.setString(6, analitico);
		pstmt.setInt(7, grau);
		pstmt.executeUpdate();

		try {
			if (pstmt != null) {
				pstmt.close();
			}
			if (jdbc != null) {
				jdbc.closeSession();
			}
		} catch (Exception se) {
			insertLogIntegracao("Erro cadastrando turma: " + se.getMessage(),
					"Erro", "", "");
			se.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}

	}

	public BigDecimal insertCurso(String cursoDescricao, String cursoId,
			String credorNome, String alunoNome) throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		// BigDecimal cursoNovo = BigDecimal.ZERO;

		String ativo = "S";
		String analitico = "S";

		int codCenCusPai = 10101000;
		int grau = 4;

		System.out.println("ID_EXTERNO: " + cursoId);
		jdbc.openSession();

		String sqlUpdate = "INSERT INTO TSICUS"
				+ "(CODCENCUS, CODCENCUSPAI, DESCRCENCUS, ATIVO, ANALITICO, GRAU, AD_IDEXTERNO)"
				+ "VALUES ((SELECT MAX(CODCENCUS) + 1 FROM TSICUS "
				+ "WHERE CODCENCUSPAI = '10101000'), ? ,? , ?, ?, ?, ?)";

		pstmt = jdbc.getPreparedStatement(sqlUpdate);

		pstmt.setInt(1, codCenCusPai);
		pstmt.setString(2, cursoDescricao);
		pstmt.setString(3, ativo);
		pstmt.setString(4, analitico);
		pstmt.setInt(5, grau);
		pstmt.setString(6, cursoId);
		pstmt.executeUpdate();

		try {
			if (pstmt != null) {
				pstmt.close();
			}
			if (jdbc != null) {
				jdbc.closeSession();
			}
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
		return null;

	}

	public BigDecimal insertCursoProj(String cursoDescricao, String credorNome,
			String alunoNome) throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		// BigDecimal cursoNovo = getMaxNumProjPai();
		int codProjPai = 20000000;

		String ativo = "S";
		String analitico = "S";
		int grau = 2;

		jdbc.openSession();

		String sqlUpdate = "INSERT INTO TCSPRJ"
				+ "(CODPROJ, CODPROJPAI ,IDENTIFICACAO, ABREVIATURA ,ATIVO, ANALITICO, GRAU)"
				+ "VALUES ((SELECT MAX(CODPROJ) + 1000 AS CURSONOVO FROM TCSPRJ WHERE CODPROJ = '20000000' "
				+ "AND GRAU = '1' ) , ? , ?, ?, ?, ?, ?)";

		pstmt = jdbc.getPreparedStatement(sqlUpdate);
		// pstmt.setBigDecimal(1, cursoNovo);
		pstmt.setInt(1, codProjPai);
		pstmt.setString(2, cursoDescricao);
		if (cursoDescricao.length() >= 20) {
			pstmt.setString(3, cursoDescricao.substring(0, 20));
		} else {

			pstmt.setString(3, cursoDescricao);
		}
		pstmt.setString(4, ativo);
		pstmt.setString(5, analitico);
		pstmt.setInt(6, grau);
		pstmt.executeUpdate();

		try {
			if (pstmt != null) {
				pstmt.close();
			}
			if (jdbc != null) {
				jdbc.closeSession();
			}
		} catch (Exception se) {
			insertLogIntegracao(
					"Erro cadastrando curso como projeto: " + se.getMessage(),
					"Erro", "", "");
			se.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}
		return null;

	}

	// public BigDecimal getMaxNumProj() throws Exception {
	//
	// EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
	// JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
	// PreparedStatement pstmt = null;
	// ResultSet rs = null;
	// BigDecimal bd = BigDecimal.ZERO;
	//
	// try {
	//
	// updateTgfNumParc();
	//
	// jdbc.openSession();
	//
	// String sqlUpd = "select (max(codproj) + 10000000) as atualCurso  "
	// + "from TCSPRJ where codprojpai = '-999999999' or codproj = 0";
	//
	// pstmt = jdbc.getPreparedStatement(sqlUpd);
	// rs = pstmt.executeQuery();
	// while (rs.next()) {
	//
	// bd = rs.getBigDecimal("atualCurso");
	//
	// }
	//
	// } catch (SQLException e) {
	// e.printStackTrace();
	// } finally {
	// if (pstmt != null) {
	// pstmt.close();
	// }
	// jdbc.closeSession();
	// }
	//
	// return bd;
	//
	// }

	public void insertAluno(BigDecimal credotAtual, String alunoId,
			String alunoNome, String alunoNomeSocial, String alunoEndereco,
			String alunoCep, String alunoBairro, String alunoCidade,
			String alunoUf, String alunoSexo, String alunoDataNascimento,
			String alunoRg, String alunoCpf, String alunoCelular,
			String alunoResidencial, String alunoEmail, String alunoSituacao,
			String alunoSituacaoId, String credorNome) throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		if (alunoNomeSocial == null) {
			alunoNomeSocial = " ";
		}

		try {
			jdbc.openSession();

			String sqlP = "INSERT INTO AD_ALUNOS ( CODPARC, ID_EXTERNO, NOME, NOME_SOCIAL, ENDERECO, "
					+ "CEP, BAIRRO, CIDADE, UF, SEXO, DATA_NASCIMENTO, RG, CPF, TELEFONE_CELULAR, TELEFONE_RESIDENCIAL, "
					+ "EMAIL, SITUACAO, SITUACAO_ID ) "
					+ "	VALUES "
					+ "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT TO_CHAR(TO_DATE(?, 'yyyy-MM-dd'), 'dd/MM/yyyy') FROM dual), ?, ?, ?, ?, ?, ?, ?)";

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
			pstmt.executeUpdate();

		} catch (SQLException e) {
			insertLogIntegracao("Erro cadastrando aluno: " + e.getMessage(),
					"Erro", "", alunoNome);
			e.printStackTrace();
		} finally {
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

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TSIEND WHERE TRANSLATE(  "
					+ "	    upper(NOMEEND),   "
					+ "	    'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ',   "
					+ "	    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC'   "
					+ "	  ) like TRANSLATE(    "
					+ "	    upper((SELECT LTRIM(SUBSTR(?, INSTR(?, ' ') + 1))  FROM DUAL)),   "
					+ "	    'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ',   "
					+ "	    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setString(1, endereco);
			pstmt.setString(2, endereco);

			rs = pstmt.executeQuery();

			if (rs.next()) {

				count = rs.getBigDecimal("COUNT");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			insertLogIntegracao("Erro ao validar se endereço já cadastrado: "
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
		} else {
			return false;
		}

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

			String sqlNota = "SELECT COUNT(0) AS COUNT FROM TSIBAI WHERE TRANSLATE(  "
					+ "	    upper(NOMEBAI),   "
					+ "	    'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ',   "
					+ "	    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC'   "
					+ "	  ) like TRANSLATE(    "
					+ "	    upper(?),   "
					+ "	    'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ',   "
					+ "	    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC')";

			pstmt = jdbc.getPreparedStatement(sqlNota);
			pstmt.setString(1, bairro);

			rs = pstmt.executeQuery();

			if (rs.next()) {

				count = rs.getBigDecimal("COUNT");
			}
		} catch (SQLException e) {
			e.printStackTrace();
			insertLogIntegracao(
					"Erro ao validar se bairro já cadastrado: "
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
		} else {
			return false;
		}

	}

	public BigDecimal insertBairro(String bairro, String credorNome,
			String alunoNome) throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		BigDecimal codbai = getMaxNumBai();

		jdbc.openSession();

		String sqlUpdate = "INSERT INTO TSIBAI (CODBAI, NOMEBAI, DTALTER)"
				+ "VALUES (?, ?, SYSDATE) ";

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
			insertLogIntegracao("Erro ao bairro endereço: " + se.getMessage(),
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

		jdbc.openSession();

		String sqlUpdate = "UPDATE TGFNUM SET ULTCOD = ULTCOD + 1  WHERE ARQUIVO = 'TSIBAI'";

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

		jdbc.openSession();

		String descricaoCompleta = null;

		if (credorNome.equals("") && alunoNome.equals("")) {
			descricaoCompleta = descricao;
		} else if (credorNome.equals("") && !alunoNome.isEmpty()) {
			descricaoCompleta = descricao + " " + " Aluno:" + alunoNome;
		} else if (!credorNome.isEmpty() && alunoNome.equals("")) {
			descricaoCompleta = descricao + " " + " Credor:" + credorNome;
		} else if (!credorNome.isEmpty() && !alunoNome.isEmpty()) {
			descricaoCompleta = descricao + " " + " Credor:" + credorNome + " "
					+ " Aluno:" + alunoNome;
		}

		String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, STATUS)"
				+ "VALUES (((SELECT NVL(MAX(NUMUNICO), 0) + 1 FROM AD_LOGINTEGRACAO)), ?, SYSDATE, ?)";

		pstmt = jdbc.getPreparedStatement(sqlUpdate);
		pstmt.setString(1, descricaoCompleta);
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
		} finally {
			if (pstmt != null) {
				pstmt.close();
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

		System.out.println("Entrou na API cleiton");
		System.out.println("URL: " + ur);
		System.out.println("https: " + https);
		System.out.println("token: " + token);
		System.out.println("Passou do token");

		https.setRequestMethod("GET");
		https.setRequestProperty("User-Agent",
				"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)");
		https.setRequestProperty("Content-Type",
				"application/json; charset=UTF-8");
		https.setRequestProperty("Authorization", "Bearer "
				+ "2|kLyB1dv4cuyNOiCVgwGYus60QstshlOBN7pPV0JF");
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

		System.out.println("Response apiget: " + response);

		return new String[] { Integer.toString(status), response };

	}
}