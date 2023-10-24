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

		try {
			insertLogIntegracao("Iniciando chamada do endpoint de Alunos",
					"Sucesso", " ");

			String[] response = apiGet("https://api.acadweb.com.br/testegrautboavistasankhya"
					+ "/alunos");
			// + "?" // + "dataInicial=2023-04-19 06:36:00?"
			// + "dataFinal=2023-04-24 06:36:00?" + "quantidade=10");

			String responseString = response[1];

			/*
			 * String responseString = "[\n" + "  {\n" +
			 * "    \"aluno_id\": \"CDI130001\",\n" +
			 * "    \"aluno_nome\": \"Luzia Barbosa do Nascimento\",\n" +
			 * "    \"aluno_nome_social\": null,\n" +
			 * "    \"aluno_pai\": \"Jose Manoel do Nascimento\",\n" +
			 * "    \"aluno_mae\": \"Iraci Barbosa do Nascimento\",\n" +
			 * "    \"aluno_endereco\": \"Rua Aracatiara 155\",\n" +
			 * "    \"aluno_endereco_cep\": \"52110440\",\n" +
			 * "    \"aluno_endereco_bairro\": \"Mangabeira\",\n" +
			 * "    \"aluno_endereco_cidade\": \"Recife\",\n" +
			 * "    \"aluno_endereco_uf\": \"PE\",\n" +
			 * "    \"aluno_sexo\": \"F\",\n" +
			 * "    \"aluno_data_nascimento\": \"1979-12-13\",\n" +
			 * "    \"aluno_uf_nascimento\": \"PE\",\n" +
			 * "    \"aluno_naturalidade\": \"Recife\",\n" +
			 * "    \"aluno_nacionalidade\": \"Brasileira\",\n" +
			 * "    \"aluno_tipo_sanguineo\": null,\n" +
			 * "    \"aluno_raca_id\": null,\n" +
			 * "    \"aluno_raca_descricao\": null,\n" +
			 * "    \"aluno_rg\": \"6607432\",\n" +
			 * "    \"aluno_rg_orgao\": \"SDS\",\n" +
			 * "    \"aluno_rg_data_expedicao\": null,\n" +
			 * "    \"aluno_cpf\": \"07026794450\",\n" +
			 * "    \"aluno_titulo_eleitoral\": null,\n" +
			 * "    \"aluno_titulo_zona\": null,\n" +
			 * "    \"aluno_titulo_secao\": null,\n" +
			 * "    \"aluno_reservista\": null,\n" +
			 * "    \"aluno_reservista_categoria\": null,\n" +
			 * "    \"aluno_telefone_residencial\": \"008185296881\",\n" +
			 * "    \"aluno_telefone_celular\": \"008130330754\",\n" +
			 * "    \"aluno_telefone_comercial\": \"008186341564\",\n" +
			 * "    \"aluno_telefone_fax\": null,\n" +
			 * "    \"aluno_email\": null,\n" +
			 * "    \"aluno_emancipado\": \"S\",\n" +
			 * "    \"aluno_grau_instrucao_id\": \"03\",\n" +
			 * "    \"aluno_grau_instrucao_descricao\": \"Ensino Médio\",\n" +
			 * "    \"aluno_profissao_id\": \"044\",\n" +
			 * "    \"aluno_profissoa_descricao\": \"Outras\",\n" +
			 * "    \"aluno_religiao_id\": null,\n" +
			 * "    \"aluno_religiao_descricao\": null,\n" +
			 * "    \"aluno_instituicao_2grau_id\": null,\n" +
			 * "    \"aluno_instituicao_2grau_descricao\": null,\n" +
			 * "    \"credor_nome\": \"Luzia Barbosa do Nascimento\",\n" +
			 * "    \"credor_cpf\": \"07026794450\",\n" +
			 * "    \"credor_endereco\": \"Rua Aracatiara 155\",\n" +
			 * "    \"credor_endereco_cep\": \"52110440\",\n" +
			 * "    \"credor_endereco_bairro\": \"Mangabeira\",\n" +
			 * "    \"credor_endereco_cidade\": \"Recife\",\n" +
			 * "    \"credor_endereco_uf\": \"PE\",\n" +
			 * "    \"credor_telefone_residencial\": \"008185296881\",\n" +
			 * "    \"credor_telefone_celular\": \"008130330754\",\n" +
			 * "    \"credor_telefone_comercial\": \"008186341564\",\n" +
			 * "    \"credor_telefone_fax\": null,\n" +
			 * "    \"ativo\": \"I\",\n" +
			 * "    \"data_atualizacao\": \"2013-03-19 00:00:00.000\",\n" +
			 * "    \"cursos\": [\n" + "      {\n" +
			 * "        \"turma_id\": null,\n" +
			 * "        \"curso_id\": \"00022\",\n" +
			 * "        \"curso_descricao\": \"Formação Complementar em Cuidados com Idosos\"\n"
			 * + "      }\n" + "    ]\n" + "  },\n" + "  {\n" +
			 * "    \"aluno_id\": \"CDI130002\",\n" +
			 * "    \"aluno_nome\": \"Maria Gorette de Oliveira Beltrao\",\n" +
			 * "    \"aluno_nome_social\": null,\n" +
			 * "    \"aluno_pai\": \"Fausto de Oliveira Beltrao\",\n" +
			 * "    \"aluno_mae\": \"Edite Severina de Oliveira\",\n" +
			 * "    \"aluno_endereco\": \"Rua 03, 29\",\n" +
			 * "    \"aluno_endereco_cep\": \"55816560\",\n" +
			 * "    \"aluno_endereco_bairro\": \"COHAB 02\",\n" +
			 * "    \"aluno_endereco_cidade\": \"Carpina\",\n" +
			 * "    \"aluno_endereco_uf\": \"PE\",\n" +
			 * "    \"aluno_sexo\": \"F\",\n" +
			 * "    \"aluno_data_nascimento\": \"1968-10-13\",\n" +
			 * "    \"aluno_uf_nascimento\": \"PE\",\n" +
			 * "    \"aluno_naturalidade\": \"Igarassu\",\n" +
			 * "    \"aluno_nacionalidade\": \"Brasileiro\",\n" +
			 * "    \"aluno_tipo_sanguineo\": null,\n" +
			 * "    \"aluno_raca_id\": null,\n" +
			 * "    \"aluno_raca_descricao\": null,\n" +
			 * "    \"aluno_rg\": \"3059803\",\n" +
			 * "    \"aluno_rg_orgao\": \"SDS\",\n" +
			 * "    \"aluno_rg_data_expedicao\": null,\n" +
			 * "    \"aluno_cpf\": \"46502971415\",\n" +
			 * "    \"aluno_titulo_eleitoral\": null,\n" +
			 * "    \"aluno_titulo_zona\": null,\n" +
			 * "    \"aluno_titulo_secao\": null,\n" +
			 * "    \"aluno_reservista\": null,\n" +
			 * "    \"aluno_reservista_categoria\": null,\n" +
			 * "    \"aluno_telefone_residencial\": \"008196758651\",\n" +
			 * "    \"aluno_telefone_celular\": \"008198212116\",\n" +
			 * "    \"aluno_telefone_comercial\": null,\n" +
			 * "    \"aluno_telefone_fax\": null,\n" +
			 * "    \"aluno_email\": \"gorete42@hotmail.com\",\n" +
			 * "    \"aluno_emancipado\": \"S\",\n" +
			 * "    \"aluno_grau_instrucao_id\": \"03\",\n" +
			 * "    \"aluno_grau_instrucao_descricao\": \"Ensino Médio\",\n" +
			 * "    \"aluno_profissao_id\": \"027\",\n" +
			 * "    \"aluno_profissoa_descricao\": \"Estudante\",\n" +
			 * "    \"aluno_religiao_id\": null,\n" +
			 * "    \"aluno_religiao_descricao\": null,\n" +
			 * "    \"aluno_instituicao_2grau_id\": null,\n" +
			 * "    \"aluno_instituicao_2grau_descricao\": null,\n" +
			 * "    \"credor_nome\": \"Maria Gorette de Oliveira Beltrao\",\n" +
			 * "    \"credor_cpf\": \"46502971415\",\n" +
			 * "    \"credor_endereco\": \"Rua 03, 29\",\n" +
			 * "    \"credor_endereco_cep\": \"55816560\",\n" +
			 * "    \"credor_endereco_bairro\": \"COHAB 02\",\n" +
			 * "    \"credor_endereco_cidade\": \"Carpina\",\n" +
			 * "    \"credor_endereco_uf\": \"PE\",\n" +
			 * "    \"credor_telefone_residencial\": \"008196758651\",\n" +
			 * "    \"credor_telefone_celular\": \"008198212116\",\n" +
			 * "    \"credor_telefone_comercial\": null,\n" +
			 * "    \"credor_telefone_fax\": null,\n" +
			 * "    \"ativo\": \"I\",\n" +
			 * "    \"data_atualizacao\": \"2013-03-27 00:00:00.000\",\n" +
			 * "    \"cursos\": [\n" + "      {\n" +
			 * "        \"turma_id\": null,\n" +
			 * "        \"curso_id\": \"00022\",\n" +
			 * "        \"curso_descricao\": \"Formação Complementar em Cuidados com Idosos\"\n"
			 * + "      }\n" + "    ]\n" + "  },\n" + "  {\n" +
			 * "    \"aluno_id\": \"CDI130003\",\n" +
			 * "    \"aluno_nome\": \"Palloma Dulce de Sousa\",\n" +
			 * "    \"aluno_nome_social\": null,\n" +
			 * "    \"aluno_pai\": \"William Alexandre de Souza\",\n" +
			 * "    \"aluno_mae\": \"Ivanilda Dulce da Silva Souza\",\n" +
			 * "    \"aluno_endereco\": \"R Doutor Machado 548\",\n" +
			 * "    \"aluno_endereco_cep\": \"52040020\",\n" +
			 * "    \"aluno_endereco_bairro\": \"Campo Grande\",\n" +
			 * "    \"aluno_endereco_cidade\": \"Recife\",\n" +
			 * "    \"aluno_endereco_uf\": \"PE\",\n" +
			 * "    \"aluno_sexo\": \"F\",\n" +
			 * "    \"aluno_data_nascimento\": \"1993-10-12\",\n" +
			 * "    \"aluno_uf_nascimento\": \"PE\",\n" +
			 * "    \"aluno_naturalidade\": \"Recife\",\n" +
			 * "    \"aluno_nacionalidade\": \"Brasileira\",\n" +
			 * "    \"aluno_tipo_sanguineo\": null,\n" +
			 * "    \"aluno_raca_id\": null,\n" +
			 * "    \"aluno_raca_descricao\": null,\n" +
			 * "    \"aluno_rg\": \"7464564\",\n" +
			 * "    \"aluno_rg_orgao\": \"sds\",\n" +
			 * "    \"aluno_rg_data_expedicao\": null,\n" +
			 * "    \"aluno_cpf\": \"06641841464\",\n" +
			 * "    \"aluno_titulo_eleitoral\": null,\n" +
			 * "    \"aluno_titulo_zona\": null,\n" +
			 * "    \"aluno_titulo_secao\": null,\n" +
			 * "    \"aluno_reservista\": null,\n" +
			 * "    \"aluno_reservista_categoria\": null,\n" +
			 * "    \"aluno_telefone_residencial\": \"008134273176\",\n" +
			 * "    \"aluno_telefone_celular\": \"008186538027\",\n" +
			 * "    \"aluno_telefone_comercial\": \"008197955990\",\n" +
			 * "    \"aluno_telefone_fax\": null,\n" +
			 * "    \"aluno_email\": \"pallomadulce@hotmail.com\",\n" +
			 * "    \"aluno_emancipado\": \"S\",\n" +
			 * "    \"aluno_grau_instrucao_id\": \"03\",\n" +
			 * "    \"aluno_grau_instrucao_descricao\": \"Ensino Médio\",\n" +
			 * "    \"aluno_profissao_id\": \"027\",\n" +
			 * "    \"aluno_profissoa_descricao\": \"Estudante\",\n" +
			 * "    \"aluno_religiao_id\": null,\n" +
			 * "    \"aluno_religiao_descricao\": null,\n" +
			 * "    \"aluno_instituicao_2grau_id\": null,\n" +
			 * "    \"aluno_instituicao_2grau_descricao\": null,\n" +
			 * "    \"credor_nome\": \"Palloma Dulce de Sousa\",\n" +
			 * "    \"credor_cpf\": \"06641841464\",\n" +
			 * "    \"credor_endereco\": \"R Doutor Machado 548\",\n" +
			 * "    \"credor_endereco_cep\": \"52040020\",\n" +
			 * "    \"credor_endereco_bairro\": \"Campo Grande\",\n" +
			 * "    \"credor_endereco_cidade\": \"Recife\",\n" +
			 * "    \"credor_endereco_uf\": \"PE\",\n" +
			 * "    \"credor_telefone_residencial\": \"008134273176\",\n" +
			 * "    \"credor_telefone_celular\": \"008186538027\",\n" +
			 * "    \"credor_telefone_comercial\": \"008197955990\",\n" +
			 * "    \"credor_telefone_fax\": null,\n" +
			 * "    \"ativo\": \"I\",\n" +
			 * "    \"data_atualizacao\": \"2013-03-28 00:00:00.000\",\n" +
			 * "    \"cursos\": [\n" + "      {\n" +
			 * "        \"turma_id\": null,\n" +
			 * "        \"curso_id\": \"00022\",\n" +
			 * "        \"curso_descricao\": \"Formação Complementar em Cuidados com Idosos\"\n"
			 * + "      }\n" + "    ]\n" + "  }]";
			 */

			/*
			 * "[\n" + "  {\n" + "    \"aluno_id\": \"ADM200026\",\n" +
			 * "    \"aluno_nome\": \"Fernando Figueiredo\",\n" +
			 * "    \"aluno_nome_social\": null ,\n" +
			 * "    \"aluno_mae\": \"Clarice Mariana Vitória\",\n" +
			 * "    \"aluno_pai\": \"Lorenzo Guilherme Cláudio Figueiredo\",\n"
			 * + "    \"aluno_endereco\": \"Rua Calêndula\",\n" +
			 * "    \"aluno_endereco_cep\": \"69099524\",\n" +
			 * "    \"aluno_endereco_bairro\": \"Bairro Novo\",\n" +
			 * "    \"aluno_endereco_cidade\": \"Recife\",\n" +
			 * "    \"aluno_endereco_uf\": \"PE\",\n" +
			 * "    \"aluno_sexo\": \"F\",\n" +
			 * "    \"aluno_data_nascimento\": \"2000-02-09\",\n" +
			 * "    \"aluno_uf_nascimento\": \"PE\",\n" +
			 * "    \"aluno_naturalidade\": \"Recife\",\n" +
			 * "    \"aluno_nacionalidade\": \"Brasileira\",\n" +
			 * "    \"aluno_tipo_sanguineo\": null,\n" +
			 * "    \"aluno_raca_id\": null,\n" +
			 * "    \"aluno_raca_descricao\": null,\n" +
			 * "    \"aluno_rg\": \"398059949\",\n" +
			 * "    \"aluno_rg_orgao\": \"SDS PE\",\n" +
			 * "    \"aluno_rg_data_expedicao\": null,\n" +
			 * "    \"aluno_cpf\": \"67175535708\",\n" +
			 * "    \"aluno_titulo_eleitoral\": null,\n" +
			 * "    \"aluno_titulo_zona\": null,\n" +
			 * "    \"aluno_titulo_secao\": null,\n" +
			 * "    \"aluno_reservista\": null,\n" +
			 * "    \"aluno_reservista_categoria\": null,\n" +
			 * "    \"aluno_telefone_residencial\": \"8138660962\",\n" +
			 * "    \"aluno_telefone_celular\": \"81991147250\",\n" +
			 * "    \"aluno_telefone_comercial\": null,\n" +
			 * "    \"aluno_telefone_fax\": null,\n" +
			 * "    \"aluno_email\": \"exemplo@email.com.br\",\n" +
			 * "    \"aluno_emancipado\": \"S\",\n" +
			 * "    \"aluno_grau_instrucao_id\": \"03\",\n" +
			 * "    \"aluno_grau_instrucao_descricao\": \"Ensino Médio\",\n" +
			 * "    \"aluno_profissao_id\": \"027\",\n" +
			 * "    \"aluno_profissoa_descricao\": \"Estudante\",\n" +
			 * "    \"aluno_religiao_id\": null,\n" +
			 * "    \"aluno_religiao_descricao\": null,\n" +
			 * "    \"aluno_instituicao_2grau_id\": null,\n" +
			 * "    \"aluno_instituicao_2grau_descricao\": null,\n" +
			 * "    \"credor_nome\": \"Kauê Noah Yago Porto\",\n" +
			 * "    \"credor_cpf\": \"03716471003\",\n" +
			 * "    \"credor_endereco\": \"Rua Pedro Jusselino de Aquino\",\n" +
			 * "    \"credor_endereco_cep\": \"58052370\",\n" +
			 * "    \"credor_endereco_bairro\": \"Jardim Universitário\",\n" +
			 * "    \"credor_endereco_cidade\": \"Recife\",\n" +
			 * "    \"credor_endereco_uf\": \"PE\",\n" +
			 * "    \"credor_telefone_residencial\": \"8129463856\",\n" +
			 * "    \"credor_telefone_celular\": \"81999849284\",\n" +
			 * "    \"credor_telefone_comercial\": null,\n" +
			 * "    \"credor_telefone_fax\": null,\n" +
			 * "    \"ativo\": \"I\",\n" + "    \"cursos\": [\n" + "      {\n" +
			 * "        \"curso_id\":null,\n" +
			 * "        \"curso_descricao\": \"Técnico em Administração\",\n" +
			 * "        \"turma_id\": \"ADM01\"\n" + "      }\n" + "    ]\n" +
			 * "  }\n" + "]";
			 */

			JsonParser parser = new JsonParser();
			JsonArray jsonArray = parser.parse(responseString).getAsJsonArray();

			for (JsonElement jsonElement : jsonArray) {
				JsonObject jsonObject = jsonElement.getAsJsonObject();

				String alunoId = jsonObject.get("aluno_id").isJsonNull() ? null
						: jsonObject.get("aluno_id").getAsString();
				String alunoNome = jsonObject.get("aluno_nome").isJsonNull() ? null
						: jsonObject.get("aluno_nome").getAsString();
				String alunoNomeSocial = jsonObject.get("aluno_nome_social")
						.isJsonNull() ? null : jsonObject.get(
						"aluno_nome_social").getAsString();
				String alunoMae = jsonObject.get("aluno_mae").isJsonNull() ? null
						: jsonObject.get("aluno_mae").getAsString();
				String alunoPai = jsonObject.get("aluno_pai").isJsonNull() ? null
						: jsonObject.get("aluno_pai").getAsString();
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
				String cursoDescricao = jsonObject.getAsJsonArray("cursos")
						.get(0).getAsJsonObject().get("curso_descricao")
						.isJsonNull() ? null : jsonObject
						.getAsJsonArray("cursos").get(0).getAsJsonObject()
						.get("curso_descricao").getAsString();
				String turmaId = jsonObject.getAsJsonArray("cursos").get(0)
						.getAsJsonObject().get("turma_id").isJsonNull() ? null
						: jsonObject.getAsJsonArray("cursos").get(0)
								.getAsJsonObject().get("turma_id")
								.getAsString();

				boolean credor = getIfCredorExist(credorCpf);

				if (credor == true) {
					BigDecimal credotAtual = insertCredor(credorNome,
							credorCpf, credorEndereco, credorCep, credorBairro,
							credorCidade, credorUf, credorResidencial,
							credorCelular, credorComercial);
					insertLogIntegracao("Finalizando Credor", "Sucesso",
							credorNome);

					insertCursoTurma(cursoDescricao, turmaId, credorNome);
					insertLogIntegracao("Finalizando Curso e turma", "Sucesso",
							credorNome);

					insertAluno(credotAtual, alunoId, alunoNome,
							alunoNomeSocial, alunoMae, alunoPai, alunoEndereco,
							alunoCep, alunoBairro, alunoCidade, alunoUf,
							alunoSexo, alunoDataNascimento, alunoRg, alunoCpf,
							alunoCelular, alunoResidencial, alunoEmail,
							credorNome);
					insertLogIntegracao("Finalizando Aluno", "Sucesso",
							credorNome);

					insertLogIntegracao(
							"Finalizando chamada do endpoint de Alunos",
							"Sucesso", credorNome);
				} else {
					insertLogIntegracao(
							"Finalizando chamada do endpoint de Alunos. Credor já cadastrado",
							"Erro", credorNome);
				}

			}
		} catch (Exception e) {
			e.printStackTrace();
			try {
				insertLogIntegracao("Erro no chamado do endpoint", "Erro", "");
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

		if (credor >= 0) {
			return true;
		} else {
			return false;
		}

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
			String credorCelular, String credorComercial) throws Exception {

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

		if (credorEndereco != null) {
			if (validarCadastroEndereco(credorEndereco, credorNome)) {
				codEnd = insertEndereco(credorEndereco, credorNome);
				countEnd = countEnd.add(BigDecimal.ONE);
			}
		}

		if (credorBairro != null) {
			if (validarCadastroBairro(credorBairro, credorNome)) {
				codBai = insertBairro(credorBairro, credorNome);
				countBai = countBai.add(BigDecimal.ONE);
			}
		}

		try {
			jdbc.openSession();

			String sqlP = "INSERT INTO TGFPAR(CODPARC, NOMEPARC, RAZAOSOCIAL ,TIPPESSOA, CODEND, CODBAI, CODCID, CEP,"
					+ "TELEFONE, CGC_CPF, DTCAD, DTALTER) "
					+ "		VALUES(?, ?, ?, ?,NVL((select codend from tsiend where TRANSLATE( "
					+ "			    upper(nomeend), "
					+ "			    'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', "
					+ "			    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC' "
					+ "			  ) like TRANSLATE( "
					+ "			    upper((SELECT LTRIM(SUBSTR(?, INSTR(?, ' ') + 1))  FROM DUAL)), "
					+ "			    'áéíóúâêîôûàèìòùãõçÁÉÍÓÚÂÊÎÔÛÀÈÌÒÙÃÕÇ', "
					+ "			    'aeiouaeiouaeiouaocAEIOUAEIOUAEIOUAOC' "
					+ "			  )), 0), NVL((select codbai from tsibai where TRANSLATE( "
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
			pstmt.setString(6, credorEndereco);

			pstmt.setString(7, credorBairro);

			pstmt.setString(8, credorCidade);
			pstmt.setString(9, credorCidade);

			pstmt.setString(10, credorCep);
			pstmt.setString(11, credorCelular);
			pstmt.setString(12, credorCpf);

			pstmt.executeUpdate();

		} catch (SQLException e) {
			insertLogIntegracao("Erro ao cadastrar credor", "Erro", credorNome);
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}

		return atualCodparc;
	}

	public void insertCursoTurma(String cursoDescricao, String turmaId,
			String credorNome) throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		BigDecimal cursoNovo = BigDecimal.ZERO;
		BigDecimal cursoNovoProj = BigDecimal.ZERO;
		BigDecimal turma = BigDecimal.ZERO;

		if (cursoDescricao != null) {
			if (validarCadastroCurso(cursoDescricao, credorNome)) {
				cursoNovo = insertCurso(cursoDescricao, credorNome);
			}
		}

		if (cursoDescricao != null) {
			if (validarCadastroCursoProj(cursoDescricao, credorNome)) {
				cursoNovoProj = insertCursoProj(cursoDescricao, credorNome);
			}
		}

		if (turmaId != null) {
			if (validarCadastroTurma(turmaId, credorNome)) {
				try {
					String ativo = "S";
					String analitico = "N";

					jdbc.openSession();

					String sqlUpdate = "INSERT INTO TCSPRJ"
							+ "(CODPROJ, CODPROJPAI ,IDENTIFICACAO, ABREVIATURA, ATIVO, ANALITICO)"
							+ "VALUES (( SELECT  (MAX(CASE  WHEN CODPROJPAI IS NULL THEN "
							+ "(select (max(codproj)) as atualCurso  from TCSPRJ where codproj = "
							+ "? )ELSE CODPROJPAI END) + 1000) AS atualTurma FROM TCSPRJ), ?, ? , ?, ?, ?)";

					pstmt = jdbc.getPreparedStatement(sqlUpdate);

					pstmt.setBigDecimal(1, cursoNovoProj);
					pstmt.setBigDecimal(2, cursoNovo);

					pstmt.setString(3, turmaId);
					if (turmaId.length() >= 20) {
						pstmt.setString(4, turmaId.substring(0, 20));
					} else {

						pstmt.setString(4, turmaId);
					}
					pstmt.setString(5, ativo);
					pstmt.setString(6, analitico);
					pstmt.executeUpdate();

				} catch (SQLException e) {
					e.printStackTrace();
					insertLogIntegracao("Erro ao cadastrar/validar curso",
							"Erro", credorNome);
				} finally {
					if (pstmt != null) {
						pstmt.close();
					}
					jdbc.closeSession();
				}
			}
		}

	}

	public boolean validarCadastroCurso(String curso, String credorNome)
			throws Exception {
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
			insertLogIntegracao("Erro ao validar se curso já cadastrado",
					"Erro", credorNome);
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

	public boolean validarCadastroCursoProj(String curso, String credorNome)
			throws Exception {
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
					"Erro ao validar se curso já cadastrado como projeto",
					"Erro", credorNome);
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

	public boolean validarCadastroTurma(String turma, String credorNome)
			throws Exception {
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

	public BigDecimal insertCurso(String cursoDescricao, String credorNome)
			throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		BigDecimal cursoNovo = BigDecimal.ZERO;

		String ativo = "S";
		String analitico = "S";

		jdbc.openSession();

		String sqlUpdate = "INSERT INTO TSICUS"
				+ "(CODCENCUS, CODCENCUSPAI, DESCRCENCUS, ATIVO, ANALITICO, GRAU)"
				+ "VALUES ((SELECT CODCENCUS + 1 FROM TSICUS "
				+ "WHERE CODCENCUSPAI = '10101000' "
				+ "ORDER BY NVL(CODCENCUS, 0) DESC "
				+ "FETCH FIRST 1 ROW ONLY), ? ,? , ?, ?, ?)";

		pstmt = jdbc.getPreparedStatement(sqlUpdate);

		pstmt.setBigDecimal(1, cursoNovo);
		pstmt.setString(2, cursoDescricao);
		pstmt.setString(3, ativo);
		pstmt.setString(4, analitico);
		pstmt.setInt(5, 4);
		pstmt.executeUpdate();

		try {
			if (pstmt != null) {
				pstmt.close();
			}
			if (jdbc != null) {
				jdbc.closeSession();
			}
		} catch (Exception se) {
			insertLogIntegracao("Erro cadastrando curso", "Erro", credorNome);
			se.printStackTrace();
		}

		return cursoNovo;

	}

	public BigDecimal insertCursoProj(String cursoDescricao, String credorNome)
			throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		BigDecimal cursoNovo = getMaxNumProjPai();
		String codProjPai = "-999999999";

		String ativo = "S";
		String analitico = "S";

		jdbc.openSession();

		String sqlUpdate = "INSERT INTO TCSPRJ"
				+ "(CODPROJ, CODPROJPAI ,IDENTIFICACAO, ABREVIATURA ,ATIVO, ANALITICO)"
				+ "VALUES (? , ? , ?, ?, ?, ?)";

		pstmt = jdbc.getPreparedStatement(sqlUpdate);
		pstmt.setBigDecimal(1, cursoNovo);
		pstmt.setString(2, codProjPai);
		pstmt.setString(3, cursoDescricao);
		if (cursoDescricao.length() >= 20) {
			pstmt.setString(4, cursoDescricao.substring(0, 20));
		} else {

			pstmt.setString(4, cursoDescricao);
		}
		pstmt.setString(5, ativo);
		pstmt.setString(6, analitico);
		pstmt.executeUpdate();

		try {
			if (pstmt != null) {
				pstmt.close();
			}
			if (jdbc != null) {
				jdbc.closeSession();
			}
		} catch (Exception se) {
			insertLogIntegracao("Erro cadastrando curso como projeto", "Erro",
					credorNome);
			se.printStackTrace();
		}

		return cursoNovo;

	}

	public BigDecimal getMaxNumProjPai() throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;
		BigDecimal bd = BigDecimal.ZERO;

		try {

			updateTgfNumParc();

			jdbc.openSession();

			String sqlUpd = "select (max(codproj) + 10000000) as atualCurso  "
					+ "from TCSPRJ where codprojpai = '-999999999' or codproj = 0";

			pstmt = jdbc.getPreparedStatement(sqlUpd);
			rs = pstmt.executeQuery();
			while (rs.next()) {

				bd = rs.getBigDecimal("atualCurso");

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

	public void insertAluno(BigDecimal credotAtual, String alunoId,
			String alunoNome, String alunoNomeSocial, String alunoMae,
			String alunoPai, String alunoEndereco, String alunoCep,
			String alunoBairro, String alunoCidade, String alunoUf,
			String alunoSexo, String alunoDataNascimento, String alunoRg,
			String alunoCpf, String alunoCelular, String alunoResidencial,
			String alunoEmail, String credorNome) throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		try {
			jdbc.openSession();

			String sqlP = "INSERT INTO AD_ALUNOS ( CODPARC, ID_EXTERNO, NOME, NOME_SOCIAL, NOME_MAE, NOME_PAI, ENDERECO, "
					+ "CEP, BAIRRO, CIDADE, UF, SEXO, DATA_NASCIMENTO, RG, CPF, TELEFONE_CELULAR, TELEFONE_RESIDENCIAL, "
					+ "EMAIL, GRAU_INSTRUCAO, PROFISSAO ) "
					+ "	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, (SELECT TO_CHAR(TO_DATE(?, 'yyyy-MM-dd'), 'dd/MM/yyyy') FROM dual), ?, ?, ?, ?, ?,? )";

			pstmt = jdbc.getPreparedStatement(sqlP);
			pstmt.setBigDecimal(1, credotAtual);
			pstmt.setString(2, alunoId);
			pstmt.setString(3, alunoNome);
			pstmt.setString(4, alunoNomeSocial);
			pstmt.setString(5, alunoMae);
			pstmt.setString(6, alunoPai);
			pstmt.setString(7, alunoEndereco);
			pstmt.setString(8, alunoCep);
			pstmt.setString(9, alunoBairro);
			pstmt.setString(10, alunoCidade);
			pstmt.setString(11, alunoUf);
			pstmt.setString(12, alunoSexo);
			pstmt.setString(13, alunoDataNascimento);
			pstmt.setString(14, alunoRg);
			pstmt.setString(15, alunoCpf);
			pstmt.setString(16, alunoCelular);
			pstmt.setString(17, alunoResidencial);
			pstmt.setString(18, alunoEmail);
			pstmt.executeUpdate();

		} catch (SQLException e) {
			insertLogIntegracao("Erro cadastrando aluno", "Erro", credorNome);
			e.printStackTrace();
		} finally {
			if (pstmt != null) {
				pstmt.close();
			}
			jdbc.closeSession();
		}

	}

	public boolean validarCadastroEndereco(String endereco, String credorNome)
			throws Exception {
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
			insertLogIntegracao("Erro ao validar se endereço já cadastrado",
					"Erro", credorNome);
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

	public BigDecimal insertEndereco(String endereco, String credorNome)
			throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		BigDecimal codend = getMaxNumEnd();

		jdbc.openSession();

		String sqlUpdate = "INSERT INTO TSIEND"
				+ "(CODEND, NOMEEND, TIPO, DTALTER)"
				+ "VALUES (?, (SELECT LTRIM(SUBSTR(?, INSTR(?, ' ') + 1)) FROM DUAL), "
				+ "(SELECT LTRIM(SUBSTR(?, 1, 2)) FROM DUAL), SYSDATE) ";
		// + "(SELECT LTRIM(SUBSTR(?, 1,INSTR(? , ' ') - 1)) "
		// + "FROM DUAL)";

		pstmt = jdbc.getPreparedStatement(sqlUpdate);
		pstmt.setBigDecimal(1, codend);
		pstmt.setString(2, endereco.toUpperCase());
		pstmt.setString(3, endereco.toUpperCase());
		pstmt.setString(4, endereco.toUpperCase());

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
			insertLogIntegracao("Erro ao cadastrar endereço", "Erro",
					credorNome);
		}

		return codend;

	}

	public BigDecimal getMaxNumEnd() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null;

		BigDecimal id = BigDecimal.ZERO;

		try {

			updateNumEnd();

			jdbc.openSession();

			String sqlNota = "SELECT MAX(ULTCOD) AS ID FROM TGFNUM WHERE ARQUIVO = 'TSIEND'";

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

	public void updateNumEnd() throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		jdbc.openSession();

		String sqlUpdate = "UPDATE TGFNUM SET ULTCOD = ULTCOD + 1  WHERE ARQUIVO = 'TSIEND'";

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

	public boolean validarCadastroBairro(String bairro, String credorNome)
			throws Exception {
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
			insertLogIntegracao("Erro ao validar se bairro já cadastrado",
					"Erro", credorNome);
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

	public BigDecimal insertBairro(String bairro, String credorNome)
			throws Exception {

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
			insertLogIntegracao("Erro ao bairro endereço", "Erro", credorNome);
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
		}

	}

	public void insertLogIntegracao(String descricao, String status,
			String credorNome) throws Exception {

		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;

		jdbc.openSession();

		String descricaoCompleta = descricao + " " + credorNome;

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
		}

	}

	public String[] apiGet(String ur) throws Exception {

		BufferedReader reader;
		String line;
		StringBuilder responseContent = new StringBuilder();
		// String key = preferenciaSenha();

		URL url = new URL(ur);

		HttpURLConnection http = (HttpURLConnection) url.openConnection();
		http.setConnectTimeout(10000);
		http.setRequestProperty("User-Agent",
				"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.0)");
		http.setRequestProperty("Content-Type",
				"application/json; charset=UTF-8");
		http.setRequestProperty("Authorization", "Bearer "
				+ "2|VFBUMOCUNitomQYMrwWY7dCaTLts1Lsab3Bktpf5");
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
		String response = responseContent.toString();

		System.out.println("response: " + response);
		System.out.println("responseContent: " + responseContent);
		System.out.println("responseContent.toString(): "
				+ responseContent.toString());

		http.disconnect();

		return new String[] { Integer.toString(status), response };
	}

}