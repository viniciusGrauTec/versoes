package br.com.sankhya.acoesgrautec.jobs;


import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class ValidacaoIntegracaoAlunosJob implements ScheduledAction {
	 
	@Override
    public void onTime(ScheduledActionContext contexto) {
        EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
        JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
        
        System.out.println("=== INÍCIO DA VERIFICAÇÃO DE ALUNOS FALTANTES ===");

        try {
            jdbc.openSession();
            
            // 1. Buscar empresas ativas com integração habilitada
            List<BigDecimal> empresas = buscarEmpresasAtivas(jdbc);
            
            for (BigDecimal codEmp : empresas) {
                // 2. Buscar configurações da empresa
                EmpresaConfig config = buscarConfigEmpresa(jdbc, codEmp);
                
                if (config != null) {
                    // 3. Buscar IDs de alunos do endpoint
                    List<String> idsEndpoint = buscarIdsAlunosEndpoint(config);
                    
                    // 4. Buscar IDs de alunos no banco local
                    List<String> idsLocal = buscarIdsAlunosLocal(jdbc, codEmp);
                    
                    // 5. Identificar discrepâncias
                    List<String> faltantes = compararIds(idsEndpoint, idsLocal);
                    
                    // 6. Registrar log de faltantes
                    if (!faltantes.isEmpty()) {
                        registrarFaltantes(faltantes, codEmp, config);
                    }
                }
            }
        } catch (Exception e) {
            System.err.println("Erro na verificação de alunos faltantes: " + e.getMessage());
            e.printStackTrace();
        } finally {
            jdbc.closeSession();
            System.out.println("=== FIM DA VERIFICAÇÃO DE ALUNOS FALTANTES ===");
        }
    }

    private List<BigDecimal> buscarEmpresasAtivas(JdbcWrapper jdbc) throws Exception {
        List<BigDecimal> empresas = new ArrayList<>();
        String sql = "SELECT CODEMP FROM AD_LINKSINTEGRACAO WHERE INTEGRACAO = 'S'";
        
        try (PreparedStatement pstmt = jdbc.getPreparedStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            
            while (rs.next()) {
                empresas.add(rs.getBigDecimal("CODEMP"));
            }
        }
        return empresas;
    }

    private EmpresaConfig buscarConfigEmpresa(JdbcWrapper jdbc, BigDecimal codEmp) throws Exception {
        String sql = "SELECT URL, TOKEN FROM AD_LINKSINTEGRACAO WHERE CODEMP = ?";
        
        try (PreparedStatement pstmt = jdbc.getPreparedStatement(sql)) {
            pstmt.setBigDecimal(1, codEmp);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return new EmpresaConfig(
                        rs.getString("URL"),
                        rs.getString("TOKEN"),
                        codEmp
                    );
                }
            }
        }
        return null;
    }

    
    // faz uma requisição HTTP para o endpoint externo e recupera todos os IDs de alunos disponíveis na API.
    private List<String> buscarIdsAlunosEndpoint(EmpresaConfig config) throws Exception {
        List<String> ids = new ArrayList<>();
        
        if (config == null || config.url == null || config.token == null) {
            System.out.println("Configuração inválida para buscar alunos no endpoint");
            return ids;
        }
        
        String url = config.url.trim() + "/alunos?fields=id";

        try {
            URL urlObj = new URI(url).toURL();
            HttpURLConnection conn = (HttpURLConnection) urlObj.openConnection();
            conn.setRequestMethod("GET");
            conn.setRequestProperty("Authorization", "Bearer " + config.token);

            if (conn.getResponseCode() == 200) {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(conn.getInputStream()))) {
                    
                    JsonParser parser = new JsonParser();
                    JsonArray alunos = parser.parse(reader).getAsJsonArray();
                    for (JsonElement aluno : alunos) {
                        JsonElement idElement = aluno.getAsJsonObject().get("id");
                        if (idElement != null && !idElement.isJsonNull()) {
                            ids.add(idElement.getAsString());
                        }
                    }
                }
            } else {
                System.out.println("Erro ao chamar API: " + conn.getResponseCode() + " - " + conn.getResponseMessage());
            }
        } catch (Exception e) {
            System.out.println("Erro ao buscar alunos no endpoint: " + e.getMessage());
            throw e;
        }
        
        return ids;
    }

    //consulta o banco de dados local para obter todos os IDs de alunos já cadastrados para aquela empresa.
    private List<String> buscarIdsAlunosLocal(JdbcWrapper jdbc, BigDecimal codEmp) throws Exception {
        List<String> ids = new ArrayList<>();
        String sql = "SELECT ID_EXTERNO FROM AD_ALUNOS WHERE CODEMP = ?";
        
        try (PreparedStatement pstmt = jdbc.getPreparedStatement(sql)) {
            pstmt.setBigDecimal(1, codEmp);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    ids.add(rs.getString("ID_EXTERNO"));
                }
            }
        }
        return ids;
    }
    
    
    //compara as duas listas e identifica quais IDs existem no endpoint externo mas não estão presentes no banco local
    private List<String> compararIds(List<String> endpointIds, List<String> localIds) {
        return endpointIds.stream()
            .filter(id -> !localIds.contains(id))
            .collect(Collectors.toList());
    }

    private void registrarFaltantes(List<String> faltantes, BigDecimal codEmp, EmpresaConfig config) {
        String mensagem = String.format(
            "Alunos faltantes na empresa %s: %s | URL: %s",
            codEmp,
            String.join(", ", faltantes),
            config.url
        );
        
        inserirLog(mensagem, codEmp);
    }

    
    //trocar para a tabela de log diario  depois
    private void inserirLog(String mensagem, BigDecimal codEmp) {
        String sql = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, STATUS, CODEMP) " +
                     "VALUES ((SELECT NVL(MAX(NUMUNICO),0)+1 FROM AD_LOGINTEGRACAO), ?, SYSDATE, 'ERRO', ?)";
        
        try (PreparedStatement pstmt = EntityFacadeFactory.getDWFFacade().getJdbcWrapper().getPreparedStatement(sql)) {
            pstmt.setString(1, mensagem);
            pstmt.setBigDecimal(2, codEmp);
            pstmt.executeUpdate();
        } catch (Exception e) {
            System.err.println("Erro ao registrar log: " + e.getMessage());
        }
    }

    private static class EmpresaConfig {
        String url;
        String token;
        BigDecimal codEmp;

        EmpresaConfig(String url, String token, BigDecimal codEmp) {
            this.url = url;
            this.token = token;
            this.codEmp = codEmp;
        }
    }

}
