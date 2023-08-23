package br.com.sankhya.acoesultramega.extensions;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;

import br.com.sankhya.acoesultramega.services.SkwServicoProducao;
import br.com.sankhya.acoesultramega.util.EnviromentUtils;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.QueryExecutor;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.modelcore.MGEModelException;

public class AcaoChamadoIniciarAtendimento implements AcaoRotinaJava {

	/**
	 * Ação: Iniciar atendimento. Autor: Anderson Nascimento. Data: 23/03/2022
	 */

	@Override
	public void doAction(ContextoAcao contexto) throws Exception {
		StringBuffer mensagem = new StringBuffer();
		QueryExecutor queryValidarStatus = contexto.getQuery();
		QueryExecutor queryUltCodMov = contexto.getQuery();
		QueryExecutor queryInsertdMov = contexto.getQuery();
		String statusAtualChamado = "";
		BigDecimal UltCodMov = BigDecimal.ZERO;
		try {
			// DECLARAÇÃO DE VARIAVEIS
			Registro[] linhas = contexto.getLinhas();
			

			// LISTA DOS REGISTROS SELECIONADOS
			for (Registro registro : linhas) {

				// VERIFICAR SE POSSUI REGISTRO SELECIONADO
				if (linhas.length == 0) {
					throw new MGEModelException(
							"Selecione pelo menos um chamado");
				}

				// CAPTURAR O CODCHAMADO
				BigDecimal codchamado = (BigDecimal) registro
						.getCampo("CODCHAMADO");
				//Verificar se está como "NOVO" antes de salvar
				//caso contrário, informar para o usuário.
				String sqlStatusChamado = " SELECT STATUS, CODGRUPOAT "
						+ " FROM AD_CHAMADOSANP "
						+ " WHERE CODCHAMADO = {CODCHAMADO} ";
				
				queryValidarStatus.setParam("CODCHAMADO", codchamado);
				queryValidarStatus.nativeSelect(sqlStatusChamado);
				while(queryValidarStatus.next()){
					statusAtualChamado = queryValidarStatus.getString("STATUS");
				}
				
				if(!"N".equals(statusAtualChamado)){
					throw new MGEModelException("Só é possivel iniciar um chamado com o Status NOVO");
				}else{
					String	sqlUltCodMov = " SELECT MAX(CODCHAMADOMOV) CODMOV ";
							sqlUltCodMov += " FROM AD_CHAMADOMOVANP ";
							sqlUltCodMov += " where codchamado = {CODCHAMADO} ";
							
					queryUltCodMov.setParam("CODCHAMADO", codchamado);
					queryUltCodMov.nativeSelect(sqlUltCodMov);
					while(queryUltCodMov.next()){
						UltCodMov = queryUltCodMov.getBigDecimal("CODMOV");
					}
					
					String sqlInsertMov = " INSERT INTO ad_chamadomovanp ( "
							+ " codchamado, "
							+ " codchamadomov, "
							+ " codusucadastro, "
							+ " dtinicio, "	            
							+ " status "
							+ " ) VALUES ( "
				            + " {CODCHAMADO}, "
				            + " {CODCHAMADOMOV} + 1, "
				            + " P_CODUSU,  "
				            + " SYSDATE,  "
				            + " 'A') ";
					queryInsertdMov.setParam("CODCHAMADO", codchamado);
					queryInsertdMov.setParam("CODCHAMADOMOV", UltCodMov);
					queryInsertdMov.update(sqlInsertMov);
				}
	
			}
			mensagem.append("Chamado iniciado com sucesso");

		} catch (Exception e) {
			mensagem.append(e.getMessage());
		} finally {
			queryValidarStatus.close();
			queryUltCodMov.close();
			queryInsertdMov.close();
			contexto.setMensagemRetorno(mensagem.toString());
		}
	}

}
