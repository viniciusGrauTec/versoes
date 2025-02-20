package br.com.sankhya.acoesgrautec.extensions;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;

import br.com.sankhya.acoesgrautec.services.SkwServicoProducao;
import br.com.sankhya.acoesgrautec.util.EnviromentUtils;
import br.com.sankhya.acoesgrautec.util.LogConfiguration;
import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.QueryExecutor;
import br.com.sankhya.extensions.actionbutton.Registro;
import br.com.sankhya.modelcore.MGEModelException;
import br.com.sankhya.modelcore.util.SWRepositoryUtils;

import br.com.sankhya.acoesgrautec.util.LogCatcher;
import br.com.sankhya.acoesgrautec.util.LogConfiguration;

public class AcaoModelo implements AcaoRotinaJava {
	private SkwServicoProducao skwServicoProducao;
	
	/* (non-Javadoc)
	 * @see br.com.sankhya.extensions.actionbutton.AcaoRotinaJava#doAction(br.com.sankhya.extensions.actionbutton.ContextoAcao)
	 */
	@Override
	public void doAction(ContextoAcao contexto) throws Exception {
		
		//LogConfiguration.setPath(SWRepositoryUtils.getBaseFolder()+"/personalizacao/financeirofromdiscount/logs");
		
		skwServicoProducao = new SkwServicoProducao(contexto);
		SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
		System.out.println("/*************** Inicio - AcaoAjusteTabelaPreco  *****************/ ");
		long tempoAnterior = System.currentTimeMillis();
		long tempoInicio = System.currentTimeMillis();

		String usuLogado = contexto.getUsuarioLogado().toString();
		StringBuffer mensagem = new StringBuffer();
		BigDecimal idTabela = BigDecimal.ZERO;
		QueryExecutor queryTab = contexto.getQuery();
		QueryExecutor queryArtigoCor = contexto.getQuery();
		QueryExecutor queryConfArtigo = contexto.getQuery();
		QueryExecutor queryConfCor = contexto.getQuery();

		String erroSql = "";
		int qtde = 0;
		int codArtigo = 0;
		int codProd = 0;
		String catCor = null;

		try {
			Registro[] linhas = contexto.getLinhas();
			for (Registro registro : linhas) {

				if (linhas.length == 0) { 
					throw new MGEModelException("Selecione pelo menos um produto");
				}

				idTabela = (BigDecimal) registro.getCampo("IDTABELAPRECO");	
				String categoriaCor = (String) registro.getCampo("CATEGORIACOR");
				BigDecimal artigo = (BigDecimal) registro.getCampo("ARTIGO");
				int nuTab = 0;		

				/*
				System.out.println("categoriaCor - " + categoriaCor);
				System.out.println("artigo - " + artigo);

				String sqlTab = " SELECT distinct ";
				sqlTab += "              tab.NUTAB, ";
				sqlTab += "              tab.DTVIGOR, ";
				sqlTab += "              ite.ARTIGO, ";
				sqlTab += "              ite.CATEGORIACOR, ";
				sqlTab += "              ite.VRPRECO ";
				sqlTab += "       From AD_TABELAPRECOAJUSTE tab, ";
				sqlTab += "            AD_TABELAPRECOITENSAJUSTE ite ";
				sqlTab += "      Where tab.IDTABELAPRECO = {TABPRECO} ";
				sqlTab += "      and   ite.ARTIGO = {ARTIGO} ";
				sqlTab += "      and   ite.IDTABELAPRECO = tab.IDTABELAPRECO ";


				queryTab.setParam("TABPRECO", idTabela);
				queryTab.setParam("ARTIGO", artigo);
				System.out.println(" sqlTab " + sqlTab);
				queryTab.nativeSelect(sqlTab);	

				while (queryTab.next()) {	
					String categCor = queryTab.getString("CATEGORIACOR"); 				
					nuTab = queryTab.getInt("NUTAB");
					Date dataVigencia = queryTab.getDate("DTVIGOR");
					int idArtigo = queryTab.getInt("ARTIGO");					
					BigDecimal vrPreco = queryTab.getBigDecimal("VRPRECO");
					String dataVigor = formatter.format(dataVigencia);
					String vrPrecoStr = vrPreco.toString().replace(".", ","); 

					String sqlArtigoCor = " ";
					sqlArtigoCor   += " Select pro.codprod, ";
					sqlArtigoCor   += "        substr(pro.ad_cod_item,7,6) cat_cor, "; 
					sqlArtigoCor   += "        to_char('') descricao, "; 
					sqlArtigoCor   += "        pro.usoprod, ";
					sqlArtigoCor   += "        substr(pro.ad_cod_item,3,4) artigo, "; 
					sqlArtigoCor   += "        pro.ad_cod_item, ";
					sqlArtigoCor   += "        pro.descrprod ";
					sqlArtigoCor   += " From TGFPRO pro, ";
					sqlArtigoCor   += "      TGFEXC exc, ";
					sqlArtigoCor   += "      AD_ARTIGOS art, ";
					sqlArtigoCor   += "      AD_COR cor ";
					sqlArtigoCor   += " Where exc.nutab = {NUTAB} ";
					sqlArtigoCor   += " and   exc.codprod = pro.codprod ";
					sqlArtigoCor   += " and   art.codigo = {ARTIGO} ";
					sqlArtigoCor   += " and   art.codigo = substr(pro.ad_cod_item,3,4) ";				
					sqlArtigoCor   += " and   cor.categoriacor = {CATEGORIACOR} ";
					sqlArtigoCor   += " and   cor.codigo = substr(pro.ad_cod_item,7,6) ";
					queryArtigoCor.setParam("NUTAB", nuTab);

					queryArtigoCor.setParam("ARTIGO", idArtigo);			
						queryArtigoCor.setParam("CATEGORIACOR", categCor);

					System.out.println(" sqlArtigoCor " + sqlArtigoCor);
					queryArtigoCor.nativeSelect(sqlArtigoCor);

					while (queryArtigoCor.next()) {	
						int codProd = queryArtigoCor.getInt("CODPROD");

						String UpdPreco = " UPDATE TGFEXC SET VLRVENDA = '" + vrPrecoStr + "', AD_CODUSU = " + usuLogado + ",  AD_DTULTALTERACAO = sysdate Where NUTAB = " + nuTab + " AND CODPROD = " + codProd + " ";
						System.out.println(UpdPreco);
						EnviromentUtils.updateQueryConnection(UpdPreco);
						qtde++;
					} // queryArtigoCor.next()



				} // queryTab.next()
				queryTab.close();

				String UpdCab = " UPDATE TGFTAB SET DTALTER = to_date(to_char(sysdate,'dd/MM/yyyy'),'dd/MM/yyyy') Where NUTAB = " + nuTab + " ";
				EnviromentUtils.updateQueryConnection(UpdCab);
				 */

				/*				
				String sqlConfArtigo = " ";
				sqlConfArtigo   += " Select distinct ";
				sqlConfArtigo   += "        art.codigo as ARTIGO, ";
				sqlConfArtigo   += "        exc.NUTAB ";
				sqlConfArtigo   += " From TGFPRO               pro, ";
				sqlConfArtigo   += "      TGFEXC               exc, ";
				sqlConfArtigo   += "      AD_ARTIGOS           art, ";
				sqlConfArtigo   += "      AD_TABELAPRECOAJUSTE tpa ";
				sqlConfArtigo   += "  Where exc.codprod = pro.codprod ";
				sqlConfArtigo   += "  and exc.nutab = tpa.nutab ";
				sqlConfArtigo   += "  and art.codigo = substr(pro.ad_cod_item, 3, 4) ";
				sqlConfArtigo   += "  and art.codigo = {ARTIGO} ";
				sqlConfArtigo   += "  and tpa.idtabelapreco = {IDTABELAPRECO} ";
				sqlConfArtigo   += "  and exc.nutab = exc.nutab ";

				queryConfCor.setParam("IDTABELAPRECO", idTabela);
				queryConfCor.setParam("ARTIGO", artigo);	
				queryConfCor.nativeSelect(sqlConfArtigo);*/


				String sqlArtigoCor = " ";
				sqlArtigoCor   += " Select distinct";
				sqlArtigoCor   += "        pro.CODPROD, ";
				sqlArtigoCor   += "        tpa.NUTAB, ";
				sqlArtigoCor   += "        art.codigo as ARTIGO, ";
				sqlArtigoCor   += "        cor.CATEGORIACOR ";
				sqlArtigoCor   += " From TGFPRO pro, ";
				sqlArtigoCor   += "      TGFEXC exc, ";     
				sqlArtigoCor   += "      AD_ARTIGOS art, ";
				sqlArtigoCor   += "      AD_COR cor, ";				
				sqlArtigoCor   += "      AD_TABELAPRECOAJUSTE tpa ";
				sqlArtigoCor   += " Where exc.codprod = pro.codprod "; 
				sqlArtigoCor   += " and   exc.nutab = tpa.nutab "; 
				sqlArtigoCor   += " and   art.codigo = substr(pro.ad_cod_item,3,4) "; 
				sqlArtigoCor   += " and   art.codigo = {ARTIGO} ";
				sqlArtigoCor   += " and   cor.codigo = substr(pro.ad_cod_item,7,6) ";
				if (categoriaCor != null) {
					sqlArtigoCor   += " and   cor.categoriacor = {CATEGORIACOR} ";
				}
				sqlArtigoCor   += " and   tpa.idtabelapreco = {IDTABELAPRECO} ";
				sqlArtigoCor   += " and   exc.nutab = exc.nutab ";

				queryArtigoCor.setParam("IDTABELAPRECO", idTabela);
				queryArtigoCor.setParam("ARTIGO", artigo);				
				if (categoriaCor != null) {
					queryArtigoCor.setParam("CATEGORIACOR", categoriaCor);
				}
				erroSql = sqlArtigoCor;

				//		System.out.println(" sqlProdCor " + erroSql);

				queryArtigoCor.nativeSelect(sqlArtigoCor);


				while (queryArtigoCor.next()) {	
					nuTab     = queryArtigoCor.getInt("NUTAB");
					codProd   = queryArtigoCor.getInt("CODPROD");					
					codArtigo = queryArtigoCor.getInt("ARTIGO");
					catCor    = queryArtigoCor.getString("CATEGORIACOR");

					String sqlTab = " SELECT distinct ";
					sqlTab += "              ite.SEQITEM, ";
					sqlTab += "              ite.VRPRECO ";
					sqlTab += "       From AD_TABELAPRECOITENSAJUSTE ite "; 
					sqlTab += "       Where ite.IDTABELAPRECO = {TABPRECO} ";
					sqlTab += "       and   ite.artigo = {ARTIGO} ";
					if (categoriaCor != null) {
						sqlTab += "       and   ite.categoriacor = {CATEGORIACOR} ";						
					}

					queryTab.setParam("TABPRECO", idTabela);
					queryTab.setParam("ARTIGO", codArtigo);
					if (categoriaCor != null) {
						queryTab.setParam("CATEGORIACOR", catCor);
					}

					erroSql = sqlTab;
					//			System.out.println(" sqlTab " + sqlTab);					
					queryTab.nativeSelect(sqlTab);		
					while (queryTab.next()) {	
						int seqItem = 0;
						seqItem            = queryTab.getInt("SEQITEM");
						BigDecimal vrPreco = queryTab.getBigDecimal("VRPRECO");						
						String vrPrecoStr  = vrPreco.toString().replace(".", ",");						

						if (seqItem > 0 && nuTab > 0) {
							String UpdPreco = " UPDATE TGFEXC SET VLRVENDA = '" + vrPrecoStr + "', AD_CODUSU = " + usuLogado + ",  AD_DTULTALTERACAO = sysdate Where NUTAB = " + nuTab + " AND CODPROD = " + codProd + " ";
							//	System.out.println(UpdPreco);
							erroSql = UpdPreco;
							EnviromentUtils.updateQueryConnection(UpdPreco);
							qtde++;
						}
					} // queryTab.next()

				} // queryArtigoCor.next()


				String UpdCab = " UPDATE TGFTAB SET DTALTER = to_date(to_char(sysdate,'dd/MM/yyyy'),'dd/MM/yyyy') Where NUTAB = " + nuTab + " ";
				EnviromentUtils.updateQueryConnection(UpdCab);

				int intArtigo = Integer.valueOf(artigo.intValue());
				if (intArtigo != codArtigo) {
					contexto.mostraErro("Artigo - [" + artigo.toString() + "], ou Categoria de Cor - [" + categoriaCor + "], não cadastrados na tabela de preços.");
				}

			} // for (Registro registro : linhas) {
			mensagem.append("Quantidade de Produtos Alterados " + qtde);
			System.out.println("/*************** Fim - AcaoAjusteTabelaPreco *****************/");

			skwServicoProducao.printLogDebug(tempoInicio, "Tempo Total: ");
		} catch (Exception e) {
			System.err.println("ERRO SQL - " + erroSql );
			mensagem.append(e.getMessage());
		} finally {
			queryTab.close();
			queryArtigoCor.close();
			contexto.setMensagemRetorno(mensagem.toString());
		}
	}

}
