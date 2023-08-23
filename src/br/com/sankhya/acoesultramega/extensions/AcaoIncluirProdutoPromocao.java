package br.com.sankhya.acoesultramega.extensions;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.extensions.actionbutton.QueryExecutor;
import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;

public class AcaoIncluirProdutoPromocao implements AcaoRotinaJava {

	@Override
	public void doAction(ContextoAcao contexto) throws Exception {
		
		System.out.println("Entrou no cadastro de produtos nas promoções");
		
		QueryExecutor query = contexto.getQuery();
		
		String dtVencIni = contexto.getParam("DTVENCINI").toString();
		String dtVencFin = contexto.getParam("DTVENCFIM").toString();
		String dtIniDesc = contexto.getParam("DTINIDESC").toString();
		String dtFimDesc = contexto.getParam("DTFIMDESC").toString();
		String nomePromo = contexto.getParam("NOMEPROMO").toString();
		String tipDesc = (String) contexto.getParam("TIPDESC");
		
		BigDecimal codprod = null;
		BigDecimal qtdAte = null;
		BigDecimal percdesc = null;
		BigDecimal codemp = null;
		BigDecimal codlocal = null;
		String controle = null;
		
		if(contexto.getParam("PRODUTO") != null){
			codprod = new BigDecimal (contexto.getParam("PRODUTO").toString());
		}
		if(contexto.getParam("QTDATE") != null){
			qtdAte = new BigDecimal(contexto.getParam("QTDATE").toString());
		}
		if(contexto.getParam("PERCDESC") != null){
			percdesc = new BigDecimal(contexto.getParam("PERCDESC").toString());
		}
		if(contexto.getParam("CODEMP") != null){
			codemp = new BigDecimal(contexto.getParam("CODEMP").toString());
		}
		
		
		BigDecimal codprodQuery = BigDecimal.ZERO;
		
		SimpleDateFormat inputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
		SimpleDateFormat outputFormat = new SimpleDateFormat("dd/MM/yyyy");
        
		Date dateIni = inputFormat.parse(dtVencIni);
		Date dateFin = inputFormat.parse(dtVencFin);
		Date dateIniDesc = inputFormat.parse(dtIniDesc);
		Date dateFinDesc = inputFormat.parse(dtFimDesc);
		
        String DateFIni = outputFormat.format(dateIni);
        String DateFFin = outputFormat.format(dateFin);
        String DateFIniDesc = outputFormat.format(dateIniDesc);
        String DateFFinDesc = outputFormat.format(dateFinDesc);
		
		System.out.println("Data Inicio Vencimento: " + DateFIni);
		System.out.println("Data Final Vencimento: " + DateFFin);
		System.out.println("Data Inicio Desconto: " + DateFIniDesc);
		System.out.println("Data Final Desconto: " + DateFFinDesc);
		System.out.println("Produto: " + codprod);
		System.out.println("Nome Promoção: " + nomePromo);
		System.out.println("Quantidade até: " + qtdAte);
		System.out.println("Percentual Desconto: " + percdesc);
		System.out.println("Tipo Desconto: " + tipDesc);
		
		StringBuilder queryProd = new StringBuilder();
		queryProd.append("select * from tgfest where trunc(dtval) between trunc(to_date({DTINI})) and trunc(to_date({DTFIN})) and ativo = 'S' and estoque > 0");
		
		query.setParam("DTINI", DateFIni);
		query.setParam("DTFIN", DateFFin);
		if(codprod != null){
			queryProd.append(" and codprod = {CODPROD}");
			query.setParam("CODPROD", codprod);
		}
		
		query.nativeSelect(queryProd.toString());
		
		while(query.next()){
			
			codprodQuery = query.getBigDecimal("CODPROD");
			controle = query.getString("CONTROLE");
			codlocal = query.getBigDecimal("CODLOCAL");
			
			insertPromo(contexto, DateFIniDesc, DateFFinDesc, 
					nomePromo, qtdAte, percdesc, tipDesc.trim(), codemp, 
					codlocal, controle, codprodQuery);
		}
		
	}
	
	public void insertPromo(ContextoAcao contexto, String DateFIniDesc, String DateFFinDesc
			, String nomePromo, BigDecimal qtdAte, BigDecimal percdesc, String tipDesc, 
			BigDecimal codemp, BigDecimal codlocal, String controle, BigDecimal codprod) throws Exception{
		
		BigDecimal nuPromo = getMaxNum();
		
		QueryExecutor queryExec = contexto.getQuery();
		
		StringBuilder query = new StringBuilder();
		
		query.append("INSERT INTO TGFDES (");
		query.append("NUPROMOCAO, ");
		query.append("DESCRPROMOCAO, ");
		query.append("DTINICIAL, ");
		query.append("DTFINAL, ");
		query.append("CODPARC, ");
		query.append("CODPROD, ");
		query.append("TIPPROMOCAO, ");
		query.append("PERCENTUAL, ");
		query.append("VLRDESC, ");
		query.append("CODEMP, ");
		query.append("APLICDESCPORLOCAL, ");
		query.append("CODLOCAL, ");
		query.append("USADESCCTRL, ");
		query.append("CONTROLE, ");
		query.append("GRUPODESCPARC, ");
		query.append("GRUPODESCPROD, ");
		query.append("USADESCQTD) ");
		query.append("VALUES ({NUPROMOCAO},");
		query.append("{NOMEPROMO},");
		query.append("{DTINICIAL},");
		query.append("{DTFINAL}, ");
		query.append("0, ");
		query.append("{CODPROD}, ");
		query.append("{TIPPROMOCAO}, ");
		query.append("{PERCENTUAL}, ");
		query.append("{VLRDESC}, ");
		query.append("{CODEMP}, ");
		query.append("'S', ");
		query.append("{CODLOCAL}, ");
		query.append("'S', ");
		query.append("{CONTROLE}, ");
		query.append("'***************', ");
		query.append("'***************', ");
		query.append("{USADESCQTD})");
		
		queryExec.setParam("NUPROMOCAO", nuPromo);
		queryExec.setParam("NOMEPROMO", nomePromo);
		queryExec.setParam("DTINICIAL", DateFIniDesc);
		queryExec.setParam("DTFINAL", DateFFinDesc);
		queryExec.setParam("CODPROD", codprod);
		queryExec.setParam("TIPPROMOCAO", "P");
		
		System.out.println("tipo desconto insert: " + tipDesc);
		
		if(tipDesc.equals("1")){
			System.out.println("Entrou no if do percentual");
			queryExec.setParam("PERCENTUAL", percdesc);
			queryExec.setParam("VLRDESC", BigDecimal.ZERO);
		}else{
			System.out.println("Entrou no else do percentual");
			queryExec.setParam("PERCENTUAL", BigDecimal.ZERO);
			queryExec.setParam("VLRDESC", percdesc);
		}
		
		if(codemp != null){
			queryExec.setParam("CODEMP", codemp);
		}else{
			queryExec.setParam("CODEMP", BigDecimal.ZERO);
		}
		queryExec.setParam("CODLOCAL", codlocal);
		queryExec.setParam("CONTROLE", controle);
		if(qtdAte != null){
			queryExec.setParam("USADESCQTD", "S");
		}else{
			queryExec.setParam("USADESCQTD", "N");
		}
		
		queryExec.update(query.toString());
		
	}
	
	public BigDecimal getMaxNum() throws Exception {
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		ResultSet rs = null; 
		
		BigDecimal id = BigDecimal.ZERO;
		
		try {
			
			updateNumCab();
			
			jdbc.openSession();
			
			String sqlNota = "SELECT MAX(ULTCOD) AS ID FROM TGFNUM WHERE ARQUIVO = 'TGFDES'";
			
			pstmt = jdbc.getPreparedStatement(sqlNota);
			
			rs = pstmt.executeQuery();
			
			if (rs.next()){
				
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
	
	public void updateNumCab() throws Exception{
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		
		
		jdbc.openSession();
		
				String sqlUpdate = "UPDATE TGFNUM SET ULTCOD = ULTCOD + 1  WHERE ARQUIVO = 'TGFDES'";
				
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
}
