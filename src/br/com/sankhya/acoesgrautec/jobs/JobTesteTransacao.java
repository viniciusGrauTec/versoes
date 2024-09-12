package br.com.sankhya.acoesgrautec.jobs;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Locale;

import javax.servlet.http.HttpServletRequest;

import org.cuckoo.core.ScheduledAction;
import org.cuckoo.core.ScheduledActionContext;

import br.com.sankhya.jape.EntityFacade;
import br.com.sankhya.jape.dao.JdbcWrapper;
import br.com.sankhya.modelcore.util.EntityFacadeFactory;
import br.com.sankhya.ws.ServiceContext;

public class JobTesteTransacao implements ScheduledAction {

	@Override
	public void onTime(ScheduledActionContext arg0) {
		System.out.println("Teste transação");
		
		EntityFacade entityFacade = EntityFacadeFactory.getDWFFacade();
		JdbcWrapper jdbc = entityFacade.getJdbcWrapper();
		PreparedStatement pstmt = null;
		try {
			
			jdbc.openSession();
			jdbc.getConnection().setAutoCommit(false);
			
			System.out.println("set auto commit como falso");

			String sqlUpdate = "INSERT INTO AD_LOGINTEGRACAO (NUMUNICO, DESCRICAO, DTHORA, STATUS) "
					+ "VALUES (((SELECT NVL(MAX(NUMUNICO), 0) + 1 FROM AD_LOGINTEGRACAO)), ?, SYSDATE, ?)";

			pstmt = jdbc.getPreparedStatement(sqlUpdate);
			pstmt.setString(1, "TESTE");
			pstmt.setString(2, "TESTE");
			pstmt.executeUpdate();
			
			System.out.println("passou do insert");
			
			jdbc.getConnection().commit();
			
			System.out.println("passou do commit da transação");
			
			
		} catch (Exception e) {
			e.printStackTrace();
			try {
				jdbc.getConnection().rollback();
			} catch (SQLException e1) {
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
			jdbc.closeSession();
		}

	}

    private static String buildLocalHostUrl() {
        HttpServletRequest servletRequest = ServiceContext.getCurrent().getHttpRequest();
        String url = servletRequest.getLocalAddr();
        String porta = String.valueOf(servletRequest.getLocalPort());
        String protocol = servletRequest.getProtocol().split("/")[0].toLowerCase(Locale.ROOT);
        return protocol + "://" + url + ":" + porta;
    }

}
