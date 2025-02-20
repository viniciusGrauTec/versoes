package br.com.sankhya.acoesgrautec.extensions;


import java.util.Locale;

import javax.servlet.http.HttpServletRequest;

import br.com.sankhya.extensions.actionbutton.AcaoRotinaJava;
import br.com.sankhya.extensions.actionbutton.ContextoAcao;
import br.com.sankhya.ws.ServiceContext;

public class AcaoTesteUrl implements AcaoRotinaJava{

	@Override
	public void doAction(ContextoAcao contexto) throws Exception {
		
		contexto.setMensagemRetorno("teste url: " + criarUrl());
		
	}
	
	public String criarUrl() throws Exception{

		HttpServletRequest servletRequest = ServiceContext.getCurrent().getHttpRequest();
		String url = servletRequest.getLocalAddr();
		String porta = String.valueOf(servletRequest.getLocalPort());
		String protocol = servletRequest.getProtocol().split("/")[0].toLowerCase(Locale.ROOT);
		return protocol + "://" + url + ":" + porta;
	    
	}

}
