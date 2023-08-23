package br.com.sankhya.acoesultramega.services;

import java.util.EnumMap;
import java.util.Map;

import br.com.sankhya.acoesultramega.model.Apis;
import br.com.sankhya.acoesultramega.services.PropertiesService;
import br.com.sankhya.acoesultramega.services.DBAcessService;
import br.com.sankhya.acoesultramega.services.DBAcessServiceUltramega;
import br.com.sankhya.acoesultramega.util.LogCatcher;
import br.com.sankhya.acoesultramega.util.LogConfiguration;
import br.com.sankhya.modelcore.util.SWRepositoryUtils;




public class ProjectControllerService {
	
	private static Apis api;
	//private static final Map<Apis, ApiService> services = new EnumMap<>(Apis.class);
	//private static final Map<Apis, DBAcessLogs> logs = new EnumMap<>(Apis.class);

	//private static final Map<Apis, EntityFactoryService> entity = new EnumMap<>(Apis.class);
	//private static final Map<Apis, JsonFactoryService> json = new EnumMap<>(Apis.class);
	
	//private static final Map<Apis, AuthorizationIntegrationService> authorization = new EnumMap<>(Apis.class);
	//private static final Map<Apis, CreateAccountIntegrationService> createAccount = new EnumMap<>(Apis.class);
	//private static final Map<Apis, GetCreditLimitsIntegrationService> updtCredtLimits = new EnumMap<>(Apis.class);
	
	
	
	//private static final Map<Apis, DBAcessToken> tokens = new EnumMap<>(Apis.class);
	private static final Map<Apis, DBAcessService> dba = new EnumMap<>(Apis.class);
    static {
    	LogConfiguration.setPath(SWRepositoryUtils.getBaseFolder()+"/personalizacao/financeirofromdiscount/logs");
    	
    	getApi();
    	setupDBAcessServices();
    	/*
    	setupEntityFactoryServices();
    	setupJsonFactoryServices();
    	setupApiServices();
    	
    	
    	
    	setupDBAcessTokenServices();
    	setupDBAcessServices();
    	
    	setupAuthorizationIntegrationServices();
    	setupCreateAccountIntegrationServices();
    	setupGetCreditLimitsIntegrationServices();*/
    	
    	
    	/*
    	
    	setupDBAcessLogsServices();
    	*/
    	
    }

    ////////////////////////////////////PROPS/////////////////////////////////
    private static void getApi(){
		try {
			api = Apis.valueOf(PropertiesService.getApplicationProperties().getProperty("prop.api.name"));
		} catch (Exception e) {
			LogCatcher.logError(e);
		}
	}
    ////////////////////////////////////PROPS/////////////////////////////////

    
    
    
    
    ////////////////////////////////////LOGS/////////////////////////////////
   /* private static void setupDBAcessLogsServices() {
		logs.put(Apis.REDE, new DBAcessLogsRede());
	}
    
    public static DBAcessLogs getDBAcessLogsService() throws Exception {
		return logs.get(api);
	}*/
    
    ////////////////////////////////////LOGS/////////////////////////////////
    
    
    
    
    
    ////////////////////////////////////API SERVICE/////////////////////////////////
    /*private static void setupApiServices() {
		services.put(Apis.INFRAPAY, new ApiServiceInfrapay());
	}
    
    public static ApiService getApiService() throws Exception {
		ApiService service = services.get(api);
		//service.setHost();
		return service;
	}*/
    
    ////////////////////////////////////API SERVICE/////////////////////////////////
	
    
    
    
	
    ////////////////////////////////////DBACESS TOKEN/////////////////////////////////
   /* private static void setupDBAcessTokenServices() {
		tokens.put(Apis.INFRAPAY, new DBAcessTokenInfrapay());
	}
    
    public static DBAcessToken getDBAcessTokenService() throws Exception {
		DBAcessToken service = tokens.get(api);
		return service;
	}
	*/
    ////////////////////////////////////DBACESS TOKEN/////////////////////////////////
    
	
    
    
    
    
    ////////////////////////////////////ENTITY FACTORYS/////////////////////////////////
    /*private static void setupEntityFactoryServices() {
    	entity.put(Apis.INFRAPAY, new EntityFactoryServiceInfrapay());
	}
    
    public static EntityFactoryService getEntityFactoryService() throws Exception {
    	EntityFactoryService service = entity.get(api);
		return service;
	}*/
	
    ////////////////////////////////////ENTITY FACTORYS/////////////////////////////////
	
	
	
    
    ////////////////////////////////////JSON FACTORYS/////////////////////////////////
    /*private static void setupJsonFactoryServices() {
    	json.put(Apis.INFRAPAY, new JsonFactoryServiceInfrapay());
	}
    
    public static JsonFactoryService getJsonFactoryService() throws Exception {
    	JsonFactoryService service = json.get(api);
		return service;
	}*/
	
    ////////////////////////////////////JSON FACTORYS/////////////////////////////////
	
	
    
    
    ////////////////////////////////////DBACESS GENERAL/////////////////////////////////
   	private static void setupDBAcessServices() {
    	dba.put(Apis.ULTRAMEGA, new DBAcessServiceUltramega());
	}
    
    public static DBAcessService getDBAcessService() throws Exception {
    	DBAcessService service = dba.get(api);
		return service;
	}
    
    ////////////////////////////////////DBACESS GENERAL/////////////////////////////////
	
    ////////////////////////////////////SENDNOTE SERVICE/////////////////////////////////
   /* private static void setupAuthorizationIntegrationServices() {
    	authorization.put(Apis.INFRAPAY, new AuthorizationIntegrationServiceInfrapay());
	}
    
    public static AuthorizationIntegrationService getAuthorizationIntegrationService() throws Exception {
    	AuthorizationIntegrationService service = authorization.get(api);
		return service;
	}*/
    
    ////////////////////////////////////DBACESS GENERAL/////////////////////////////////
	
	
    
    ////////////////////////////////////CREATE ACCOUNT SERVICE/////////////////////////////////
    /*private static void setupCreateAccountIntegrationServices() {
    	createAccount.put(Apis.INFRAPAY, new CreateAccountIntegrationServiceInfrapay());
	}
    
    public static CreateAccountIntegrationService getCreateAccountIntegrationService() throws Exception {
    	CreateAccountIntegrationService service = createAccount.get(api);
		return service;
	}*/
    
    ////////////////////////////////////CREATE ACCOUNT GENERAL/////////////////////////////////
	
	
    
    ////////////////////////////////////UPDT CREDIT LIMITS SERVICE/////////////////////////////////
   /* private static void setupGetCreditLimitsIntegrationServices() {
    	updtCredtLimits.put(Apis.INFRAPAY, new GetCreditLimitsIntegrationServiceInfrapay());
	}
    
    public static GetCreditLimitsIntegrationService getGetCreditLimitsIntegrationService() throws Exception {
    	GetCreditLimitsIntegrationService service = updtCredtLimits.get(api);
		return service;
	}*/
    
    ////////////////////////////////////UPDT CREDIT LIMITS GENERAL/////////////////////////////////
	
	
    
    

}
