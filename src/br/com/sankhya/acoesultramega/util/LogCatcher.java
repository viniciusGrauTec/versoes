package br.com.sankhya.acoesultramega.util;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * This class creates the log file
 * 
 * */
public class LogCatcher {

	/**
	 * Creates an error message with the entire stack trace of an exception [use it as e.printStackTrace]
	 * @param error
	 * */
	public static void logError(Throwable error){
		StringWriter errors = new StringWriter();
		error.printStackTrace(new PrintWriter(errors));
		
		String message = buildMessage(LogType.ERROR, errors.toString());
		try {
			log(message);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Creates an error message with a simple message string 
	 * @param String erro
	 * */
	public static void logError(String error){
		
		String message = buildMessage(LogType.ERROR, error);
		try {
			log(message);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	 * Creates an info message with a simple message string [use it as SYSOUT]
	 * @param String info
	 * */
	public static void logInfo(String info) {
		
		String message = buildMessage(LogType.INFO, info);
		try {
			log(message);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}

	/*private static void log(String message) throws Exception {

		String path = LogConfiguration.getPath()+"/log"+LocalDate.now().toString()+".txt";

		System.out.println("caminho "+ path);
		File logfile = new File(path);
		logfile.createNewFile();
		
		FileWriter writer = new FileWriter(path, true);
		writer.write(message);
		writer.close();
	}*/
	
	private static String buildMessage(LogType type,String message) {
		
		String fmessage = actualDateTime() + " -  ["+type.toString()+"]  - "+message+ "\n";
		
		
		return fmessage;
	}
	
	/*private static String actualDateTime() {
		LocalDateTime dt = LocalDateTime.now();
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
        String formatoPersonalizado = dt.format(formatter);
		return formatoPersonalizado;
	}*/
	
	private static String actualDateTime() {
        // Obtendo a data e hora atual
        Calendar calendar = Calendar.getInstance();
        Date date = calendar.getTime();

        // Formatando a data e hora para texto
        SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        String formatoPersonalizado = sdf.format(date);

        return formatoPersonalizado;
    }

    private static void log(String message) throws IOException {
        // Obtendo o caminho do log
        String path = LogConfiguration.getPath() + "/log" + getFormattedDate() + ".txt";

        System.out.println("caminho " + path);
        File logfile = new File(path);
        logfile.createNewFile();

        FileWriter writer = new FileWriter(path, true);
        writer.write(message);
        writer.close();
    }

    private static String getFormattedDate() {
        // Obtendo a data atual
        Calendar calendar = Calendar.getInstance();
        Date date = calendar.getTime();

        // Formatando a data para texto
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(date);
    }

    // Classe fictícia para simular a obtenção do caminho do log
    private static class LogConfiguration {
        public static String getPath() {
            // Simulando um caminho de diretório para os logs
            return "/caminho/do/diretorio";
        }
    }

    public static void main(String[] args) {
        String formattedDateTime = actualDateTime();
        System.out.println("Data e Hora atual: " + formattedDateTime);

        try {
            log("Mensagem de log...");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
