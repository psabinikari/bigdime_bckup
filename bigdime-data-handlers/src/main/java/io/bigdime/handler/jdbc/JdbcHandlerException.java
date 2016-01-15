package io.bigdime.handler.jdbc;



/**
 * Jdbc Handler Exception is thrown when the application is not able to
 * process the rdbms data.
 * 
 * @author Pavan Sabinikari
 * 
 */
public class JdbcHandlerException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Throwable sourceException;

	public JdbcHandlerException(Throwable sourceException) {
		this.sourceException = sourceException;

	}

	public JdbcHandlerException(String errorMessage) {
		super(errorMessage);

	}

}


