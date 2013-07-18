package mp2;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

/**
 * This class is used by the LogQuerier in order to allow query of a
 * log created by a ClientNode
 *
 */
public class TcpConnectionManager implements Runnable {

	private Socket socket;

	/**
	 * 
	 */
	public TcpConnectionManager(Socket theSocket) {
		socket = theSocket;

		Thread t = new Thread(this);
		t.start();
	}

	/**
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Runnable#run()
	 */
	public void run() {

		try {

			ObjectInputStream ois = new ObjectInputStream(
					socket.getInputStream());

			ObjectOutputStream oos = new ObjectOutputStream(
					socket.getOutputStream()); 

			Object inputLine; 

			GrepHandler handler = new GrepHandler();

			String responseHeader = (String)ois.readObject();
			
			inputLine = ois.readObject(); 
			
			handler.grepInline((String) inputLine,responseHeader, oos);

			ois.close();
			oos.close();
			socket.close();

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
