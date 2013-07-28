package mp3;
import java.io.IOException; 
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Timer;
import java.util.TimerTask;

import mp2.LogQuerierServer;
import mp2.Logger;

public class ClientNode {

	protected final String clientId;
	protected Ack ack;
	protected Ping ping;

	public ArrayList<String> membershipList;
	public HashSet<String> membersSet;

	private volatile boolean stop = false; 
	private String introducerHost = "localhost"; 

	private final int introducerPort;

	private String trackedNodeId;
	private final String hostname;
	private final int portNumber;
	private final Logger logger; 

	private DatagramSocket defaultSocket;
	private volatile boolean ackReceived = true; 
	private boolean ignoreMessages = false;
	private volatile boolean membershipAccepted = false;
	private ConnectionManager connectionManager; 
	
	private final String linux5 = "linux5.ews.illinois.edu";
	private final String linux6 = "linux6.ews.illinois.edu";
	private final String linux7 = "linux7.ews.illinois.edu"; 
	
	private Boolean isIntroducer = false;

	/**
	 * Initializes a new client
	 */
	public ClientNode(String hostName, int port) {
		this(hostName,port,0);
	}
	
	/**
	 * Initializes a new client which leaves after lifespan milliseconds
	 */
	public ClientNode(String hostName, int port,int lifespan) {
		this(hostName,port,-1,false,"localhost",10000,0);
	}
	
	/**
	 * Initializes a new client which leaves after lifespan milliseconds
	 */
	public ClientNode(String hostName, int port,String introducerHost,int introducerPort) {
		this(hostName,port,-1,false,introducerHost,introducerPort,0);
	}
	
	/**
	 * Initializes a new client which leaves after lifespan milliseconds
	 */
	public ClientNode(String hostName, int port,int lifespan,final boolean crash) {
		this(hostName,port,lifespan,crash,"localhost",10000,0);
	}
	
	/**
	 * Initializes a new client which leaves after lifespan milliseconds
	 * 
	 * @param hostName
	 * @param port
	 * @param lifespan
	 * @param crash
	 *            - specifies whether it is a crash or voluntary leave
	 */
	public ClientNode(String hostName, int port, int lifespan,
			final boolean crash,String introducerHost,int introducerPort,int lossRate) {
		
		this.hostname = hostName;
		this.portNumber = port;
		this.introducerHost = introducerHost;
		this.introducerPort = introducerPort;
		
		this.isIntroducer = hostName.equals(introducerHost) && 
				(port == introducerPort);
		
		String logname;
		
		if (this.hostname.equals(linux5)){
			logname = "machine.1.log";
		}else if (this.hostname.equals(linux6)){
			logname = "machine.2.log";
		}else if (this.hostname.equals(linux7)){
			logname = "machine.3.log";
		}else{
			logname = this.hostname + "-" + this.portNumber + ".log";
		}		
		
		clientId = Helper.generateNodeId(hostname, portNumber);
		
		logger = new Logger(this.hostname, this.portNumber,logname);
		
		//start log querier server application
		
		startLogQuerierServer(logname);
		
		// initialize the listening socket info
		try {
			defaultSocket = new DatagramSocket(this.portNumber);
			connectionManager = new ConnectionManager(defaultSocket, this,lossRate);

			initialize();

		} catch (IOException ex) {
			System.out.println(String.format(
					"Unable to listen to incoming connection on port %s",
					this.portNumber));
		}

		if (lifespan > 0) {
			(new Timer()).schedule(new TimerTask() {
				@Override
				public void run() {
					if (!crash) {
						leaveGroup();
					} else {
						crash();
					}
				}
			}, lifespan);
		}
	}
	
	public boolean getAckReceived() {
		return this.ackReceived;
	}

	public void setAckReceived() {
		this.ackReceived = true;
	}

	public void resetAckReceived() {
		this.ackReceived = false;
	}

	public void setMembershipAccepted() {
		this.membershipAccepted = true;
	}
	
	public void setIsIntroducer(boolean isIntro){
		this.isIntroducer = isIntro;
	}
	
	public boolean getIsIntroducer(){
		return this.isIntroducer;
	}
	
	public DatagramSocket getSocket(){
		return this.defaultSocket;
	}
	
	public String getTrackedNodeId(){
		return this.trackedNodeId;
	}

	/**
	 * Request group join or initalize membership
	 */
	public void initialize() {	
		if (isIntroducer){
			membershipList = new ArrayList<String>();
			membersSet = new HashSet<String>();
			
			membershipList.add(clientId);	
			membersSet.add(clientId);
		}else{
			joinGroup();
		}
	}

	/**
	 * @return the current node's hostname
	 */
	public String getHostname() {
		return this.hostname;
	}

	/**
	 * @return the current node's port number
	 */
	public int getNodePortNumber() {
		return this.portNumber;
	}

	/**
	 * Used for testing crashes
	 * 
	 * @return the flag for message handling
	 */
	public boolean getIgnoreMessage() {
		return this.ignoreMessages;
	}

	/**
	 * @param message
	 * A B-multicast message
	 */
	public void sendBMulticastMessage(final String message) {

		if (stop)
			return;

		synchronized (membershipList) {
			Helper.sendBMulticastMessage(defaultSocket, message, membershipList);
		}
	}

	/*
	 * initiate a join request and block until it is accepted
	 */
	public void joinGroup() {
		String message = String.format("%s:%s",
				MessageHandler.JOIN_REQUEST_PREFIX, clientId);
		Helper.sendUnicastMessage(defaultSocket, message, introducerHost,
				introducerPort);
		log(String.format("join request sent to <%s:%d>",introducerHost,introducerPort));

		// wait and verify the

		(new Timer()).schedule(new TimerTask() {
			public void run() {
				checkJoinStatus();
			}
		}, 5000);
	}

	/**
	 * 
	 */
	public void checkJoinStatus() {
		if (!membershipAccepted) {
			log("unable to join the group");
			reset();
		}
	}

	/**
	 * initiates a leave
	 */
	public void leaveGroup() {
		if (!(this.isIntroducer) &&  !membershipAccepted)
			return;
		
		String message = String.format("%s:%s",
				MessageHandler.LEAVE_MESSAGE_PREFIX, clientId);
		log(String.format("voluntarily leaving the group (%d bytes)",message.length()));
		sendBMulticastMessage(message);

		reset();
	}

	/**
	 * 
	 */
	private void reset() {
		stop = true;
		ignoreMessages = true;
		connectionManager.stop = true;

		if (ping != null)
			ping.stop = true;

		ackReceived = true;
	}

	/**
	 * 
	 */
	public void crash() {
		reset();
	}

	/**
	 * @return the identifier for the client
	 */
	public String getNodeId() {
		return clientId;
	}

	/**
	 * Starts the ping manager to monitor the state of the provided client to
	 * which the node id belongs to
	 */
	public void startPingManager() {
		if (ping == null) {
			ping = new Ping(defaultSocket, this);
		} else {
			ping.stop = true;
			ping.waitStop();
			ping = new Ping(defaultSocket, this);
		}

		// find the node to ping
		synchronized (membershipList) {

			String nodeToTrack = getNodeToPing();

			if (nodeToTrack.equals("") || nodeToTrack.equals(trackedNodeId)
					|| nodeToTrack.equals(clientId))
				return;

			trackedNodeId = nodeToTrack;

			(new Thread() {
				public void run() {
					NodeInfo info = Helper.extractNodeInfoFromId(trackedNodeId);

					ping.Initialize(info.getHostname(), info.getPort());
					ping.autoRun();
				}
			}).start();
		}
	}

	/**
	 * Adds a new member to the membership list
	 * 
	 * @param id
	 */
	public synchronized void addMember(String id) {
		// 1- update the membership list
		synchronized (membershipList) {
			if (membersSet.contains(id))
				return;

			membersSet.add(id);
			membershipList.add(id);
			Collections.sort(membershipList);
		}

		String nodeToPing = getNodeToPing();

		if (!nodeToPing.equals(trackedNodeId)) {
			// restart the ping manager which forces it to find the successor it
			// should rightfully track
			startPingManager();
		}
	}

	/**
	 * @param id
	 */
	public synchronized void removeMember(String id) {
		// 1- update the membership list

		synchronized (membershipList) {
			if (!membersSet.contains(id))
				return;

			membersSet.remove(id);
			membershipList.remove(id);
			Collections.sort(membershipList);
		}

		String nodeToPing = getNodeToPing();

		if (!nodeToPing.equals(trackedNodeId)) {

			// restart the ping manager which forces it to find the successor it
			// should
			// rightfully track
			startPingManager();
		}
	}

	/**
	 * @return get the next node to ping in the ring
	 */
	private String getNodeToPing() {

		String nodeToTrack;

		synchronized (membershipList) {
			if (membershipList.size() < 2)
				return "";

			int currentIdx = membershipList.indexOf(clientId);

			if (currentIdx == -1) {
				System.out.println(String.format("Invalid clientId %s",clientId));
			}

			nodeToTrack = membershipList.get((currentIdx + 1)
					% membershipList.size());
		}

		return nodeToTrack;
	}

	/**
	 * logs the provided message string
	 */
	public synchronized void log(String message) {
		logger.log(message);
	}

	/**
	 * Handles ack not received scenario
	 */
	public void notifyUnreachable() {
		String crash_message = String.format("%s:%s",
				MessageHandler.CRASHED_MESSAGE_PREFIX, trackedNodeId);
		
		sendBMulticastMessage(crash_message);

		log(String.format("%s has crashed (%d bytes)", trackedNodeId,crash_message.length()));
	}
	
	
	/**
	 * @param logName
	 */
	public void startLogQuerierServer(final String logName){
		
		(new Thread(){
			@Override
			public void run(){
				(new LogQuerierServer(hostname,portNumber, logName)).listen(); 
			}
		}).start();
	}
	
	
	/**
	 * @param socket
	 * @param packet
	 * @param node
	 * @return
	 */
	public MessageHandler getMessageHandler(DatagramSocket socket,DatagramPacket packet,ClientNode node){
		return new MessageHandler(socket, packet, node);
	}

	/**
	 * Class entry point
	 */
	public static void main(String[] args) {

		if (args.length < 2) {
			System.out
					.println("Usage: ClientNode hostname port [introducer_host] [introducer_port] [lifespan] [crash] [loss_rate]");
			System.exit(0);
		}

		String hostName = args[0];
		int port = Integer.parseInt(args[1]);
		String introducerHost = "localhost";
		int introducerPort = 10000;
		int lifespan = 0;
		boolean crash = false;
		int loss_rate = 0;

		if (args.length > 2) {
			introducerHost =  args[2];
		}
		
		if (args.length > 3) {
			introducerPort =  Integer.parseInt(args[3]);
		}
		
		if (args.length > 4) {
			lifespan =  Integer.parseInt(args[4]);
		}
		
		if (args.length > 5) {
			crash =  Integer.parseInt(args[5]) == 1;
		}
		
		if (args.length > 6) {
			loss_rate =  Integer.parseInt(args[6]);
		}

		// an automatic group join is initiated here.
		new ClientNode(hostName, port,lifespan, crash, introducerHost, introducerPort, loss_rate);
	}
}
