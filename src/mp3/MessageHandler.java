package mp3;
import java.net.DatagramPacket;
import java.net.DatagramSocket; 
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;

/**
 * Handles messages received from nodes
 */
public class MessageHandler implements Runnable {
	
	public static final String ACK_MESSAGE = "ack";
	public static final String PING_MESSAGE = "ping";
	
	public static final String JOIN_OK_PREFIX = "ok";
	public static final String JOIN_REQUEST_PREFIX = "joinreq";
	public static final String LEAVE_MESSAGE_PREFIX = "leave";
	public static final String JOINED_MESSAGE_PREFIX = "joined";
	public static final String CRASHED_MESSAGE_PREFIX = "crashed";
	public static final String MEMBERS_MESSAGE_PREFIX = "members";
	
	private DatagramSocket socket;
	private DatagramPacket packet;
	private ClientNode node;
	
	private String packetHostname;
	private int packetPort;
	private int messagesize;
 
	/**
	 * Initializes a new instance of the message handler class
	 * 
	 * @param socket
	 * @param packet
	 * @param node
	 */
	public MessageHandler(DatagramSocket socket,DatagramPacket packet,ClientNode node){
		this.socket = socket;
		this.node = node;
		this.packet = packet;
		
		this.packetHostname = packet.getAddress().getHostName();
		this.packetPort = packet.getPort();
		
		Thread t = new Thread(this);
		t.start();
	}
	

	/**
	 * @param message The message in our packet
	 */
	public void processMessage(String message){
		if (node.getIgnoreMessage())
			return;
		
		if (message.startsWith(PING_MESSAGE)){
			handlePing();
		}
		else if (message.startsWith(ACK_MESSAGE)){
			handleAck();
		}
		else if (message.startsWith(JOIN_REQUEST_PREFIX)){
			handleJoinRequest(message);
		} 
		else if (message.startsWith(MEMBERS_MESSAGE_PREFIX)){
			handleMembersList(message);
		}
		else if (message.startsWith(JOINED_MESSAGE_PREFIX)){
			handleJoined(message);
		}
		else if (message.startsWith(LEAVE_MESSAGE_PREFIX)){
			handleLeave(message);
		}
		else if (message.startsWith(CRASHED_MESSAGE_PREFIX)){
			handleCrashed(message);
		}

		customHandleMessage(message);
	}
	
	/**
	 * @return the current node in charge of handling this message
	 */
	public ClientNode getClientNode(){
		return this.node;
	}
 
	/**
	 * @param message
	 * The message format is joinreq:memberId where memberId is the
	 * id of the node which originated the request to the introducer
	 */
	public synchronized void handleJoinRequest(String message){
		//checks the current node to make sure it is the introducer
		if (!(node.getIsIntroducer()))
			return;
		
		node.log(String.format("membership request received (%d bytes)",messagesize));
		 		
		String [] blocks = message.split(":");	
		String memberId = blocks[1].trim();  
		
		node.addMember(memberId);
		
		String membersStr;
		
		synchronized (node.membershipList) {
			membersStr = Helper.join(node.membershipList);
		}
		
		String membersListMessage = String.format("%s:%s",MessageHandler.MEMBERS_MESSAGE_PREFIX,membersStr);
			
    	Helper.sendUnicastMessage(socket ,membersListMessage, packetHostname, packetPort);
    	
    	node.log(String.format("membership accepted for [%s], sending member list...",memberId));
	}
	
	/**
	 * handles ping messages upon their arrival
	 */
	public void handlePing(){	
        node.log(String.format("ping received from <%s:%d> (%d bytes)", packetHostname, packetPort,messagesize));
		Helper.sendUnicastMessage(socket,ACK_MESSAGE,  packetHostname, packetPort);
	}
	
	/**
	 * handles ack messages upon their arrival
	 */
	public void handleAck(){
		node.setAckReceived();
		node.log(String.format("ack sent to <%s:%d>", packetHostname, packetPort));
	}
	
	/**
	 * Handles membership list provided by the introducer and
	 * add members from that list to the current node's membership list
	 * @param message
	 * The message format is "members:member1,member2,member3..."
	 */
	public synchronized void handleMembersList(String message){
		node.setMembershipAccepted();
		
		String [] blocks = message.split(":");
		String [] memberList = blocks[1].split(",");
		
		int numMembers = 0 ;
		
		ArrayList<String> broadcastList = new ArrayList<String>();
		
		//add members from received list in our own list
		if (memberList != null){	
			node.membershipList =  new ArrayList<String>();
			node.membersSet = new HashSet<String>();
			
			for(String member:memberList){
				node.membershipList.add(member);
				node.membersSet.add(member);
				
				if (!node.clientId.equals(member))
					broadcastList.add(member);
				
				numMembers+=1;
			}	
		}
		
		node.log(String.format("member list received: %d members in group (%d bytes)",numMembers,messagesize));
		node.log(String.format("ring sequence: [%s]",Helper.join(node.membershipList,"->")));
			
		Collections.sort(node.membershipList);
		
		//start sending ping messages to the next node in the ring 
		node.startPingManager();
		
		//broadcast joined message to everyone in the list
		String joinedMessage = String.format("%s:%s",JOINED_MESSAGE_PREFIX,node.clientId);
		
		Helper.sendBMulticastMessage(socket, joinedMessage, broadcastList);
		node.log(String.format("new join multicast to %d nodes  (%d bytes)", 
				broadcastList.size(),joinedMessage.length() *  broadcastList.size()));
	}
	
	/**
	 * Process the message and adds the member to our list
	 * @param message The message to be processed
	 * the message has the form "joined:memberId" where memberId is 
	 * the id of the node who joined
	 */
	public synchronized void handleJoined(String message){
		String [] info = message.split(":");		
		node.addMember(info[1]);
		node.log(String.format("reporting %d live nodes", node.membershipList.size()));
	}
	
	/**
	 * @param message
	 * The message format is "leave:memberId" where memberId is the id
	 * of the leaving member
	 */
	public void handleLeave(String message){
		String [] info = message.split(":");		
		node.removeMember(info[1]);
		node.log(String.format("reporting %d live nodes", node.membershipList.size()));
	}
	
	/**
	 * @param message
	 * The message format is "crashed:memberId" where memberId is the 
	 * Id of the crashed node
	 */
	public void handleCrashed(String message){
		String [] info = message.split(":");		
		node.removeMember(info[1]);
		node.log(String.format("reporting %d live nodes", node.membershipList.size()));
	}

	@Override
	public void run() {
		messagesize = packet.getLength();
		String message = new String(packet.getData(),0,packet.getLength());
		processMessage(message);
	}
	
	public DatagramSocket getSocket(){
		return this.socket;
	}
	
	/**
	 * @param message
	 * Gives derived classes a chance to further handle received messages
	 */
	public void customHandleMessage(String message){
		
	}
}
