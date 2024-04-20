
/**
author: Sisi Duan. 
Implemented a simple key store interface so that we can deal with the storage and encryption in the python module 
 */

package bftsmart.demo.keyvalue;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ServiceReplica;
//import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;
import bftsmart.tom.server.defaultservices.DefaultRecoverable;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.net.ServerSocket;
import java.net.Socket;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import java.nio.ByteBuffer;
import java.security.Security;

import bftsmart.tom.util.Storage;

import java.nio.ByteBuffer;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;

public final class KVServer1 extends DefaultRecoverable {
    private int interval;

    public final static String DEFAULT_CONFIG_FOLDER = "./library/config/";
    private int id;
    // private ServiceReplica replia = null;
    private ServiceReplica replica;
    private String configFolder;
    private int sequence = 0;
    private static DataOutputStream fos;

    private static ServerSocket forwardServer = null;
    private static Socket forwardSocket = null;

    private Storage totalLatency = null;
    private Storage consensusLatency = null;
    private Storage preConsLatency = null;
    private Storage posConsLatency = null;
    private Storage proposeLatency = null;
    private Storage writeLatency = null;
    private Storage acceptLatency = null;
    private int iterations = 0;
    private float maxTp = -1;
    private long throughputMeasurementStartTime = System.currentTimeMillis();

    public KVServer1(int id, int interval) { // throws IOException {
        this.interval = interval;
        this.id = id;
        this.configFolder = (configFolder != null ? configFolder : KVServer.DEFAULT_CONFIG_FOLDER);
        this.sequence = 0;
        System.out.println("DEBUG KVServer1 constructor 0");
        totalLatency = new Storage(interval);
        consensusLatency = new Storage(interval);
        preConsLatency = new Storage(interval);
        posConsLatency = new Storage(interval);
        proposeLatency = new Storage(interval);
        writeLatency = new Storage(interval);
        acceptLatency = new Storage(interval);
        System.out.println("DEBUG KVServer1 constructor 1");
        System.out.println("Use: java KVServer <processId>");

        replica = new ServiceReplica(id, this.configFolder, this, this);
        System.out.println("DEBUG KVServer1 constructor 2");
    }

    public static void test() {
        System.out.println("test jpype server\n");
    }

    public static void passArgs(String[] args) {
        // public static void main(String[] args){

        if (args.length < 2) {
            System.out.println("Use: java KVServer <processId>");
            System.exit(-1);
        }

        System.out.println("DEBUG not main but passargs");

        int myID = Integer.parseInt(args[0]);
        int interval = Integer.parseInt(args[1]);
        Security.addProvider(new BouncyCastleProvider());
        new KVServer1(myID, interval);

    }

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Use: java KVServer <processId>");
            System.exit(-1);
        }

        int myID = Integer.parseInt(args[0]);
        int interval = Integer.parseInt(args[1]);
        new KVServer1(myID, interval);

    }

    public static void getThroughput(String file, String conent) {
        BufferedWriter out = null;
        try {
            out = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(file, true)));
            out.write(conent + "\n");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx) {
        System.out.println("unordered\n");
        // TODO: Need to figure out for read requests
        return new byte[0];
        // return executeSingle(commands,msgCtxs);
    }

    @Override
    public byte[][] appExecuteBatch(byte[][] commands, MessageContext[] msgCtxs, boolean fromConsensus) {

        byte[][] replies = new byte[commands.length][];
        System.out.println("commands.length" + commands.length);
        for (int i = 0; i < commands.length; i++) {
            if (msgCtxs != null && msgCtxs[i] != null) {
                try {
                    replies[i] = executeSingle(commands[i], msgCtxs[i], fromConsensus);
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
        }
        return replies;
    }

    private byte[] executeSingle(byte[] command, MessageContext msgCtx, boolean fromConsensus) throws IOException {
        // System.out.println("Execute single\n");

        iterations++;
        // System.out.println(System.getProperty("file.separator"));

        float tp = -1;
        System.out.println("--- Measurements after " + iterations + " ops (" + interval + " samples) ---" + interval);

        if (iterations % interval == 0) {
            // if (context) System.out.println("--- (Context) iterations: "+ iterations + "
            // // regency: " + msgCtx.getRegency() + " // consensus: " +
            // msgCtx.getConsensusId() + " ---");

            System.out.println("--- Measurements after " + iterations + " ops (" + interval + " samples) ---");

            tp = (float) (interval * 1000 / (float) (System.currentTimeMillis() - throughputMeasurementStartTime));
            // tp =
            // (float)(interval*1000/(float)(System.currentTimeMillis()-throughputMeasurementStartTime));
            getThroughput(System.getProperty("user.dir") + "throughput.txt", Float.toString(tp));
            // System.out.println(System.getProperty("user.dir"));
            if (tp > maxTp)
                maxTp = tp;

            System.out.println("Throughput = " + tp + " operations/sec (Maximum observed: " + maxTp + " ops/sec)");

            // System.out.println("Total latency = " + totalLatency.getAverage(false) / 1000
            // + " (+/- "+ (long)totalLatency.getDP(false) / 1000 +") us ");
            // totalLatency.reset();
            // System.out.println("Consensus latency = " +
            // consensusLatency.getAverage(false) / 1000 + " (+/- "+
            // (long)consensusLatency.getDP(false) / 1000 +") us ");
            // consensusLatency.reset();
            // System.out.println("Pre-consensus latency = " +
            // preConsLatency.getAverage(false) / 1000 + " (+/- "+
            // (long)preConsLatency.getDP(false) / 1000 +") us ");
            // preConsLatency.reset();
            // System.out.println("Pos-consensus latency = " +
            // posConsLatency.getAverage(false) / 1000 + " (+/- "+
            // (long)posConsLatency.getDP(false) / 1000 +") us ");
            // posConsLatency.reset();
            // System.out.println("Propose latency = " + proposeLatency.getAverage(false) /
            // 1000 + " (+/- "+ (long)proposeLatency.getDP(false) / 1000 +") us ");
            // proposeLatency.reset();
            // System.out.println("Write latency = " + writeLatency.getAverage(false) / 1000
            // + " (+/- "+ (long)writeLatency.getDP(false) / 1000 +") us ");
            // writeLatency.reset();
            // System.out.println("Accept latency = " + acceptLatency.getAverage(false) /
            // 1000 + " (+/- "+ (long)acceptLatency.getDP(false) / 1000 +") us ");
            // acceptLatency.reset();

            throughputMeasurementStartTime = System.currentTimeMillis();
        }

        RequestTuple tuple = deserializeSignedRequest(command);
        if (tuple.type.equals("CONFIG")) {
            System.out.println("---Config request\n");
        } // We do not have config messages for now. It might be useful in the future

        if (tuple.type.equals("SEQUENCE")) {
            System.out.println("---Sequence\n");
            byte[][] reply = new byte[2][];
            // System.out.println("channelID: "+tuple.channelID+"\n");
            // System.out.println("payload: "+tuple.payload+"\n");
            // System.out.println("payload: "+new String(tuple.payload)+"\n");
            reply[0] = "SEQUENCE".getBytes();
            reply[1] = ByteBuffer.allocate(4).putInt(this.sequence).array();
            // reply[2] = tuple.payload;

            // TODO: Nedd to handle configuration file to avoid hard-coded host name and
            // port number
            // We need to deliver sequence number -- this.sequence, client id -- channelID,
            // and message content --payload
            int port = 5000 + this.id;
            Socket socket = new Socket("localhost", port);
            byte[][] deliver = new byte[3][];
            deliver[0] = ByteBuffer.allocate(4).putInt(this.sequence).array();
            deliver[1] = ByteBuffer.allocate(4).putInt(Integer.parseInt(tuple.channelID)).array();
            deliver[2] = tuple.payload;
            // byte[] bytes = serializeContents(deliver);
            byte[] bytes = serializeDelivery(this.sequence, Integer.parseInt(tuple.channelID), tuple.payload);
            DataOutputStream fos = new DataOutputStream(socket.getOutputStream());
            // fos.writeInt(bytes.length);
            fos.write(bytes);
            /*
             * fos.writeInt(this.sequence);
             * fos.flush();
             * fos.writeInt(Integer.parseInt(tuple.channelID));
             * fos.flush();
             * fos.writeUTF(new String(tuple.payload));
             */
            fos.flush();
            fos.close();
            socket.close();

            this.sequence++;

            return serializeContents(reply);
        }

        return "ACK".getBytes();
    }

    private byte[] serializeDelivery(int sequence, int cid, byte[] msg) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);

        out.writeInt(sequence);
        out.flush();
        bos.flush();

        out.writeInt(cid);
        out.flush();
        bos.flush();

        out.writeInt(msg.length);
        out.flush();
        bos.flush();

        out.write(msg);
        out.flush();
        bos.flush();

        out.close();
        bos.close();
        return bos.toByteArray();
    }

    private byte[] serializeContents(byte[][] contents) throws IOException {

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);

        out.writeInt(contents.length);

        out.flush();
        bos.flush();
        for (int i = 0; i < contents.length; i++) {

            out.writeInt(contents[i].length);

            out.write(contents[i]);

            out.flush();
            bos.flush();
        }

        out.close();
        bos.close();
        return bos.toByteArray();

    }

    private RequestTuple deserializeSignedRequest(byte[] request) throws IOException {

        ByteArrayInputStream bis = new ByteArrayInputStream(request);
        DataInput in = new DataInputStream(bis);

        int l = in.readInt();
        byte[] msg = new byte[l];
        in.readFully(msg);
        l = in.readInt();
        byte[] sig = new byte[l];
        in.readFully(sig);

        bis.close();

        bis = new ByteArrayInputStream(msg);
        in = new DataInputStream(bis);

        String type = in.readUTF();
        String channelID = in.readUTF();
        l = in.readInt();
        byte[] payload = new byte[l];
        in.readFully(payload);

        bis.close();

        /*
         * System.out.println("---type, "+type+"\n");
         * System.out.println("---channelID, "+channelID+"\n");
         * System.out.println("---payload, "+payload+"\n");
         */

        return new RequestTuple(type, channelID, payload, sig);

    }

    private RequestTuple deserializeRequest(byte[] request) throws IOException {

        ByteArrayInputStream bis = new ByteArrayInputStream(request);
        DataInput in = new DataInputStream(bis);

        String type = in.readUTF();
        String channelID = in.readUTF();
        int l = in.readInt();
        byte[] payload = new byte[l];
        in.readFully(payload);

        bis.close();

        return new RequestTuple(type, channelID, payload, null);

    }

    private RequestTuple deserializeSginedRequest(byte[] request) throws IOException {
        ByteArrayInputStream bis = new ByteArrayInputStream(request);
        DataInput in = new DataInputStream(bis);

        int l = in.readInt();
        byte[] msg = new byte[l];
        in.readFully(msg);
        l = in.readInt();
        byte[] sig = new byte[l];
        in.readFully(sig);

        bis.close();

        bis = new ByteArrayInputStream(msg);
        in = new DataInputStream(bis);

        String type = in.readUTF();
        String channelID = in.readUTF();
        l = in.readInt();
        byte[] payload = new byte[l];
        in.readFully(payload);

        bis.close();

        return new RequestTuple(type, channelID, payload, sig);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void installSnapshot(byte[] state) {
        System.out.println("install snapshot...\n");
        // TODO: Need to be extended for a fully functional version
    }

    @Override
    public byte[] getSnapshot() {
        System.out.println("get snapshot...\n");
        // TODO: Need to be extended for a fully functional version
        return new byte[0];
    }

    private class RequestTuple {
        String type = null;
        String channelID = null;
        byte[] payload = null;
        byte[] signature = null;

        RequestTuple(String type, String channelID, byte[] payload, byte[] signature) {
            this.type = type;
            this.channelID = channelID;
            this.payload = payload;
            this.signature = signature;
        }
    }
}
