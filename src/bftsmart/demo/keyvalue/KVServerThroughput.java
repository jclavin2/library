
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
import bftsmart.tom.util.Storage;

import java.nio.ByteBuffer;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;

public final class KVServerThroughput extends DefaultRecoverable {
    public final static String DEFAULT_CONFIG_FOLDER = "./library/config/";
    private int id;
    private int replySize;
    private boolean context;

    private byte[] state;
    private ServiceReplica replia = null;
    private String configFolder;
    private int sequence = 0;
    private static DataOutputStream fos;
    private int port = 0;
    private int totalBatchSize = 0;
    private int batches = 0;

    private int interval;
    private int iterations = 0;
    private int readiterations = 0;
    private float maxTp = -1;
    private long throughputMeasurementStartTime = System.currentTimeMillis();

    private Storage totalLatency = null;
    private Storage consensusLatency = null;
    private Storage preConsLatency = null;
    private Storage posConsLatency = null;
    private Storage proposeLatency = null;
    private Storage writeLatency = null;
    private Storage acceptLatency = null;

    private static ServerSocket forwardServer = null;
    private static Socket forwardSocket = null;

    public KVServerThroughput(int id, int replySize, int stateSize, boolean context) { // throws IOException {
        this.id = id;
        this.replySize = replySize;
        this.context = context;

        this.state = new byte[stateSize];

        for (int i = 0; i < stateSize; i++)
            state[i] = (byte) i;
        // this.configFolder = (configFolder != null ? configFolder:
        // KVServer.DEFAULT_CONFIG_FOLDER);
        this.sequence = 0;
        this.port = 5000 + this.id;
        this.totalBatchSize = 0;
        this.batches = 0;
        new ServiceReplica(this.id, this, this);
    }

    public KVServerThroughput(int id, int baseport, int interval) { // throws IOException {
        this.id = id;
        // this.configFolder = (configFolder != null ? configFolder:
        // KVServer.DEFAULT_CONFIG_FOLDER);
        this.sequence = 0;

        this.port = baseport + this.id;
        this.totalBatchSize = 0;
        this.batches = 0;
        this.interval = interval;

        // System.out.println("interval: "+interval);
        totalLatency = new Storage(interval);
        consensusLatency = new Storage(interval);
        preConsLatency = new Storage(interval);
        posConsLatency = new Storage(interval);
        proposeLatency = new Storage(interval);
        writeLatency = new Storage(interval);
        acceptLatency = new Storage(interval);

        new ServiceReplica(this.id, this, this);
    }

    public static void test() {
        System.out.println("test jpype server\n");
    }

    public static void passArgs(String[] args) {
        // public static void main(String[] args){
        if (args.length < 1) {
            System.out.println("Use: java KVServerThroughput <processId> <port> <interval>");
            System.exit(-1);
        }

        int myID = Integer.parseInt(args[0]);
        int baseport = Integer.parseInt(args[1]);
        int interval = Integer.parseInt(args[2]);

        new KVServerThroughput(myID, baseport, interval);

    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.out.println("Use: java KVServerThroughput <processId>");
            System.exit(-1);
        }

        int myID = Integer.parseInt(args[0]);
        // new KVServerThroughput(myID);

    }

    @Override
    public byte[][] appExecuteBatch(byte[][] commands, MessageContext[] msgCtxs, boolean fromConsensus) {

        byte[][] replies = new byte[commands.length][];

        for (int i = 0; i < commands.length; i++) {

            replies[i] = execute(commands[i], msgCtxs[i]);

        }

        return replies;
    }

    @Override
    public byte[] appExecuteUnordered(byte[] command, MessageContext msgCtx) {
        return execute(command, msgCtx);
    }

    public byte[] execute(byte[] command, MessageContext msgCtx) {
        boolean readOnly = false;

        iterations++;

        if (msgCtx != null && msgCtx.getFirstInBatch() != null) {

            readOnly = msgCtx.readOnly;

            msgCtx.getFirstInBatch().executedTime = System.nanoTime();

            totalLatency.store(msgCtx.getFirstInBatch().executedTime - msgCtx.getFirstInBatch().receptionTime);

            if (readOnly == false) {

                consensusLatency
                        .store(msgCtx.getFirstInBatch().decisionTime - msgCtx.getFirstInBatch().consensusStartTime);
                long temp = msgCtx.getFirstInBatch().consensusStartTime - msgCtx.getFirstInBatch().receptionTime;
                preConsLatency.store(temp > 0 ? temp : 0);
                posConsLatency.store(msgCtx.getFirstInBatch().executedTime - msgCtx.getFirstInBatch().decisionTime);
                proposeLatency
                        .store(msgCtx.getFirstInBatch().writeSentTime - msgCtx.getFirstInBatch().consensusStartTime);
                writeLatency.store(msgCtx.getFirstInBatch().acceptSentTime - msgCtx.getFirstInBatch().writeSentTime);
                acceptLatency.store(msgCtx.getFirstInBatch().decisionTime - msgCtx.getFirstInBatch().acceptSentTime);

            } else {

                consensusLatency.store(0);
                preConsLatency.store(0);
                posConsLatency.store(0);
                proposeLatency.store(0);
                writeLatency.store(0);
                acceptLatency.store(0);

            }

        } else {

            consensusLatency.store(0);
            preConsLatency.store(0);
            posConsLatency.store(0);
            proposeLatency.store(0);
            writeLatency.store(0);
            acceptLatency.store(0);

        }

        float tp = -1;
        if (iterations % interval == 0) {
            // if (context) System.out.println("--- (Context) iterations: "+ iterations + "
            // // regency: " + msgCtx.getRegency() + " // consensus: " +
            // msgCtx.getConsensusId() + " ---");

            System.out.println("--- Measurements after " + iterations + " ops (" + interval + " samples) ---");

            tp = (float) (interval * 1000 / (float) (System.currentTimeMillis() - throughputMeasurementStartTime));

            if (tp > maxTp)
                maxTp = tp;

            System.out.println("Throughput = " + tp + " operations/sec (Maximum observed: " + maxTp + " ops/sec)");

            System.out.println("Total latency = " + totalLatency.getAverage(false) / 1000 + " (+/- "
                    + (long) totalLatency.getDP(false) / 1000 + ") us ");
            totalLatency.reset();
            System.out.println("Consensus latency = " + consensusLatency.getAverage(false) / 1000 + " (+/- "
                    + (long) consensusLatency.getDP(false) / 1000 + ") us ");
            consensusLatency.reset();
            System.out.println("Pre-consensus latency = " + preConsLatency.getAverage(false) / 1000 + " (+/- "
                    + (long) preConsLatency.getDP(false) / 1000 + ") us ");
            preConsLatency.reset();
            System.out.println("Pos-consensus latency = " + posConsLatency.getAverage(false) / 1000 + " (+/- "
                    + (long) posConsLatency.getDP(false) / 1000 + ") us ");
            posConsLatency.reset();
            System.out.println("Propose latency = " + proposeLatency.getAverage(false) / 1000 + " (+/- "
                    + (long) proposeLatency.getDP(false) / 1000 + ") us ");
            proposeLatency.reset();
            System.out.println("Write latency = " + writeLatency.getAverage(false) / 1000 + " (+/- "
                    + (long) writeLatency.getDP(false) / 1000 + ") us ");
            writeLatency.reset();
            System.out.println("Accept latency = " + acceptLatency.getAverage(false) / 1000 + " (+/- "
                    + (long) acceptLatency.getDP(false) / 1000 + ") us ");
            acceptLatency.reset();

            throughputMeasurementStartTime = System.currentTimeMillis();
        }

        return new byte[replySize];
    }

    private byte[] serializeDelivery(int type, int sequence, int cid, int encrypted, byte[] label, byte[] time,
            byte[] msg) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);

        // This looks ugly.... requestID = encrypted, topic = msg

        out.writeInt(type);
        out.flush();
        bos.flush();

        out.writeInt(sequence);
        out.flush();
        bos.flush();

        out.writeInt(cid);
        out.flush();
        bos.flush();

        out.writeInt(encrypted);
        out.flush();
        bos.flush();

        out.writeInt(label.length);
        out.flush();
        bos.flush();

        out.writeInt(time.length);
        out.flush();
        bos.flush();

        out.writeInt(msg.length);
        out.flush();
        bos.flush();

        out.write(label);
        out.flush();
        bos.flush();

        out.write(time);
        out.flush();
        bos.flush();

        out.write(msg);
        out.flush();
        bos.flush();

        out.close();
        bos.close();
        return bos.toByteArray();
    }

    private byte[] serializeVerifySignatureDelivery(int type, int sequence, int cid, int encrypted, String label,
            String time, String msg, String msg1) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);

        // This looks ugly....

        out.writeInt(type);
        out.flush();
        bos.flush();

        out.writeInt(sequence);
        out.flush();
        bos.flush();

        out.writeInt(cid);
        out.flush();
        bos.flush();

        out.writeInt(encrypted);
        out.flush();
        bos.flush();

        out.writeInt(label.length());
        out.flush();
        bos.flush();

        out.writeInt(time.length());
        out.flush();
        bos.flush();

        out.writeInt(msg.length());
        out.flush();
        bos.flush();

        out.writeInt(msg1.length());
        out.flush();
        bos.flush();

        out.writeUTF(label);
        out.flush();
        bos.flush();

        out.writeUTF(time);
        out.flush();
        bos.flush();

        out.writeUTF(msg);
        out.flush();
        bos.flush();

        out.writeUTF(msg1);
        out.flush();
        bos.flush();

        out.close();
        bos.close();
        return bos.toByteArray();
    }

    private byte[] serializeReadDelivery(int type, int sequence, int cid, int replicaID, byte[] tid)
            throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);

        out.writeInt(0);
        out.flush();
        bos.flush();

        out.writeInt(sequence);
        out.flush();
        bos.flush();

        out.writeInt(cid);
        out.flush();
        bos.flush();

        out.writeInt(replicaID);
        out.flush();
        bos.flush();

        out.writeInt(tid.length);
        out.flush();
        bos.flush();

        out.write(tid);
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

    private ReadRequestTuple deserializeSignedReadRequest(byte[] request) throws IOException {

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
        String replicaID = in.readUTF();
        String tID = in.readUTF();
        bis.close();

        return new ReadRequestTuple(type, channelID, replicaID, tID, sig);

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
        if (!type.equals("SEQUENCE") && !type.equals("SUBSCRIBE") && !type.equals("OW") && !type.equals("REGISTER")) {
            // bis.close();
            return new RequestTuple(type, "", "", "", "", "".getBytes(), "".getBytes());
        }
        String channelID = in.readUTF();
        String encrypted = in.readUTF(); // requestID = encrypted
        String label = in.readUTF();
        String time = in.readUTF();
        l = in.readInt();
        byte[] payload = new byte[l];
        in.readFully(payload); // topic = payload

        bis.close();

        /*
         * System.out.println("---type, "+type+"\n");
         * System.out.println("---channelID, "+channelID+"\n");
         * System.out.println("---payload, "+payload+"\n");
         */

        return new RequestTuple(type, channelID, encrypted, label, time, payload, sig);

    }

    private VerifySignatureRequestTuple deserializeSignedVerifySignatureRequest(byte[] request) throws IOException {

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
        if (!type.equals("Verify")) {
            // bis.close();
            return new VerifySignatureRequestTuple(type, "", "", "", "", "", "", "".getBytes());
        }
        String channelID = in.readUTF();
        String encrypted = in.readUTF();
        String label = in.readUTF();
        String time = in.readUTF();
        String payload = in.readUTF();
        String payload2 = in.readUTF();
        // System.out.println("------this is in KVServerThroughput-----"+payload);
        // System.out.println("-----this is in KVServerThroughput------"+payload2);

        bis.close();

        /*
         * System.out.println("---type, "+type+"\n");
         * System.out.println("---channelID, "+channelID+"\n");
         * System.out.println("---payload, "+payload+"\n");
         */

        return new VerifySignatureRequestTuple(type, channelID, encrypted, label, time, payload, payload2, sig);

    }

    private RequestTuple deserializeRequest(byte[] request) throws IOException {

        ByteArrayInputStream bis = new ByteArrayInputStream(request);
        DataInput in = new DataInputStream(bis);

        String type = in.readUTF();
        String channelID = in.readUTF();
        String encrypted = in.readUTF();
        String label = in.readUTF();
        String time = in.readUTF();
        int l = in.readInt();
        byte[] payload = new byte[l];
        in.readFully(payload);

        bis.close();

        return new RequestTuple(type, channelID, encrypted, label, time, payload, null);

    }

    private VerifySignatureRequestTuple deserializeVerifySignatureRequest(byte[] request) throws IOException {

        ByteArrayInputStream bis = new ByteArrayInputStream(request);
        DataInput in = new DataInputStream(bis);

        String type = in.readUTF();
        String channelID = in.readUTF();
        String encrypted = in.readUTF();
        String label = in.readUTF();
        String time = in.readUTF();
        String payload = in.readUTF();
        String payload2 = in.readUTF();

        bis.close();

        return new VerifySignatureRequestTuple(type, channelID, encrypted, label, time, payload, payload2, null);

    }

    @SuppressWarnings("unchecked")
    @Override
    public void installSnapshot(byte[] state) {
        System.out.println("install snapshot...\n");
        // TODO: Need to be extended for a fully functional version
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
    public byte[] getSnapshot() {
        System.out.println("get snapshot...\n");
        // TODO: Need to be extended for a fully functional version
        return new byte[0];
    }

    private class RequestTuple {
        String type = null;
        String channelID = null;
        String encrypted = null; // requestID = encrypted
        String label = null;
        String time = null;
        byte[] payload = null; // topic = payload
        byte[] signature = null;

        RequestTuple(String type, String channelID, String encrypted, String label, String time, byte[] payload,
                byte[] signature) {
            this.type = type;
            this.channelID = channelID;
            this.encrypted = encrypted;
            this.payload = payload;
            this.label = label;
            this.time = time;
            this.signature = signature;
        }
    }

    private class VerifySignatureRequestTuple {
        String type = null;
        String channelID = null;
        String encrypted = null;
        String label = null;
        String time = null;
        String payload = null;
        String payload2 = null;
        byte[] signature = null;

        VerifySignatureRequestTuple(String type, String channelID, String encrypted, String label, String time,
                String payload, String payload2, byte[] signature) {
            this.type = type;
            this.channelID = channelID;
            this.encrypted = encrypted;
            this.payload = payload;
            this.payload2 = payload2;
            this.label = label;
            this.time = time;
            this.signature = signature;
        }
    }

    private class ReadRequestTuple {
        String type = null;
        String channelID = null;
        String replicaID = null;
        String tID = null;
        byte[] signature = null;

        ReadRequestTuple(String type, String channelID, String replicaID, String tID, byte[] signature) {
            this.type = type;
            this.channelID = channelID;
            this.replicaID = replicaID;
            this.tID = tID;
            this.signature = signature;
        }
    }
}
