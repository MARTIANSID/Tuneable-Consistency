package org.example;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import org.example.Keys.KeyGeneration;

import java.security.*;
import java.util.Base64;

import org.ds.paxos.*;
public class Main {

    public static void main(String[] args) {
        try {
            // Retrieve keys
            PrivateKey privateKey = KeyGeneration.privateKeys.get(13);
            PublicKey publicKey = KeyGeneration.publicKeys.get(13);


            System.out.println("IN MAIN");
            System.out.println(publicKey);

            // Construct a Client message (assuming Transaction message is created as `transaction`)
            Client clientMessage = Client.newBuilder()
                    .setTimestamp("20189-10-29T12:00:00Z")
                    .setClientId(13)
                    .setT(Transaction.newBuilder().setSenderId(299).setReceiverId(1001).setAmount(2.0).build())
                    .build();

            // Sign the Client message (excluding the signature field)
            byte[] signatureBytes = signMessage(clientMessage.toBuilder().clearSignature().build().toByteArray(), privateKey);

            // Attach the signature to the Client message
            Client signedClientMessage = clientMessage.toBuilder()
                    .setSignature(ByteString.copyFrom(signatureBytes))
                    .build();

            ManagedChannel channel = ManagedChannelBuilder.forAddress("localhost",8001).usePlaintext().build();

            PbftGrpc.PbftStub asyncStub = PbftGrpc.newStub(channel);
//            asyncStub.clientForwardRequest(signedClientMessage, new StreamObserver<Reply>() {
//                @Override
//                public void onNext(Reply reply) {
//
//                }
//
//                @Override
//                public void onError(Throwable throwable) {
//
//                }
//
//                @Override
//                public void onCompleted() {
//
//                }
//            });

            asyncStub.clientRequest(signedClientMessage, new StreamObserver<Empty>() {
                @Override
                public void onNext(Empty reply) {

                }

                @Override
                public void onError(Throwable throwable) {

                }

                @Override
                public void onCompleted() {

                }
            });
            Thread.sleep(1000);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public static String createDigestOfClientMessage(Client c) throws NoSuchAlgorithmException {
        byte[] clientData = c.toBuilder().clearSignature().build().toByteArray();
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        digest.update(clientData);
        byte[] hashBytes = digest.digest();
        return Base64.getEncoder().encodeToString(hashBytes);
    }

    // Method to sign the message bytes
    public static byte[] signMessage(byte[] messageData, PrivateKey privateKey) throws Exception {
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(privateKey);
        signature.update(messageData);
        return signature.sign();
    }

    // Method to verify the Client message's signature with the public key
    public static boolean verifyMessage(Client clientMessage, PublicKey publicKey) throws Exception {
        // Extract the message data (excluding the signature)
        Client messageWithoutSignature = clientMessage.toBuilder().clearSignature().build();
        byte[] messageData = messageWithoutSignature.toByteArray();

        // Get the signature bytes
        byte[] signatureBytes = clientMessage.getSignature().toByteArray();

        // Verify the signature
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initVerify(publicKey);
        signature.update(messageData);
        return signature.verify(signatureBytes);
    }
}
