package org.example;

import org.example.Keys.KeyGeneration;

import java.security.*;
import java.util.Base64;

public class Test {
    public static void main(String[] args) throws Exception {
        // Generate a key pair (private and public keys)
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048);
        KeyPair keyPair = keyGen.generateKeyPair();

        // Get the private key
        PrivateKey privateKey = keyPair.getPrivate();
        PublicKey publicKey = keyPair.getPublic();

        // Message to be signed
        byte[] messageData = "This is a test message".getBytes();

        // Sign the message
        Signature signature1 = Signature.getInstance("SHA256withRSA");
        signature1.initSign(privateKey);
        signature1.update(messageData);
        byte[] signedData1 = signature1.sign();

        Thread.sleep(1000);
        // Sign again to see if it produces a different signature
        Signature signature2 = Signature.getInstance("SHA256withRSA");
        signature2.initSign(privateKey);
        signature2.update(messageData);
        byte[] signedData2 = signature2.sign();

        // Output the signatures (in Base64 for readability)
        System.out.println("Signature 1: " + Base64.getEncoder().encodeToString(signedData1));
        System.out.println("Signature 2: " + Base64.getEncoder().encodeToString(signedData2));
    }
}
