package org.example.Keys;

import java.io.*;
import java.security.*;
import java.security.spec.*;
import java.util.concurrent.ConcurrentHashMap;

public class KeyGeneration {
    public static ConcurrentHashMap<Integer, PublicKey> publicKeys = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<Integer, PrivateKey> privateKeys = new ConcurrentHashMap<>();
    private static final String KEY_DIRECTORY = "keys/";

    static {
        File keyDir = new File(KEY_DIRECTORY);
        if (!keyDir.exists()) keyDir.mkdir();

        try {
            initializeKeys();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void initializeKeys() throws Exception {
        for (int i = 1; i <= 12; i++) {
            int id = i;
            loadOrGenerateKeyPair(id);
        }
            int id = 13;
            loadOrGenerateKeyPair(id);
    }

    private static void loadOrGenerateKeyPair(int id) throws Exception {
        File publicKeyFile = new File(KEY_DIRECTORY + "public_" + id + ".key");
        File privateKeyFile = new File(KEY_DIRECTORY + "private_" + id + ".key");

        if (publicKeyFile.exists() && privateKeyFile.exists()) {
            publicKeys.put(id, loadPublicKeyFromFile(publicKeyFile));
            privateKeys.put(id, loadPrivateKeyFromFile(privateKeyFile));
            System.out.println("Loaded existing keys for " + id);
        } else {
            KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
            keyGen.initialize(2048);
            KeyPair keyPair = keyGen.generateKeyPair();

            publicKeys.put(id, keyPair.getPublic());
            privateKeys.put(id, keyPair.getPrivate());

            saveKeyToFile(publicKeyFile, keyPair.getPublic());
            saveKeyToFile(privateKeyFile, keyPair.getPrivate());
            System.out.println("Generated and saved new keys for " + id);
        }
    }

    private static void saveKeyToFile(File file, Key key) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(key.getEncoded());
        }
    }

    private static PublicKey loadPublicKeyFromFile(File file) throws Exception {
        byte[] keyBytes = readFileToByteArray(file);
        X509EncodedKeySpec spec = new X509EncodedKeySpec(keyBytes);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        return keyFactory.generatePublic(spec);
    }

    private static PrivateKey loadPrivateKeyFromFile(File file) throws Exception {
        byte[] keyBytes = readFileToByteArray(file);
        PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(keyBytes);
        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        return keyFactory.generatePrivate(spec);
    }

    private static byte[] readFileToByteArray(File file) throws IOException {
        try (FileInputStream fis = new FileInputStream(file)) {
            return fis.readAllBytes();
        }
    }

    public static void reloadKeys() throws Exception {
        publicKeys.clear();
        privateKeys.clear();
        initializeKeys();
        System.out.println("All keys reloaded from files.");
    }

    public static void main(String[] args) {
        try {
            System.out.println("Keys initialized:");
            publicKeys.forEach((id, key) -> System.out.println(id + ": " + key));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
