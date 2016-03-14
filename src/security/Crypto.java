package security;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

/**
 * Contains functions for hashing strings and validating hashes.
 * 
 * Reference: http://howtodoinjava.com/security/how-to-generate-secure-password-hash-md5-sha-pbkdf2-bcrypt-examples/
 */
public class Crypto {
	
	private Crypto() {
	}
	
	private static final String ALGORITHM = "PBKDF2WithHmacSHA1";
	private static final int ITERATIONS = 1000;
	private static final int KEY_LENGTH = 1024;
	private static final int SALT_LENGTH = 64;
	
	/**
	 * Hashing with SHA-256.
	 * 
	 * @param text clear text to hash.
	 * 
	 * @return hashed version of input.
	 */
	public static String simpleHash(String text) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			md.update(text.getBytes("UTF-8"));
			
			byte[] digest = md.digest();
			String hash = String.format("%064x", new java.math.BigInteger(1, digest));
			return hash;
			
		} catch (UnsupportedEncodingException | NoSuchAlgorithmException ex) {
			Logger.getLogger(Crypto.class.getName()).log(Level.SEVERE, null, ex);
			return "hashing error";
		}
	}
	
	
	/**
	 * Generates a hash from the input clear text.
	 * 
	 * @param clear The clear text to hash.
	 * 
	 * @return The generated hash.
	 */
	public static String generateHash(String clear) {
		try {
			SecureRandom srnd = SecureRandom.getInstance("SHA1PRNG");
			byte[] salt = new byte[SALT_LENGTH];
			srnd.nextBytes(salt);
		
			PBEKeySpec keySpec = new PBEKeySpec(clear.toCharArray(), salt, ITERATIONS, KEY_LENGTH);
			SecretKeyFactory keyFact = SecretKeyFactory.getInstance(ALGORITHM);
			
			byte[] hash = keyFact.generateSecret(keySpec).getEncoded();
			
			String saltStr = Base64.getEncoder().encodeToString(salt);
			String hashStr = Base64.getEncoder().encodeToString(hash);
			return saltStr + ":" + hashStr;
		} catch (Exception ex) {
			Logger.getLogger(Crypto.class.getName()).log(Level.SEVERE, null, ex);
			return null;
		}
	}
	
	/**
	 * Validates the clear text with the hash.
	 * 
	 * @param clear The clear text.
	 * @param hashed The hash.
	 * @return Returns {@code true} if the clear text matches the hash.
	 */
	public static boolean validateHash(String clear, String hashed) {
		try {
			String[] parts = hashed.split(":");
			
			byte[] salt = Base64.getDecoder().decode(parts[0]);
			byte[] hash = Base64.getDecoder().decode(parts[1]);
			
			PBEKeySpec keySpec = new PBEKeySpec(clear.toCharArray(), salt, ITERATIONS, KEY_LENGTH);
			SecretKeyFactory keyFact = SecretKeyFactory.getInstance(ALGORITHM);
			byte[] testHash = keyFact.generateSecret(keySpec).getEncoded();
			
			// Check if they are equal
			if (hash.length != testHash.length)
				return false;
			for (int i = 0; i < hash.length; i++) {
				if (hash[i] != testHash[i])
					return false;
			}
			return true;
		} catch (Exception ex) {
			Logger.getLogger(Crypto.class.getName()).log(Level.SEVERE, null, ex);
			return false;
		}
	}
}
