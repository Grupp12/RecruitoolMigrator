package recruitool.migrator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import security.Crypto;

/**
 * The {@code DatabaseMigrator} is a small program that is
 * used for migrating the legacy database into our new database.
 */
public class RecruitoolMigrator {
	private RecruitoolMigrator() {
	}
	
	public static void main(String[] args) {
		boolean cmd = false;
		if (args.length > 0) {
			if (args[0].equals("-cmd")) {
				cmd = true;
			}
		}
		
		File oldSqlFile;
		
		ArrayList<Byte> outputBuf = new ArrayList<>();
		if (cmd)
		{
			if (args.length > 1) {
				oldSqlFile = new File(args[1]);
			}
			else {
				oldSqlFile = new File("old.sql");
			}
		}
		else {
			System.setOut(new PrintStream(new OutputStream() {
				@Override
				public void write(int b) throws IOException {
					outputBuf.add((byte)b);
				}
			}));
			
			JFileChooser fc = new JFileChooser(".");
			fc.showOpenDialog(null);
			oldSqlFile = fc.getSelectedFile();			
		}
		
		try {
			migrate(oldSqlFile);
			
			if (!cmd) {
				byte[] bufArr = new byte[outputBuf.size()];
				for (int i = 0; i < bufArr.length; i++) {
					bufArr[i] = outputBuf.get(i);
				}
				String output = new String(bufArr);
				JOptionPane.showMessageDialog(null, output, "Migration Log", JOptionPane.INFORMATION_MESSAGE);
			}
		}
		catch (SQLException | IOException ex) {
			if (cmd) {
				ex.printStackTrace();
			}
			else {
				JOptionPane.showMessageDialog(null, ex.getMessage(), "Error!", JOptionPane.ERROR_MESSAGE);
			}
			System.exit(-1);
		}
	}
	
	private static Connection oldConn;
	private static Connection newConn;
	
	private static HashMap<Long, String> roles = new HashMap<>();
	private static HashMap<Long, String> competences = new HashMap<>();
	
	// Legacy objects
	private static HashMap<Long, LegacyAccount> oldAccounts = new HashMap<>();
	private static HashMap<Long, LegacyAvailability> oldAvailabilities = new HashMap<>();
	private static HashMap<Long, LegacyCompetenceProfile> oldProfiles = new HashMap<>();
	
	private static final Timestamp NOW = new Timestamp(System.currentTimeMillis());
	
	private static void migrate(File oldSqlFile) throws SQLException, IOException {
		oldConn = DriverManager.getConnection("jdbc:derby:memory:mig_db;create=true");
		parseLegacySqlScript(oldSqlFile);
		System.out.println("Legacy database created!");

		newConn = DriverManager.getConnection("jdbc:derby://localhost:1527/recruitool;user=root;password=1234");
		newConn.setAutoCommit(false);
		System.out.println("Connected to new database!");

		loadLegacyTables();
		oldConn.close();

		migrateAccounts();
		migrateApplications();
		newConn.commit();
		newConn.close();

		System.out.println("Database migration completed!");
	}
	
	private static void loadLegacyTables() throws SQLException {
		loadRoles();
		System.out.printf("%d roles loaded.\n", roles.size());

		loadAccounts();
		System.out.printf("%d accounts loaded.\n", oldAccounts.size());
		
		loadAvailabilities();
		System.out.printf("%d availabilities loaded.\n", oldAvailabilities.size());
		
		loadCompetences();
		System.out.printf("%d competences loaded.\n", competences.size());
		
		loadCompetenceProfiles();
		System.out.printf("%d competence profiles loaded.\n", competences.size());
	}
	
	private static void loadRoles() throws SQLException {
		try (Statement stmt = oldConn.createStatement())
		{
			ResultSet rs = stmt.executeQuery("SELECT * FROM role");
			while (rs.next())
			{
				long id = rs.getLong("role_id");

				String name = rs.getString("name").toUpperCase();

				// Fix wrong name in legacy database
				if (name.equals("RECRUIT"))
					name = "RECRUITER";

				roles.put(id, name);
			}
		}
	}
	
	private static void loadAccounts() throws SQLException {
		try (Statement stmt = oldConn.createStatement())
		{
			ResultSet rs = stmt.executeQuery("SELECT * FROM person");
			while (rs.next())
			{
				long id = rs.getLong("person_id");

				String firstName = rs.getString("name");
				String lastName = rs.getString("surname");

				String ssn = rs.getString("ssn");

				String email = rs.getString("email");

				String username = rs.getString("username");
				String password = rs.getString("password");

				long role_id = rs.getLong("role_id");

				// Hash password
				if (password != null)
					password = Crypto.simpleHash(password);
				
				// Create legacy account
				LegacyAccount account = new LegacyAccount();

				account.firstName = firstName;
				account.lastName = lastName;

				account.ssn = ssn;

				account.email = email;

				account.username = username;
				account.password = password;

				account.role_id = role_id;

				oldAccounts.put(id, account);
			}
		}
	}
	
	private static void loadAvailabilities() throws SQLException {
		try (Statement stmt = oldConn.createStatement())
		{
			ResultSet rs = stmt.executeQuery("SELECT * FROM availability");
			while (rs.next())
			{
				long id = rs.getLong("availability_id");

				Date fromDate = rs.getDate("from_date");			
				Date toDate = rs.getDate("to_date");

				long account_id = rs.getLong("person_id");

				// Create legacy availability
				LegacyAvailability availability = new LegacyAvailability();

				availability.id = id;

				availability.fromDate = fromDate;
				availability.toDate = toDate;

				availability.account_id = account_id;

				oldAvailabilities.put(id, availability);
			}
		}
	}
	
	private static void loadCompetences() throws SQLException {
		try (Statement stmt = oldConn.createStatement())
		{
			ResultSet rs = stmt.executeQuery("SELECT * FROM competence");
			while (rs.next())
			{
				long id = rs.getLong("competence_id");

				String name = rs.getString("name");

				competences.put(id, name);
			}
		}
	}
	
	private static void loadCompetenceProfiles() throws SQLException {
		try (Statement stmt = oldConn.createStatement())
		{
			ResultSet rs = stmt.executeQuery("SELECT * FROM competence_profile");
			while (rs.next())
			{
				long id = rs.getLong("competence_profile_id");

				BigDecimal yearsOfExp = rs.getBigDecimal("years_of_experience");

				long account_id = rs.getLong("person_id");
				long competence_id = rs.getLong("competence_id");

				// Create legacy competence profile
				LegacyCompetenceProfile profile = new LegacyCompetenceProfile();

				profile.id = id;

				profile.yearsOfExp = yearsOfExp;

				profile.account_id = account_id;
				profile.competence_id = competence_id;

				oldProfiles.put(id, profile);
			}
		}
	}
	
	private static void migrateAccounts() throws SQLException {
		String newAccSql = "insert into " +
				"ACCOUNT(FIRSTNAME, LASTNAME, EMAIL, USERNAME, PASSWORD, ACC_ROLE, SSN) " +
				"values(?, ?, ?, ?, ?, ?, ?)";
		try (PreparedStatement newAccStmt = newConn.prepareStatement(newAccSql))
		{
			for (LegacyAccount oldAcc : oldAccounts.values())
			{
				newAccStmt.setString(1, oldAcc.firstName);
				newAccStmt.setString(2, oldAcc.lastName);

				newAccStmt.setString(3, oldAcc.email);

				newAccStmt.setString(4, oldAcc.username);
				newAccStmt.setString(5, oldAcc.password);

				String acc_role = roles.get(oldAcc.role_id);
				newAccStmt.setString(6, acc_role);

				newAccStmt.setString(7, oldAcc.ssn);

				newAccStmt.executeUpdate();
			}
		}
		
		migrateAccountGroups();
	}
	private static MigratedAccount getMigratedAccount(String ssn) throws SQLException {
		String getAccSql = "select * from ACCOUNT where SSN=?";
		try (PreparedStatement getAccStmt = newConn.prepareStatement(getAccSql))
		{
			getAccStmt.setString(1, ssn);
			
			ResultSet rs = getAccStmt.executeQuery();
			rs.next();
			
			MigratedAccount acc = new MigratedAccount();
			
			acc.id = rs.getLong("ID");
	
			acc.firstName = rs.getString("FIRSTNAME");
			acc.lastName = rs.getString("LASTNAME");

			acc.email = rs.getString("EMAIL");

			acc.username = rs.getString("USERNAME");
			acc.password = rs.getString("PASSWORD");

			acc.acc_role = rs.getString("ACC_ROLE");

			acc.ssn = rs.getString("SSN");
			
			return acc;
		}
	}
	private static void migrateAccountGroups() throws SQLException {
		String newAccGrpSql = "insert into " +
				"ACCOUNT_GROUPS(GROUPNAME, USERNAME, ACCOUNT) " +
				"values(?, ?, ?)";
		try (PreparedStatement newAccGrpStmt = newConn.prepareStatement(newAccGrpSql))
		{
			String getAccsSql = "select * from ACCOUNT";
			try (PreparedStatement getAccsStmt = newConn.prepareStatement(getAccsSql))
			{
				ResultSet rs = getAccsStmt.executeQuery();
				while (rs.next())
				{
					long acc_id = rs.getLong("ID");
					String groupName = rs.getString("ACC_ROLE");
					String username = rs.getString("USERNAME");
					
					// Applicants don't have any login credentials
					if (groupName.equals("APPLICANT"))
						continue;
					
					newAccGrpStmt.setString(1,groupName);
					newAccGrpStmt.setString(2, username);
					newAccGrpStmt.setLong(3, acc_id);

					newAccGrpStmt.executeUpdate();
				}
			}
		}
	}
	
	private static void migrateApplications() throws SQLException {
		migrateAvailabilities();
		migrateCompetences();
		migrateCompetenceProfiles();
	}
	
	private static MigratedApplication createMigratedApplication(MigratedAccount acc) throws SQLException {
		String newApplSql = "insert into " +
				"APPLICATION(APPL_STATUS, TIME_OF_REG, ACC_ID) " +
				"values(?, ?, ?)";
		try (PreparedStatement newApplStmt = newConn.prepareStatement(newApplSql))
		{
			newApplStmt.setString(1, "SUBMITTED");
			newApplStmt.setTimestamp(2, NOW);

			newApplStmt.setLong(3, acc.id);

			newApplStmt.execute();
		}
		
		return getMigratedApplication(acc);
	}
	private static MigratedApplication getMigratedApplication(MigratedAccount acc) throws SQLException {
		String getApplSql = "select * from APPLICATION where ACC_ID=?";
		try (PreparedStatement getApplStmt = newConn.prepareStatement(getApplSql))
		{
			getApplStmt.setLong(1, acc.id);

			ResultSet rs = getApplStmt.executeQuery();
			if (!rs.next())
				return createMigratedApplication(acc);
			
			MigratedApplication appl = new MigratedApplication();
			
			appl.id = rs.getLong("ID");
			
			appl.appl_status = rs.getString("APPL_STATUS");
			appl.timeOfReg = rs.getTimestamp("TIME_OF_REG");
			
			appl.acc_id = acc.id;
			
			return appl;
		}
	}
	
	private static void migrateAvailabilities() throws SQLException {
		String newAvailSql = "insert into " +
				"AVAILABILITY(FROM_DATE, TO_DATE, APPL_ID) " +
				"values(?, ?, ?)";
		try (PreparedStatement newAvailStmt = newConn.prepareStatement(newAvailSql))
		{
			for (LegacyAvailability avail : oldAvailabilities.values())
			{
				LegacyAccount acc = oldAccounts.get(avail.account_id);
				
				MigratedApplication appl = getMigratedApplication(getMigratedAccount(acc.ssn));
				
				newAvailStmt.setDate(1, avail.fromDate);
				newAvailStmt.setDate(2, avail.toDate);
				
				newAvailStmt.setLong(3, appl.id);
				
				newAvailStmt.execute();
			}
		}
	}
	
	private static void migrateCompetences() throws SQLException {
		String newCompSql = "insert into " +
				"COMPETENCE(NAME) " +
				"values(?)";
		try (PreparedStatement newCompStmt = newConn.prepareStatement(newCompSql))
		{
			for (String comp : competences.values())
			{
				newCompStmt.setString(1, comp);
				
				newCompStmt.execute();
			}
		}
	}
	
	private static void migrateCompetenceProfiles() throws SQLException {
		String newCompProSql = "insert into " +
				"COMPETENCEPROFILE(YEARS_OF_EXP, COMP_ID, APPL_ID) " +
				"values(?, ?, ?)";
		try (PreparedStatement newCompProStmt = newConn.prepareStatement(newCompProSql))
		{
			for (LegacyCompetenceProfile profile : oldProfiles.values())
			{
				LegacyAccount acc = oldAccounts.get(profile.account_id);
				MigratedApplication appl = getMigratedApplication(getMigratedAccount(acc.ssn));
				String competence = competences.get(profile.competence_id);
				
				newCompProStmt.setBigDecimal(1, profile.yearsOfExp);
				
				newCompProStmt.setString(2, competence);
				
				newCompProStmt.setLong(3, appl.id);
				
				newCompProStmt.execute();
			}
		}
	}
	
	private static void parseLegacySqlScript(File oldSqlFile) throws IOException {
		InputStream inStream = new FileInputStream(oldSqlFile);
		Reader reader = new BufferedReader(new InputStreamReader(inStream, "UTF-8"));
		
		StringBuilder statement = new StringBuilder();
		
		int in;
		while ((in = reader.read()) != -1) {
			char ch = (char)in;
			if (ch == ';') {
				try (Statement stmt = oldConn.createStatement()) {
					stmt.execute(statement.toString());
				}
				catch (SQLException ex) {
					ex.printStackTrace();
				}
				finally {
					statement = new StringBuilder();
				}
			}
			else {
				statement.append(ch);
			}
		}
	}
}
