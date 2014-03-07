import java.util.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.io.FileInputStream;

/**
 * Runs queries against a back-end database
 */
public class Query {
	private String configFilename;
	private Properties configProps = new Properties();

	private String jSQLDriver;
	private String jSQLUrl;
	private String jSQLUser;
	private String jSQLCustomer;
	private String jSQLPassword;

	// DB Connection
	private Connection conn;
        private Connection customerConn;

	// Canned queries

	// LIKE does a case-insensitive match
	private static final String SEARCH_SQL_BEGIN =
		"SELECT * FROM movie WHERE name LIKE '%";
	private static final String SEARCH_SQL_END = 
		"%' ORDER BY id";

	private static final String DIRECTOR_MID_SQL = "SELECT y.* "
					 + "FROM movie_directors x, directors y "
					 + "WHERE x.mid = ? and x.did = y.id";
	private PreparedStatement directorMidStatement;
	
	private static final String FAST_ACTORS_BEGIN = "SELECT m.id, a.fname, a.lname FROM " +
		"movie m, casts c, actor a " +
		"WHERE m.name like '%";
	private static final String FAST_ACTORS_END = "%' " +
		"and m.id = c.mid and c.pid = a.id " +
		"order by m.id";
	
	private static final String GET_ACTORS_SQL = "SELECT m.id, a.fname, a.lname FROM " +
					"movie m, casts c, actor a " + 
					"WHERE m.name = ? and " +
					"m.id = c.mid " + 
					"and c.pid = a.id ";
	private PreparedStatement getActorsForMovies;
	
	private static final String FAST_DIRECTORS_BEGIN = "SELECT m.id, d.fname, d.lname FROM " +
		"movie m, movie_directors md, directors d " +
		"WHERE m.name like '%";
	private static final String FAST_DIRECTORS_END = "%' " +
		"and m.id = md.mid and md.did = d.id " +
		"order by m.id";
	
	private static final String RENTED_SQL = "SELECT CASE " +
					"WHEN cid = ? " +
					"THEN 'YOU HAVE IT' " +
					"ELSE 'UNAVAILABLE' " +
					"END " +
					"FROM Rental " +
					"WHERE mid = ? and status = 'open'";
	private PreparedStatement alreadyRented;
	
	private static final String CUST_INFO_SQL = 
			"select fname, maxnum, maxnum - r.checkedOut as remaining " +
			"from customer c, subscription s, " +
			  "(select count(*) as checkedOut from Rental " +
			  "where status = 'open' " +
			  "and cid = ?) as r " +
			"where  c.sid = s.sid " +
			"and c.cid = ?";
	private PreparedStatement customerInfo;
	
	private static final String ALL_PLANS_SQL = "select * from Subscription";
	private static final String SUB_IDS_SQL = "select sid from Subscription";
	
	private static final String UPDATE_SID_SQL = "UPDATE Customer SET sid = ? WHERE cid = ?";
	private PreparedStatement updateSid;
	
	private static final String CURRENT_RENTALS_SQL = "select count(*) from Rental where status = 'open' and cid=?";
	private PreparedStatement numRentals;
	
	private static final String MAX_RENTALS_SQL = "select maxnum from Subscription where sid = ?";
	private PreparedStatement maxRentals;
	
	private static final String ALL_RENTALS_SQL = "select id from Movie";
	
	private static final String NEW_RENTAL_SQL = "INSERT into Rental values(?, ?, 'open', GETDATE())";
	private PreparedStatement newTuple;
	
	private static final String RENTER_ID_SQL = "select cid from Rental where mid = ?";
	private PreparedStatement renterId;
	
	/* uncomment, and edit, after your create your own customer database */
	
	private static final String CUSTOMER_LOGIN_SQL = 
		"SELECT * FROM customer WHERE login = ? and password = ?";
	private PreparedStatement customerLoginStatement;

	private static final String BEGIN_TRANSACTION_SQL = 
		"SET TRANSACTION ISOLATION LEVEL SERIALIZABLE; BEGIN TRANSACTION;";
	private PreparedStatement beginTransactionStatement;

	private static final String COMMIT_SQL = "COMMIT TRANSACTION";
	private PreparedStatement commitTransactionStatement;

	private static final String ROLLBACK_SQL = "ROLLBACK TRANSACTION";
	private PreparedStatement rollbackTransactionStatement;
	
	

	public Query(String configFilename) {
		this.configFilename = configFilename;
	}

    /**********************************************************/
    /* Connection code to SQL Azure. Example code below will connect to the imdb database on Azure
       IMPORTANT NOTE:  You will need to create (and connect to) your new customer database before 
       uncommenting and running the query statements in this file .
     */

	public void openConnection() throws Exception {
		configProps.load(new FileInputStream(configFilename));

		jSQLDriver   = configProps.getProperty("videostore.jdbc_driver");
		jSQLUrl	   = configProps.getProperty("videostore.imdb_url");
		jSQLCustomer = configProps.getProperty("videostore.customer_url");				
		jSQLUser	   = configProps.getProperty("videostore.sqlazure_username");
		jSQLPassword = configProps.getProperty("videostore.sqlazure_password");


		/* load jdbc drivers */
		Class.forName(jSQLDriver).newInstance();

		/* open connections to the imdb database */

		conn = DriverManager.getConnection(jSQLUrl, // database
						   jSQLUser, // user
						   jSQLPassword); // password
                
		conn.setAutoCommit(true); //by default automatically commit after each statement 

		/* You will also want to appropriately set the 
                   transaction's isolation level through:  
		   conn.setTransactionIsolation(...) */

		/* Also you will put code here to specify the connection to your
		   customer DB.  E.g.

		   customerConn = DriverManager.getConnection(...);
		   customerConn.setAutoCommit(true); //by default automatically commit after each statement
		   customerConn.setTransactionIsolation(...); //
		*/
		customerConn = DriverManager.getConnection(jSQLCustomer, jSQLUser, jSQLPassword);
		customerConn.setAutoCommit(true);
	        
	}

	public void closeConnection() throws Exception {
		conn.close();
		customerConn.close();
	}

    /**********************************************************/
    /* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */

	public void prepareStatements() throws Exception {

		directorMidStatement = conn.prepareStatement(DIRECTOR_MID_SQL);
		getActorsForMovies = conn.prepareStatement(GET_ACTORS_SQL);
		alreadyRented = customerConn.prepareStatement(RENTED_SQL);
		customerInfo = customerConn.prepareStatement(CUST_INFO_SQL);
		updateSid = customerConn.prepareStatement(UPDATE_SID_SQL);
		numRentals = customerConn.prepareStatement(CURRENT_RENTALS_SQL);
		maxRentals = customerConn.prepareStatement(MAX_RENTALS_SQL);
		newTuple = customerConn.prepareStatement(NEW_RENTAL_SQL);
		renterId = customerConn.prepareStatement(RENTER_ID_SQL);

		/* uncomment after you create your customers database */
		
		customerLoginStatement = customerConn.prepareStatement(CUSTOMER_LOGIN_SQL);
		beginTransactionStatement = customerConn.prepareStatement(BEGIN_TRANSACTION_SQL);
		commitTransactionStatement = customerConn.prepareStatement(COMMIT_SQL);
		rollbackTransactionStatement = customerConn.prepareStatement(ROLLBACK_SQL);
		

		/* add here more prepare statements for all the other queries you need */
		/* . . . . . . */
	}


    /**********************************************************/
    /* Suggested helper functions; you can complete these, or write your own
       (but remember to delete the ones you are not using!) */

	public int getRemainingRentals(int cid) throws Exception {
		/* How many movies can she/he still rent?
		   You have to compute and return the difference between the customer's plan
		   and the count of outstanding rentals */
		return (99);
	}

	public String getCustomerName(int cid) throws Exception {
		/* Find the first and last name of the current customer. */
		return ("JoeFirstName" + " " + "JoeLastName");

	}

	public boolean isValidPlan(int planid) throws Exception {
		Statement ids = customerConn.createStatement();
		ResultSet subscriptionIds = ids.executeQuery(SUB_IDS_SQL);
		while(subscriptionIds.next()){
			if (planid ==subscriptionIds.getInt(1)){
				subscriptionIds.close();
				return true;
			}
		}
		subscriptionIds.close();
		return false;
	}

	public boolean isValidMovie(int mid) throws Exception {
		Statement validMovie = conn.createStatement();
		ResultSet movieIds = validMovie.executeQuery(ALL_RENTALS_SQL);
		while(movieIds.next()){
			if(movieIds.getInt(1) == mid){
				movieIds.close();
				return true;
			}
		}
		movieIds.close();
		return false;
	}
	
	private int getRenterID(int mid) throws Exception {
		/* Find the customer id (cid) of whoever currently rents the movie mid; return -1 if none */
		renterId.clearParameters();
		renterId.setInt(1, mid);
		ResultSet result = renterId.executeQuery();
		if(result.next()){
			result.close();
			return result.getInt(1);
		}
		else{ 
			result.close();
			return -1;
		}
	}

    /**********************************************************/
    /* login transaction: invoked only once, when the app is started  */
	public int transaction_login(String name, String password) throws Exception {
		/* authenticates the user, and returns the user id, or -1 if authentication fails */

		/* Uncomment after you create your own customers database */
		
		int cid;

		customerLoginStatement.clearParameters();
		customerLoginStatement.setString(1,name);
		customerLoginStatement.setString(2,password);
		ResultSet cid_set = customerLoginStatement.executeQuery();
		if (cid_set.next()) cid = cid_set.getInt(1);
		else cid = -1;
		cid_set.close();
		return(cid);
	}

	public void transaction_printPersonalData(int cid) throws Exception {
		/* println the customer's personal data: name, and plan number */
		customerInfo.clearParameters();
		customerInfo.setInt(1, cid); 
		customerInfo.setInt(2, cid);
		ResultSet cust_info_set = customerInfo.executeQuery();
		cust_info_set.next();
		System.out.println("Hello, " + cust_info_set.getString(1) + 
				"! You have " + cust_info_set.getInt(3) + " out of " + cust_info_set.getInt(2) + " rentals remaining.");
		cust_info_set.close();
	}


    /**********************************************************/
    /* main functions in this project: */

	public void transaction_search(int cid, String movie_title)
			throws Exception {
		/* searches for movies with matching titles: SELECT * FROM movie WHERE name LIKE movie_title */
		/* prints the movies, directors, actors, and the availability status:
		   AVAILABLE, or UNAVAILABLE, or YOU CURRENTLY RENT IT */

		/* Interpolate the movie title into the SQL string */
		String searchSql = SEARCH_SQL_BEGIN + movie_title + SEARCH_SQL_END;
		
		Statement searchStatement = conn.createStatement();
		ResultSet movie_set = searchStatement.executeQuery(searchSql);
		while (movie_set.next()) {
			int mid = movie_set.getInt(1);
			System.out.println("ID: " + mid + " NAME: "
					+ movie_set.getString(2) + " YEAR: "
					+ movie_set.getString(3));
			/* do a dependent join with directors */
			directorMidStatement.clearParameters();
			directorMidStatement.setInt(1, mid);
			ResultSet director_set = directorMidStatement.executeQuery();
			while (director_set.next()) {
				System.out.println("\t\tDirector: " + director_set.getString(3)
						+ "," + director_set.getString(2));
			}
			director_set.close();
			getActorsForMovies.clearParameters();
			getActorsForMovies.setString(1, movie_set.getString(2));
			ResultSet actor_set = getActorsForMovies.executeQuery();
			while(actor_set.next()){
				System.out.println("\t\tActor: " + actor_set.getString(2) + ", " + actor_set.getString(3));
			}
			actor_set.close();
			alreadyRented.clearParameters();
			alreadyRented.setInt(1, cid);
			alreadyRented.setInt(2, mid);
			ResultSet rented_set = alreadyRented.executeQuery();
			if(rented_set.next()){
				System.out.println("\t\tStatus: " + rented_set.getString(1));
			}else{
				System.out.println("\t\tStatus: Available");
			}
			rented_set.close();
		}
		movie_set.close();
		System.out.println();
	}

	public void transaction_choosePlan(int cid, int pid) throws Exception {
	    /* updates the customer's plan to pid: UPDATE customer SET plid = pid */
	    /* remember to enforce consistency ! */
		beginTransaction();
		updateSid.clearParameters();
		updateSid.setInt(1, pid);
		updateSid.setInt(2, cid);
		updateSid.execute();
		numRentals.clearParameters();
		numRentals.setInt(1, cid);
		ResultSet num = numRentals.executeQuery();
		maxRentals.clearParameters();
		maxRentals.setInt(1, pid);
		ResultSet max = maxRentals.executeQuery();
		num.next();
		max.next();
		int maxMovs = max.getInt(1);
		int numMovs = num.getInt(1);
		num.close();
		max.close();
		String movs = " Movie";
		if(numMovs - maxMovs != 1)
			movs += "s";
		if(numMovs > maxMovs){
			rollbackTransaction();
			System.out.println("Sorry, you must return " + (numMovs - maxMovs) + movs + " before switching to this plan.");
		} else{
			commitTransaction();
		}
	}

	public void transaction_listPlans() throws Exception {
	    /* println all available plans: SELECT * FROM plan */
		Statement plans = customerConn.createStatement();
		ResultSet allPlans = plans.executeQuery(ALL_PLANS_SQL);
		while(allPlans.next()){
			System.out.println("Plan Id: " + allPlans.getInt(1) + "\tPlan Name: " + allPlans.getString(2) + 
					" \tMaximum Rentals: " + allPlans.getInt(3) + "\tMonthly Price: " + allPlans.getFloat(4));
		}
		allPlans.close();
	}

	public void transaction_rent(int cid, int mid) throws Exception {
	    /* rent the movie mid to the customer cid */
	    /* remember to enforce consistency ! */
		beginTransaction();
		int renterId = getRenterID(mid);
		if(renterId == cid){
			rollbackTransaction();
			System.out.println("You are already renting this movie.");
		} else if(renterId != -1){
			rollbackTransaction();
			System.out.println("Sorry, this movie has been rented by another person.");
		} else if(isValidMovie(mid)){
			newTuple.clearParameters();
			newTuple.setInt(1, mid);
			newTuple.setInt(2, cid);
			newTuple.execute();
			commitTransaction();
		} else{
			rollbackTransaction();
			System.out.println("Not a valid movie id");
		}
	}

	public void transaction_return(int cid, int mid) throws Exception {
	    /* return the movie mid by the customer cid */
	}

	public void transaction_fastSearch(int cid, String movie_title)
			throws Exception {
		/* like transaction_search, but uses joins instead of dependent joins
		   Needs to run three SQL queries: (a) movies, (b) movies join directors, (c) movies join actors
		   Answers are sorted by mid.
		   Then merge-joins the three answer sets */
		String searchSql = SEARCH_SQL_BEGIN + movie_title + SEARCH_SQL_END;
		String fastActorSql = FAST_ACTORS_BEGIN + movie_title + FAST_ACTORS_END; 
		String fastDirectorSql = FAST_DIRECTORS_BEGIN + movie_title + FAST_DIRECTORS_END;
		
		Statement searchStatement = conn.createStatement();
		Statement actorStatement = conn.createStatement();
		Statement directorStatement = conn.createStatement();
		
		ResultSet movie_set = searchStatement.executeQuery(searchSql);
		ResultSet director_set = directorStatement.executeQuery(fastDirectorSql);
		ResultSet actor_set = actorStatement.executeQuery(fastActorSql);
		
		Map<Integer, ArrayList<String>> strings = new TreeMap<Integer, ArrayList<String>>();
		
		while(movie_set.next()){
			int mid = movie_set.getInt(1);
			strings.put(mid, new ArrayList<String>(Arrays.asList(
					"ID: " + mid + " NAME: "
					+ movie_set.getString(2) + " YEAR: "
					+ movie_set.getString(3))));
		}
		while(director_set.next()){
			int mid = director_set.getInt(1);
			ArrayList<String> temp = strings.get(mid);
			temp.add("\t\tDirector: " + director_set.getString(3) + ", " + director_set.getString(2));
			strings.put(mid, temp);
		}
		while(actor_set.next()){
			int mid = actor_set.getInt(1);
			ArrayList<String> temp = strings.get(mid);
			temp.add("\t\tActor: " + actor_set.getString(3) + ", " + actor_set.getString(2));
			strings.put(mid, temp);
		}
		for(int i : strings.keySet()){
			ArrayList<String> toPrint = strings.get(i);
			for(String j : toPrint){
				System.out.println(j);
			}
		}
		movie_set.close();
		director_set.close();
		actor_set.close();
	}
	

    /* Uncomment helpers below once you've got beginTransactionStatement,
       commitTransactionStatement, and rollbackTransactionStatement setup from
       prepareStatements():*/
    
       public void beginTransaction() throws Exception {
	    customerConn.setAutoCommit(false);
	    beginTransactionStatement.executeUpdate();	
        }

        public void commitTransaction() throws Exception {
	    commitTransactionStatement.executeUpdate();	
	    customerConn.setAutoCommit(true);
	}
        public void rollbackTransaction() throws Exception {
	    rollbackTransactionStatement.executeUpdate();
	    customerConn.setAutoCommit(true);
	    } 
    

}
