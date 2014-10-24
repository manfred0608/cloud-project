import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.mysql.jdbc.Statement;

/**
 * Servlet implementation class Q2
 */
@WebServlet("/Q2")
public class Q2 extends HttpServlet {
	private static final long serialVersionUID = 1L;
	
	private Connection conn = null;
	private Statement stmt = null;
	
	private static String ID1;
	private static String ID2;
	private static String ID3;
	private static String TEAMID;

	private static String username = null;
	private static String password = null;
	private static String ip = null;
	private static String ip2 = null;
	private static String dbname = null;
	private static String tablename = null;

	// private static
	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public Q2() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see Servlet#init(ServletConfig)
	 */
	public void init(ServletConfig config) throws ServletException {
		// TODO Auto-generated method stub
		ID1 = config.getServletContext().getInitParameter("AWS_ACCOUNT_ID1");
		ID2 = config.getServletContext().getInitParameter("AWS_ACCOUNT_ID2");
		ID3 = config.getServletContext().getInitParameter("AWS_ACCOUNT_ID3");
		TEAMID = config.getServletContext().getInitParameter("TEAMID");

		username = config.getInitParameter("USERNAME");
		password = config.getInitParameter("PASSWORD");
		dbname = config.getInitParameter("DBNAME");
		ip = config.getInitParameter("IP");
		ip2 = config.getInitParameter("IP2");
		tablename = config.getInitParameter("TABLENAME");
	}

	/**
	 * @see Servlet#getServletConfig()
	 */
	public ServletConfig getServletConfig() {
		// TODO Auto-generated method stub
		return null;
	}

	public List<String[]> getEMRRes(String userid, String tt) {
		String ftt = null;
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat sdf2 = new SimpleDateFormat(
				"E MMM dd HH:mm:ss +0000 yyyy");
		
		Date date = null;
		try {
			date = sdf1.parse(tt);
			ftt = sdf2.format(date);
			//System.out.println(ftt);
			return Query.query(userid, ftt);
		} catch (ParseException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	/*
	 * 
	 * /**
	 * 
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 * response)
	 */
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub

		String userid = request.getParameter("userid");
		String tt = request.getParameter("tweet_time");
		
		Query.begin(ip2);
		List<String[]> results = null;
		StringBuffer sb = new StringBuffer();
		try {
			results = getEMRRes(userid, tt);
			if (results == null)
				return;

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			String title = TEAMID + "," + ID1 + "," + ID2 + "," + ID3 + "\n";
			sb.append(title);

			for (String[] s : results) {
				sb.append(s[0] + ":" + s[2] + ":" + s[1] + "\n");
			}
			
			response.setCharacterEncoding("UTF-8");
			response.setContentType("text/html");
			PrintWriter out = response.getWriter();
			out.print(sb.toString());
			Query.close();
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
	}
}
