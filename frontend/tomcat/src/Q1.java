import java.io.IOException;
import java.io.PrintWriter;
import java.math.BigInteger;
import java.text.SimpleDateFormat;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class Q1
 */
@WebServlet("/Q1")
public class Q1 extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private static String ID1;
	private static String ID2;
	private static String ID3;
	private static String TEAMID;
	private static String PUBLIC_KEY;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public Q1() {
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
		PUBLIC_KEY = config.getServletContext().getInitParameter("PUBLIC_KEY");
	}

	/**
	 * @see Servlet#getServletConfig()
	 */
	public ServletConfig getServletConfig() {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException, IOException {
		// TODO Auto-generated method stub
		try {
			BigInteger nominator = new BigInteger(request.getParameter("key"));
			BigInteger denominator = new BigInteger(PUBLIC_KEY);

			String quotient = nominator.divide(denominator).toString();

			java.util.Date date = new java.util.Date();
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			String res = quotient + "\n" + TEAMID + "," + ID1 + "," + ID2 + ","
					+ ID3 + "\n" + sdf.format(date) + "\n";
			response.setContentType("text/html");
			PrintWriter out = response.getWriter();
			out.print(res);
		} catch (Exception e) {
			System.out.println("Request parameter is not a valid big integer!");
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
