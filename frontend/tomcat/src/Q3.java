import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class Q3 extends HttpServlet {
	private static final long serialVersionUID = 1L;

	private static String ID1;
	private static String ID2;
	private static String ID3;
	private static String TEAMID;
	
	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public Q3() {
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
		
		try {
			Query.createConnection();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String getEMRRes(String userid) {
		return Query.q3Query(userid);
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

		Query.getConn();
		String result = null;
		StringBuffer sb = new StringBuffer();
		try {
			result = getEMRRes(userid);
			if (result == null)
				return;
			
			String title = TEAMID + "," + ID1 + "," + ID2 + "," + ID3 + "\n";
			sb.append(title);
			
			sb.append(result);

			response.setCharacterEncoding("UTF-8");
			response.setContentType("text/html");
			PrintWriter out = response.getWriter();
			out.print(sb.toString());
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void destroy() {
		super.destroy();
		try {
			Query.closeConnection();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
