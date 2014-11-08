import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;


public class Q4 extends HttpServlet {
	private static final long serialVersionUID = 1L;

	private static String ID1;
	private static String ID2;
	private static String ID3;
	private static String TEAMID;
	
	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public Q4() {
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
			Q4Query.createConnection();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public String[] getEMRRes(String date, String location, int start, int end) {
		return Q4Query.q4Query(date, location, start, end);
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

		String date = request.getParameter("date");
		String location = request.getParameter("location");
		
		int start = Integer.valueOf(request.getParameter("m"));
		int end = Integer.valueOf(request.getParameter("n"));

		Q4Query.getConn();
		String[] results = null;
		StringBuffer sb = new StringBuffer();
		try {
			results = getEMRRes(date, location, start, end);
			if (results == null || results.length == 0)
				return;
			
			String title = TEAMID + "," + ID1 + "," + ID2 + "," + ID3 + "\n";
			sb.append(title);
			
			for(int i = 0; i < results.length; i++){
				sb.append(results[i] + "\n");
			}
			
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
			Q4Query.closeConnection();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
