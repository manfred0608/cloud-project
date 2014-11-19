package edu.cmu.cs.cloudcomputing.zjers.frontend.undertow;

import edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.handler.AuthHandler;
import edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.handler.HashtagHandler;
import edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.handler.HotHandler;
import edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.handler.RetweetHandler;
import edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.handler.ShutterHandler;
import edu.cmu.cs.cloudcomputing.zjers.frontend.undertow.handler.TweetHandler;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.util.Headers;

public final class QueryServer {

	public static void main(final String[] args) throws Exception {
		
		if (args.length < 2) {
			System.err.println("Insufficient parameters.");
			usage();
			System.exit(1);
		}
		
		new QueryServer(args);
    }
	
	private static void usage() {
		System.err.println("Usage: <port> <ip-or-hostname-to-bind>");
	}
	
	public QueryServer(final String[] args) throws Exception {
		
		Undertow.builder()
			.addHttpListener(Integer.parseInt(args[0]), args[1])
			.setBufferSize(1024 * 16)
			.setIoThreads(Runtime.getRuntime().availableProcessors() * 16)
//			.setSocketOption(Options.BACKLOG, 10000)
			.setHandler(Handlers.header(Handlers.path()
					.addPrefixPath("/q1", new AuthHandler())
					.addPrefixPath("/q2", new TweetHandler())
					.addPrefixPath("/q3", new RetweetHandler())
					.addPrefixPath("/q4", new HashtagHandler())
					.addPrefixPath("/q5", new HotHandler())
					.addPrefixPath("/q6", new ShutterHandler()),
					Headers.SERVER_STRING, "ZJers"))
			.setWorkerThreads(200)
			.build()
			.start();
	}
}
