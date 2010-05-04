import sdloader.SDLoader;
import sdloader.javaee.WebAppContext;
import sdloader.util.Browser;

public class Bootstrap {

	public static final String CONTEXT_PATH = "/cassandra-tools";

	public static void main(String[] args) {
		SDLoader loader = new SDLoader();
		WebAppContext webAppContext = new WebAppContext(CONTEXT_PATH, "webapp");
		loader.addWebAppContext(webAppContext);
		loader.setPort(8090);

		loader.start();

		Browser.open("http://localhost:" + loader.getPort() + CONTEXT_PATH
				+ "/show/token");
	}
}
