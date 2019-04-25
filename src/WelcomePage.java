public class WelcomePage {
	
	private static final String version = "v0.1.0";
	private static final String copyright = "@2019 Arpitha Manindra MingxioYe Nileshwari Shivani";

	public static void splashScreen() {
		System.out.println(Utility.repeat("-", 80));
		System.out.println("Welcome to MyDatabase"); // Display the string.
		System.out.println("MyDatabase Version " + version);
		System.out.println(copyright);
		System.out
				.println("\nType \"help;\" to display the supported commands of MyDatabase.");
		System.out.println(Utility.repeat("-", 80));
	}

	public static void displayVersion() {
		System.out.println("MyDatabase Version " + version);
		System.out.println(copyright);
	}
}
