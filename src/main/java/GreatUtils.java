public class GreatUtils {
    public static String[] splitByWhitespace(String line) {
        return line.split("\\s+");
    }

    public static Double parseBorder(String line) throws NumberFormatException {
        return Double.parseDouble(splitByWhitespace(line)[1]);
    }
}
