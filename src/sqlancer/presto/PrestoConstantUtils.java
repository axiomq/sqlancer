package sqlancer.presto;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class PrestoConstantUtils {

    public static String removeNoneAscii(String str) {
        return str.replaceAll("[^\\x00-\\x7F]", "");
    }

    public static String removeNonePrintable(String str) { // All Control Char
        return str.replaceAll("[\\p{C}]", "");
    }

    public static String removeOthersControlChar(String str) { // Some Control Char
        return str.replaceAll("[\\p{Cntrl}\\p{Cc}\\p{Cf}\\p{Co}\\p{Cn}]", "");
    }

    public static String removeAllControlChars(String str) {
        return removeOthersControlChar(removeNonePrintable(str)).replaceAll("[\\r\\n\\t]", "");
    }

    public static BigDecimal getDecimal(double val, int scale, int precision) {
        int part = precision - scale;

        // long part
        long lng = (long) val;
        // decimal places
        double d1 = val - lng;

        String x_str = Long.toString(lng);
        long new_x = Long.parseLong(x_str.substring(x_str.length() - part));

        double finalD = new_x + d1;
        BigDecimal finalBD = new BigDecimal(finalD).setScale(scale, RoundingMode.CEILING);
        return finalBD;
    }

}
