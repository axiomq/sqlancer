package sqlancer.presto.ast;

import sqlancer.Randomly;
import sqlancer.common.ast.newast.Node;
import sqlancer.presto.PrestoSchema;

import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;

public abstract class PrestoConstant implements Node<PrestoExpression>, PrestoExpression {

    private static final String[] timeZones = {"Africa/Abidjan", "Africa/Accra", "Africa/Addis_Ababa", "Africa/Algiers", "Africa/Asmara", "Africa/Asmera", "Africa/Bamako", "Africa/Bangui", "Africa/Banjul", "Africa/Bissau", "Africa/Blantyre", "Africa/Brazzaville", "Africa/Bujumbura", "Africa/Cairo", "Africa/Casablanca", "Africa/Ceuta", "Africa/Conakry", "Africa/Dakar", "Africa/Dar_es_Salaam", "Africa/Djibouti", "Africa/Douala", "Africa/El_Aaiun", "Africa/Freetown", "Africa/Gaborone", "Africa/Harare", "Africa/Johannesburg", "Africa/Juba", "Africa/Kampala", "Africa/Khartoum", "Africa/Kigali", "Africa/Kinshasa", "Africa/Lagos", "Africa/Libreville", "Africa/Lome", "Africa/Luanda", "Africa/Lubumbashi", "Africa/Lusaka", "Africa/Malabo", "Africa/Maputo", "Africa/Maseru", "Africa/Mbabane", "Africa/Mogadishu", "Africa/Monrovia", "Africa/Nairobi", "Africa/Ndjamena", "Africa/Niamey", "Africa/Nouakchott", "Africa/Ouagadougou", "Africa/Porto-Novo", "Africa/Sao_Tome", "Africa/Timbuktu", "Africa/Tripoli", "Africa/Tunis", "Africa/Windhoek",
            "America/Adak", "America/Anchorage", "America/Anguilla", "America/Antigua", "America/Araguaina", "America/Argentina/Buenos_Aires", "America/Argentina/Catamarca", "America/Argentina/ComodRivadavia", "America/Argentina/Cordoba", "America/Argentina/Jujuy", "America/Argentina/La_Rioja", "America/Argentina/Mendoza", "America/Argentina/Rio_Gallegos", "America/Argentina/Salta", "America/Argentina/San_Juan", "America/Argentina/San_Luis", "America/Argentina/Tucuman", "America/Argentina/Ushuaia", "America/Aruba", "America/Asuncion", "America/Atikokan", "America/Atka", "America/Bahia", "America/Bahia_Banderas", "America/Barbados", "America/Belem", "America/Belize", "America/Blanc-Sablon", "America/Boa_Vista", "America/Bogota", "America/Boise", "America/Buenos_Aires", "America/Cambridge_Bay", "America/Campo_Grande", "America/Cancun", "America/Caracas", "America/Catamarca", "America/Cayenne", "America/Cayman", "America/Chicago", "America/Chihuahua", "America/Coral_Harbour", "America/Cordoba", "America/Costa_Rica", "America/Creston", "America/Cuiaba", "America/Curacao", "America/Danmarkshavn", "America/Dawson", "America/Dawson_Creek", "America/Denver", "America/Detroit", "America/Dominica", "America/Edmonton", "America/Eirunepe", "America/El_Salvador", "America/Ensenada", "America/Fort_Nelson", "America/Fort_Wayne", "America/Fortaleza", "America/Glace_Bay", "America/Godthab", "America/Goose_Bay", "America/Grand_Turk", "America/Grenada", "America/Guadeloupe", "America/Guatemala", "America/Guayaquil", "America/Guyana", "America/Halifax", "America/Havana", "America/Indiana/Indianapolis", "America/Indiana/Knox", "America/Indiana/Marengo", "America/Indiana/Petersburg", "America/Indiana/Tell_City", "America/Indiana/Vevay", "America/Indiana/Vincennes", "America/Indiana/Winamac", "America/Indianapolis", "America/Inuvik", "America/Iqaluit", "America/Jamaica", "America/Jujuy", "America/Juneau", "America/Kentucky/Louisville", "America/Kentucky/Monticello", "America/Knox_IN", "America/Kralendijk", "America/La_Paz", "America/Lima", "America/Los_Angeles", "America/Louisville", "America/Lower_Princes", "America/Maceio", "America/Managua", "America/Manaus", "America/Marigot", "America/Martinique", "America/Matamoros", "America/Mazatlan", "America/Mendoza", "America/Menominee", "America/Merida", "America/Metlakatla", "America/Mexico_City", "America/Miquelon", "America/Moncton", "America/Monterrey", "America/Montevideo", "America/Montreal", "America/Montserrat", "America/Nassau", "America/New_York", "America/Nipigon", "America/Nome", "America/Noronha", "America/North_Dakota/Beulah", "America/North_Dakota/Center", "America/North_Dakota/New_Salem", "America/Ojinaga", "America/Panama", "America/Pangnirtung", "America/Paramaribo", "America/Phoenix", "America/Port-au-Prince", "America/Port_of_Spain", "America/Porto_Acre", "America/Porto_Velho", "America/Puerto_Rico", "America/Punta_Arenas", "America/Rainy_River", "America/Rankin_Inlet", "America/Recife", "America/Regina", "America/Resolute", "America/Rio_Branco", "America/Rosario", "America/Santa_Isabel", "America/Santarem", "America/Santiago", "America/Santo_Domingo", "America/Sao_Paulo", "America/Scoresbysund", "America/Shiprock", "America/Sitka", "America/St_Barthelemy", "America/St_Johns", "America/St_Kitts", "America/St_Lucia", "America/St_Thomas", "America/St_Vincent", "America/Swift_Current", "America/Tegucigalpa", "America/Thule", "America/Thunder_Bay", "America/Tijuana", "America/Toronto", "America/Tortola", "America/Vancouver", "America/Virgin", "America/Whitehorse", "America/Winnipeg", "America/Yakutat", "America/Yellowknife",
            "Antarctica/Casey", "Antarctica/Davis", "Antarctica/DumontDUrville", "Antarctica/Macquarie", "Antarctica/Mawson", "Antarctica/McMurdo", "Antarctica/Palmer", "Antarctica/Rothera", "Antarctica/South_Pole", "Antarctica/Syowa", "Antarctica/Troll", "Antarctica/Vostok", "Arctic/Longyearbyen",
            "Asia/Aden", "Asia/Almaty", "Asia/Amman", "Asia/Anadyr", "Asia/Aqtau", "Asia/Aqtobe", "Asia/Ashgabat", "Asia/Ashkhabad", "Asia/Atyrau", "Asia/Baghdad", "Asia/Bahrain", "Asia/Baku", "Asia/Bangkok", "Asia/Barnaul", "Asia/Beirut", "Asia/Bishkek", "Asia/Brunei", "Asia/Calcutta", "Asia/Chita", "Asia/Choibalsan", "Asia/Chongqing", "Asia/Chungking", "Asia/Colombo", "Asia/Dacca", "Asia/Damascus", "Asia/Dhaka", "Asia/Dili", "Asia/Dubai", "Asia/Dushanbe", "Asia/Gaza", "Asia/Harbin", "Asia/Hebron", "Asia/Ho_Chi_Minh", "Asia/Hong_Kong", "Asia/Hovd", "Asia/Irkutsk", "Asia/Istanbul", "Asia/Jakarta", "Asia/Jayapura", "Asia/Jerusalem", "Asia/Kabul", "Asia/Kamchatka", "Asia/Karachi", "Asia/Kashgar", "Asia/Kathmandu", "Asia/Katmandu", "Asia/Khandyga", "Asia/Kolkata", "Asia/Krasnoyarsk", "Asia/Kuala_Lumpur", "Asia/Kuching", "Asia/Kuwait", "Asia/Macao", "Asia/Macau", "Asia/Magadan", "Asia/Makassar", "Asia/Manila", "Asia/Muscat", "Asia/Nicosia", "Asia/Novokuznetsk", "Asia/Novosibirsk", "Asia/Omsk", "Asia/Oral", "Asia/Phnom_Penh", "Asia/Pontianak", "Asia/Pyongyang", "Asia/Qatar", "Asia/Qyzylorda", "Asia/Rangoon", "Asia/Riyadh", "Asia/Saigon", "Asia/Sakhalin", "Asia/Samarkand", "Asia/Seoul", "Asia/Shanghai", "Asia/Singapore", "Asia/Srednekolymsk", "Asia/Taipei", "Asia/Tashkent", "Asia/Tbilisi", "Asia/Tehran", "Asia/Tel_Aviv", "Asia/Thimbu", "Asia/Thimphu", "Asia/Tokyo", "Asia/Tomsk", "Asia/Ujung_Pandang", "Asia/Ulaanbaatar", "Asia/Ulan_Bator", "Asia/Urumqi", "Asia/Ust-Nera", "Asia/Vientiane", "Asia/Vladivostok", "Asia/Yakutsk", "Asia/Yangon", "Asia/Yekaterinburg", "Asia/Yerevan",
            "Atlantic/Azores", "Atlantic/Bermuda", "Atlantic/Canary", "Atlantic/Cape_Verde", "Atlantic/Faeroe", "Atlantic/Faroe", "Atlantic/Jan_Mayen", "Atlantic/Madeira", "Atlantic/Reykjavik", "Atlantic/South_Georgia", "Atlantic/St_Helena", "Atlantic/Stanley",
            "Australia/ACT", "Australia/Adelaide", "Australia/Brisbane", "Australia/Broken_Hill", "Australia/Canberra", "Australia/Currie", "Australia/Darwin", "Australia/Eucla", "Australia/Hobart", "Australia/LHI", "Australia/Lindeman", "Australia/Lord_Howe", "Australia/Melbourne", "Australia/NSW", "Australia/North", "Australia/Perth", "Australia/Queensland", "Australia/South", "Australia/Sydney", "Australia/Tasmania", "Australia/Victoria", "Australia/West", "Australia/Yancowinna", "Brazil/Acre", "Brazil/DeNoronha", "Brazil/East", "Brazil/West", "CET",
            "Canada/Atlantic", "Canada/Central", "Canada/Eastern", "Canada/Mountain", "Canada/Newfoundland", "Canada/Pacific", "Canada/Saskatchewan", "Canada/Yukon",
            "Europe/Amsterdam", "Europe/Andorra", "Europe/Astrakhan", "Europe/Athens", "Europe/Belfast", "Europe/Belgrade", "Europe/Berlin", "Europe/Bratislava", "Europe/Brussels", "Europe/Bucharest", "Europe/Budapest", "Europe/Busingen", "Europe/Chisinau", "Europe/Copenhagen", "Europe/Dublin", "Europe/Gibraltar", "Europe/Guernsey", "Europe/Helsinki", "Europe/Isle_of_Man", "Europe/Istanbul", "Europe/Jersey", "Europe/Kaliningrad", "Europe/Kiev", "Europe/Kirov", "Europe/Lisbon", "Europe/Ljubljana", "Europe/London", "Europe/Luxembourg", "Europe/Madrid", "Europe/Malta", "Europe/Mariehamn", "Europe/Minsk", "Europe/Monaco", "Europe/Moscow", "Europe/Nicosia", "Europe/Oslo", "Europe/Paris", "Europe/Podgorica", "Europe/Prague", "Europe/Riga", "Europe/Rome", "Europe/Samara", "Europe/San_Marino", "Europe/Sarajevo", "Europe/Simferopol", "Europe/Skopje", "Europe/Sofia", "Europe/Stockholm", "Europe/Tallinn", "Europe/Tirane", "Europe/Tiraspol", "Europe/Ulyanovsk", "Europe/Uzhgorod", "Europe/Vaduz", "Europe/Vatican", "Europe/Vienna", "Europe/Vilnius", "Europe/Volgograd", "Europe/Warsaw", "Europe/Zagreb", "Europe/Zaporozhye", "Europe/Zurich"};

    private PrestoConstant() {
    }


    public boolean isNull() {
        return false;
    }

    public boolean isInt() {
        return false;
    }

    public boolean isBoolean() {
        return false;
    }

    public boolean isString() {
        return false;
    }

    public boolean isFloat() {
        return false;
    }

    public boolean asBoolean() {
        throw new UnsupportedOperationException(this.toString());
    }

    public long asInt() {
        throw new UnsupportedOperationException(this.toString());
    }

    public String asString() {
        throw new UnsupportedOperationException(this.toString());
    }

    public double asFloat() {
        throw new UnsupportedOperationException(this.toString());
    }

    public abstract Node<PrestoExpression> cast(PrestoSchema.PrestoDataType dataType);

    public abstract Node<PrestoExpression> isEquals(PrestoConstant rightVal);

    public abstract Node<PrestoExpression> isLessThan(PrestoConstant rightVal);


    public static class PrestoNullConstant extends PrestoConstant {

        @Override
        public String toString() {
            return "NULL";
        }


        @Override
        public boolean isNull() {
            return true;
        }

        @Override
        public Node<PrestoExpression> cast(PrestoSchema.PrestoDataType dataType) {
            return PrestoConstant.createNullConstant();
        }

        @Override
        public Node<PrestoExpression> isEquals(PrestoConstant rightVal) {
            return PrestoConstant.createNullConstant();
        }

        @Override
        public Node<PrestoExpression> isLessThan(PrestoConstant rightVal) {
            return PrestoConstant.createNullConstant();
        }

        @Override
        public PrestoSchema.PrestoDataType getExpectedType() {
            return PrestoSchema.PrestoDataType.NULL;
        }


    }

    public static class PrestoIntConstant extends PrestoConstant {

        private final long value;

        public PrestoIntConstant(long value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }

        public long getValue() {
            return value;
        }

        @Override
        public boolean isInt() {
            return true;
        }

        @Override
        public Node<PrestoExpression> cast(PrestoSchema.PrestoDataType dataType) {
            switch (dataType) {
                case BOOLEAN:
                    return new PrestoBooleanConstant(value != 0);
                case INT:
                    return this;
                case VARCHAR:
                    return new PrestoTextConstant(String.valueOf(value));
                default:
                    return null;
            }
        }

        @Override
        public Node<PrestoExpression> isEquals(PrestoConstant rightVal) {
            if (rightVal.isNull()) {
                return PrestoConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                return PrestoConstant.createBooleanConstant(value == rightVal.asInt());
            } else {
                throw new AssertionError(rightVal);
            }

        }

        @Override
        public Node<PrestoExpression> isLessThan(PrestoConstant rightVal) {
            if (rightVal.isNull()) {
                return PrestoConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                return PrestoConstant.createBooleanConstant(value < rightVal.asInt());
            } else if (rightVal.isFloat()) {
                return PrestoConstant.createBooleanConstant(value < rightVal.asFloat());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public PrestoSchema.PrestoDataType getExpectedType() {
            return PrestoSchema.PrestoDataType.INT;
        }


    }

    public static class PrestoFloatConstant extends PrestoConstant {

        private final double value;

        public PrestoFloatConstant(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            if (value == Double.POSITIVE_INFINITY) {
                return "'+Inf'";
            } else if (value == Double.NEGATIVE_INFINITY) {
                return "'-Inf'";
            }
            return String.valueOf(value);
        }


        @Override
        public boolean isFloat() {
            return true;
        }

        @Override
        public Node<PrestoExpression> cast(PrestoSchema.PrestoDataType dataType) {
            switch (dataType) {
                case FLOAT:
                    return this;
                case DECIMAL:
                    return createDecimalConstant(value);
                case INT:
                    return PrestoConstant.createIntConstant((long) value);
                case BOOLEAN:
                    return PrestoConstant.createBooleanConstant(value != 0);
                case VARCHAR:
                    return PrestoConstant.createStringConstant(String.valueOf(value));
                default:
                    return null;
            }
        }

        @Override
        public double asFloat() {
            return value;
        }

        @Override
        public Node<PrestoExpression> isEquals(PrestoConstant rightVal) {
            return null;
        }

        @Override
        public Node<PrestoExpression> isLessThan(PrestoConstant rightVal) {
            if (rightVal.isNull()) {
                return PrestoConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                return PrestoConstant.createBooleanConstant(value < rightVal.asInt());
            } else if (rightVal.isFloat()) {
                return PrestoConstant.createBooleanConstant(value < rightVal.asFloat());
            } else {
                throw new AssertionError(rightVal);
            }
        }

    }

    public static class PrestoDecimalConstant extends PrestoConstant {

        private static final DecimalFormat df = new DecimalFormat("###0.0000");

        private final double value;

        public PrestoDecimalConstant(double value) {
            this.value = value;
        }

        public double getValue() {
            return value;
        }

        @Override
        public String toString() {
            if (value == Double.POSITIVE_INFINITY) {
                return "'+Inf'";
            } else if (value == Double.NEGATIVE_INFINITY) {
                return "'-Inf'";
            }
            return df.format(value);
        }

        @Override
        public Node<PrestoExpression> cast(PrestoSchema.PrestoDataType dataType) {
            switch (dataType) {
                case FLOAT:
                    return createFloatConstant(value);
                case DECIMAL:
                    return this;
                case INT:
                    return createIntConstant((long) value);
                case BOOLEAN:
                    return createBooleanConstant(value != 0);
                case VARCHAR:
                    return createStringConstant(String.valueOf(value));
                default:
                    return null;
            }
        }

        @Override
        public double asFloat() {
            return value;
        }

        @Override
        public Node<PrestoExpression> isEquals(PrestoConstant rightVal) {
            return null;
        }

        @Override
        public Node<PrestoExpression> isLessThan(PrestoConstant rightVal) {
            if (rightVal.isNull()) {
                return PrestoConstant.createNullConstant();
            } else if (rightVal.isInt()) {
                return PrestoConstant.createBooleanConstant(value < rightVal.asInt());
            } else if (rightVal.isFloat()) {
                return PrestoConstant.createBooleanConstant(value < rightVal.asFloat());
            } else {
                throw new AssertionError(rightVal);
            }
        }

    }

    public static class PrestoTextConstant extends PrestoConstant {

        private final String value;

        public PrestoTextConstant(String value) {
            this.value = value;
        }

        public PrestoTextConstant(String value, int size) {
            this.value = value.substring(0, Math.min(value.length(), size));
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "'" + value.replace("'", "''") + "'";
        }

        @Override
        public Node<PrestoExpression> cast(PrestoSchema.PrestoDataType dataType) {
            switch (dataType) {
                case VARCHAR:
                    return this;
                case INT:
                    try {
                        return new PrestoIntConstant(Long.parseLong(value));
                    } catch (NumberFormatException e) {
                        return new PrestoIntConstant(-1);
                    }
                case BOOLEAN:
                    if ("false".contentEquals(value.toLowerCase())) {
                        return new PrestoBooleanConstant(false);
                    } else if ("true".contentEquals(value.toLowerCase())) {
                        return new PrestoBooleanConstant(true);
                    } else {
                        throw new AssertionError(String.format("string: %s, cannot be forced to boolean", value));
                    }
                case FLOAT:
                    try {
                        return new PrestoFloatConstant(Double.parseDouble(value));
                    } catch (NumberFormatException e) {
                        return new PrestoFloatConstant(-1);
                    }
                default:
                    return null;
            }
        }

        @Override
        public Node<PrestoExpression> isEquals(PrestoConstant rightVal) {
            if (rightVal.isNull()) {
                return createNullConstant();
            } else if (rightVal.isString()) {
                return createBooleanConstant(value.contentEquals(rightVal.asString()));
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public Node<PrestoExpression> isLessThan(PrestoConstant rightVal) {
            if (rightVal.isNull()) {
                return createNullConstant();
            } else if (rightVal.isString()) {
                return createBooleanConstant(value.compareTo(rightVal.asString()) < 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class PrestoVarbinaryConstant extends PrestoConstant {

        private final String value;

        public PrestoVarbinaryConstant(String value) {
            this.value = value.replace("'", "");
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.format("CAST ('%s' AS VARBINARY)", value);
        }

        @Override
        public Node<PrestoExpression> cast(PrestoSchema.PrestoDataType dataType) {
            switch (dataType) {
                case VARCHAR:
                    return createStringConstant(value);
                case INT:
                    try {
                        return new PrestoIntConstant(Long.parseLong(value));
                    } catch (NumberFormatException e) {
                        return new PrestoConstant.PrestoIntConstant(-1);
                    }
                case BOOLEAN:
                    if ("false".contentEquals(value.toLowerCase())) {
                        return new PrestoConstant.PrestoBooleanConstant(false);
                    } else if ("true".contentEquals(value.toLowerCase())) {
                        return new PrestoConstant.PrestoBooleanConstant(true);
                    } else {
                        throw new AssertionError(String.format("string: %s, cannot be forced to boolean", value));
                    }
                case FLOAT:
                    try {
                        return new PrestoConstant.PrestoFloatConstant(Double.parseDouble(value));
                    } catch (NumberFormatException e) {
                        return new PrestoConstant.PrestoFloatConstant(-1);
                    }
                default:
                    return null;
            }
        }

        @Override
        public Node<PrestoExpression> isEquals(PrestoConstant rightVal) {
            if (rightVal.isNull()) {
                return createNullConstant();
            } else if (rightVal.isString()) {
                return createBooleanConstant(value.contentEquals(rightVal.asString()));
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public Node<PrestoExpression> isLessThan(PrestoConstant rightVal) {
            if (rightVal.isNull()) {
                return createNullConstant();
            } else if (rightVal.isString()) {
                return createBooleanConstant(value.compareTo(rightVal.asString()) < 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class PrestoJsonConstant extends PrestoConstant {

        private enum JSON_VALUE_TYPE {
            OBJECT, ARRAY, NUMBER, STRING, TRUE, FALSE, NULL
        }

        private final String value;
        private final JSON_VALUE_TYPE jvt;
        private final String val;

        public PrestoJsonConstant() {
            Randomly rand = new Randomly();
            JSON_VALUE_TYPE jvt = Randomly.fromOptions(JSON_VALUE_TYPE.values());
            String val = null;
            switch (jvt) {
                case NULL:
                    val = "null";
                    value = "{\"val\":" + val + "}";
                    break;
                case FALSE:
                    val = "false";
                    value = "{\"val\":" + val + "}";
                    break;
                case TRUE:
                    val = "true";
                    value = "{\"val\":" + val + "}";
                    break;
                case STRING:
                    String randString = rand.getString();
                    String string = randString.substring(0, Math.min(randString.length(), 250));
                    string = string.replace("'", "");
                    val = string;
                    value = "{\"val\": \"" + string + "\"}";
                    break;
                case NUMBER:
                    if (Randomly.getBoolean()) {
                        int no = (int) rand.getInteger();
                        val = String.valueOf(no);
                    } else {
                        double no = rand.getDouble();
                        val = String.valueOf(no);
                    }
                    value = "{\"val\": " + val + "}";
                    break;
                case ARRAY:
                    value = "{\"employees\":[\"John\", \"Anna\", \"Peter\"]}";
                    break;
                case OBJECT:
                    value = "{\"employee\":{\"name\":\"John\", \"age\":30, \"city\":\"New York\"}}";
                    break;
                default:
                    value = "{}";
            }
            this.jvt = jvt;
            this.val = val;
        }


        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "'" + value + "'";
        }

        @Override
        public Node<PrestoExpression> cast(PrestoSchema.PrestoDataType dataType) {
            switch (dataType) {
                case VARCHAR:
                    return createStringConstant(value);
                case INT:
                    if (jvt == JSON_VALUE_TYPE.NUMBER) {
                        try {
                            return new PrestoIntConstant(Long.parseLong(val));
                        } catch (NumberFormatException e) {
                            return new PrestoIntConstant(-1);
                        }
                    } else {
                        return null;
                    }
                case BOOLEAN:
                    if ("false".contentEquals(val.toLowerCase())) {
                        return new PrestoConstant.PrestoBooleanConstant(false);
                    } else if ("true".contentEquals(val.toLowerCase())) {
                        return new PrestoConstant.PrestoBooleanConstant(true);
                    } else {
                        throw new AssertionError(String.format("string: %s, cannot be forced to boolean", val));
                    }
                case FLOAT:
                    try {
                        return new PrestoConstant.PrestoFloatConstant(Double.parseDouble(val));
                    } catch (NumberFormatException e) {
                        return new PrestoConstant.PrestoFloatConstant(-1);
                    }
                default:
                    return null;
            }
        }

        @Override
        public Node<PrestoExpression> isEquals(PrestoConstant rightVal) {
            if (rightVal.isNull()) {
                return createNullConstant();
            } else if (rightVal.isString()) {
                return createBooleanConstant(value.contentEquals(rightVal.asString()));
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public Node<PrestoExpression> isLessThan(PrestoConstant rightVal) {
            if (rightVal.isNull()) {
                return createNullConstant();
            } else if (rightVal.isString()) {
                return createBooleanConstant(value.compareTo(rightVal.asString()) < 0);
            } else {
                throw new AssertionError(rightVal);
            }
        }

    }

    public static class PrestoBitConstant extends PrestoConstant {

        private final String value;

        public PrestoBitConstant(long value) {
            this.value = Long.toBinaryString(value);
        }

        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return "B'" + value + "'";
        }

        @Override
        public Node<PrestoExpression> cast(PrestoSchema.PrestoDataType dataType) {
            return null;
        }

        @Override
        public Node<PrestoExpression> isEquals(PrestoConstant rightVal) {
            return null;
        }

        @Override
        public Node<PrestoExpression> isLessThan(PrestoConstant rightVal) {
            return null;
        }
    }

    public static class PrestoDateConstant extends PrestoConstant {

        private final long value;
        public String textRepresentation;

        public PrestoDateConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            textRepresentation = dateFormat.format(timestamp);
            this.value = val;
        }

        public String getValue() {
            return textRepresentation;
        }

        @Override
        public String toString() {
            return String.format("DATE '%s'", textRepresentation);
        }

        @Override
        public Node<PrestoExpression> cast(PrestoSchema.PrestoDataType dataType) {
            return null;
        }

        @Override
        public Node<PrestoExpression> isEquals(PrestoConstant rightVal) {
            if (rightVal.isNull()) {
                return createNullConstant();
            } else if (rightVal.isInt()) {
                return createBooleanConstant(value == rightVal.asInt());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public Node<PrestoExpression> isLessThan(PrestoConstant rightVal) {
            if (rightVal.isNull()) {
                return createNullConstant();
            } else if (rightVal.isInt()) {
                return createBooleanConstant(value < rightVal.asInt());
            } else if (rightVal.isFloat()) {
                return createBooleanConstant(value < rightVal.asFloat());
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class PrestoTimeConstant extends PrestoConstant {

        private final long value;
        public String textRepresentation;

        public PrestoTimeConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
            textRepresentation = dateFormat.format(timestamp);
            this.value = val;
        }

        public String getValue() {
            return textRepresentation;
        }

        @Override
        public String toString() {
            return String.format("TIME '%s'", textRepresentation);
        }


        @Override
        public Node<PrestoExpression> cast(PrestoSchema.PrestoDataType dataType) {
            return null;
        }

        @Override
        public Node<PrestoExpression> isEquals(PrestoConstant rightVal) {
            if (rightVal.isNull()) {
                return createNullConstant();
            } else if (rightVal.isInt()) {
                return createBooleanConstant(value == rightVal.asInt());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public Node<PrestoExpression> isLessThan(PrestoConstant rightVal) {
            if (rightVal.isNull()) {
                return createNullConstant();
            } else if (rightVal.isInt()) {
                return createBooleanConstant(value < rightVal.asInt());
            } else if (rightVal.isFloat()) {
                return createBooleanConstant(value < rightVal.asFloat());
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class PrestoTimeWithTimeZoneConstant extends PrestoConstant {

        private final long value;
        public String textRepresentation;
        public String timeZone;

        public PrestoTimeWithTimeZoneConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSS");
            textRepresentation = dateFormat.format(timestamp);
            this.timeZone = Randomly.fromOptions(timeZones);
            this.value = val;
        }

        public String getValue() {
            return textRepresentation;
        }

        @Override
        public String toString() {
            return String.format("TIME '%s %s'", textRepresentation, timeZone);
        }


        @Override
        public Node<PrestoExpression> cast(PrestoSchema.PrestoDataType dataType) {
            return null;
        }

        @Override
        public Node<PrestoExpression> isEquals(PrestoConstant rightVal) {
            if (rightVal.isNull()) {
                return createNullConstant();
            } else if (rightVal.isInt()) {
                return createBooleanConstant(value == rightVal.asInt());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public Node<PrestoExpression> isLessThan(PrestoConstant rightVal) {
            if (rightVal.isNull()) {
                return createNullConstant();
            } else if (rightVal.isInt()) {
                return createBooleanConstant(value < rightVal.asInt());
            } else if (rightVal.isFloat()) {
                return createBooleanConstant(value < rightVal.asFloat());
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class PrestoTimestampConstant extends PrestoConstant {

        private final long value;
        public String textRepr;

        public PrestoTimestampConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            textRepr = dateFormat.format(timestamp);
            this.value = val;
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("TIMESTAMP '%s'", textRepr);
        }


        @Override
        public Node<PrestoExpression> cast(PrestoSchema.PrestoDataType dataType) {
            return null;
        }

        @Override
        public Node<PrestoExpression> isEquals(PrestoConstant rightVal) {
            if (rightVal.isNull()) {
                return createNullConstant();
            } else if (rightVal.isInt()) {
                return createBooleanConstant(value == rightVal.asInt());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public Node<PrestoExpression> isLessThan(PrestoConstant rightVal) {
            if (rightVal.isNull()) {
                return createNullConstant();
            } else if (rightVal.isInt()) {
                return createBooleanConstant(value < rightVal.asInt());
            } else if (rightVal.isFloat()) {
                return createBooleanConstant(value < rightVal.asFloat());
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class PrestoTimestampWithTimezoneConstant extends PrestoConstant {

        private final long value;
        public String textRepr;
        public String timeZone;

        public PrestoTimestampWithTimezoneConstant(long val) {
            Timestamp timestamp = new Timestamp(val);
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            textRepr = dateFormat.format(timestamp);
            this.timeZone = Randomly.fromOptions(timeZones);
            this.value = val;
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            return String.format("TIMESTAMP '%s %s'", textRepr, timeZone);
        }


        @Override
        public Node<PrestoExpression> cast(PrestoSchema.PrestoDataType dataType) {
            return null;
        }

        @Override
        public Node<PrestoExpression> isEquals(PrestoConstant rightVal) {
            if (rightVal.isNull()) {
                return createNullConstant();
            } else if (rightVal.isInt()) {
                return createBooleanConstant(value == rightVal.asInt());
            } else {
                throw new AssertionError(rightVal);
            }
        }

        @Override
        public Node<PrestoExpression> isLessThan(PrestoConstant rightVal) {
            if (rightVal.isNull()) {
                return createNullConstant();
            } else if (rightVal.isInt()) {
                return createBooleanConstant(value < rightVal.asInt());
            } else if (rightVal.isFloat()) {
                return createBooleanConstant(value < rightVal.asFloat());
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static class PrestoIntervalDayToSecondConstant extends PrestoConstant {

        private enum Interval {
            DAY, HOUR, MINUTE, SECOND;
        }

        public String textRepr;
        public Interval fromInterval;
        public Interval toInterval;

        public PrestoIntervalDayToSecondConstant() {
            fromInterval = Randomly.fromOptions(Interval.values());
            SimpleDateFormat dateFormat;
            dateFormat = new SimpleDateFormat("dd HH:mm:ss");
            switch (fromInterval) {
                case DAY:
                    toInterval = Randomly.fromOptions(Interval.HOUR, Interval.MINUTE, Interval.SECOND, null);
                    if (toInterval == null)
                        break;
                    switch (toInterval) {
                        case HOUR:
                            dateFormat = new SimpleDateFormat("dd HH");
                            break;
                        case MINUTE:
                            dateFormat = new SimpleDateFormat("dd HH:mm");
                            break;
                        case SECOND:
                            dateFormat = new SimpleDateFormat("dd HH:mm:ss");
                            break;
                        default:
                            dateFormat = new SimpleDateFormat("dd HH:mm:ss");
                            break;
                    }
                    break;
                case HOUR:
                    toInterval = Randomly.fromOptions(Interval.MINUTE, Interval.SECOND, null);
                    if (toInterval == null)
                        break;
                    switch (toInterval) {
                        case MINUTE:
                            dateFormat = new SimpleDateFormat("HH:mm");
                            break;
                        case SECOND:
                            dateFormat = new SimpleDateFormat("HH:mm:ss");
                            break;
                        default:
                            dateFormat = new SimpleDateFormat("HH:mm:ss");
                            break;
                    }
                    break;
                case MINUTE:
                    toInterval = Randomly.fromOptions(Interval.SECOND, null);
                    if (toInterval == null)
                        break;
                    switch (toInterval) {
                        case SECOND:
                            dateFormat = new SimpleDateFormat("mm:ss");
                            break;
                        default:
                            dateFormat = new SimpleDateFormat("mm:ss");
                            break;
                    }
                    break;
                case SECOND:
                    dateFormat = new SimpleDateFormat("ss");
                    toInterval = null;
                    break;
                default:
                    toInterval = null;
            }

            Randomly rand = new Randomly();

            Timestamp timestamp = new Timestamp(rand.getLong(0, System.currentTimeMillis()));
            textRepr = dateFormat.format(timestamp);
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            if (toInterval == null) {
                return String.format("INTERVAL '%s' %s", textRepr, fromInterval.name());
            } else {
                return String.format("INTERVAL '%s' %s TO %s", textRepr, fromInterval, toInterval);
            }
        }


        @Override
        public Node<PrestoExpression> cast(PrestoSchema.PrestoDataType dataType) {
            return null;
        }

        @Override
        public Node<PrestoExpression> isEquals(PrestoConstant rightVal) {
            return null;
        }

        @Override
        public Node<PrestoExpression> isLessThan(PrestoConstant rightVal) {
            return null;
        }
    }

    public static class PrestoIntervalYearToMonthConstant extends PrestoConstant {

        private enum Interval {
            YEAR, MONTH;
        }

        public String textRepr;
        public Interval fromInterval;
        public Interval toInterval;

        public PrestoIntervalYearToMonthConstant() {
            fromInterval = Randomly.fromOptions(Interval.values());
            SimpleDateFormat dateFormat;
            dateFormat = new SimpleDateFormat("yyyy:MM");
            switch (fromInterval) {
                case YEAR:
                    toInterval = Randomly.fromOptions(Interval.MONTH, null);
                    if (toInterval == null)
                        dateFormat = new SimpleDateFormat("yyyy");
                    break;
                case MONTH:
                    dateFormat = new SimpleDateFormat("MM");
                    break;
                default:
                    toInterval = null;
            }

            Randomly rand = new Randomly();

            Timestamp timestamp = new Timestamp(rand.getLong(0, System.currentTimeMillis()));
            textRepr = dateFormat.format(timestamp);
        }

        @Override
        public Node<PrestoExpression> cast(PrestoSchema.PrestoDataType dataType) {
            return null;
        }

        @Override
        public Node<PrestoExpression> isEquals(PrestoConstant rightVal) {
            return null;
        }

        @Override
        public Node<PrestoExpression> isLessThan(PrestoConstant rightVal) {
            return null;
        }

        public String getValue() {
            return textRepr;
        }

        @Override
        public String toString() {
            if (toInterval == null) {
                return String.format("INTERVAL '%s' %s", textRepr, fromInterval.name());
            } else {
                return String.format("INTERVAL '%s' %s TO %s", textRepr, fromInterval, toInterval);
            }
        }

    }

    public static class PrestoBooleanConstant extends PrestoConstant {

        private final boolean value;

        public PrestoBooleanConstant(boolean value) {
            this.value = value;
        }

        public boolean getValue() {
            return value;
        }

        @Override
        public String toString() {
            return String.valueOf(value);
        }


        @Override
        public boolean asBoolean() {
            return value;
        }

        @Override
        public boolean isBoolean() {
            return true;
        }

        @Override
        public PrestoConstant cast(PrestoSchema.PrestoDataType dataType) {
            switch (dataType) {
                case BOOLEAN:
                    return this;
                case INT:
                    return new PrestoConstant.PrestoIntConstant(value ? 1 : 0);
                case FLOAT:
                    return new PrestoConstant.PrestoFloatConstant(value ? 1 : 0);
                case VARCHAR:
                    return new PrestoConstant.PrestoTextConstant(value ? "1" : "0");
                default:
                    return null;
            }
        }

        @Override
        public Node<PrestoExpression> isEquals(PrestoConstant rightVal) {
            if (rightVal.isNull()) {
                return createNullConstant();
            } else if (rightVal.isBoolean()) {
                return createBooleanConstant(value == rightVal.asBoolean());
            } else {
                throw new AssertionError(rightVal);
            }

        }

        @Override
        public Node<PrestoExpression> isLessThan(PrestoConstant rightVal) {
            if (rightVal.isNull()) {
                return PrestoConstant.createNullConstant();
            } else if (rightVal.isBoolean()) {
                return PrestoConstant.createBooleanConstant((value ? 1 : 0) < (rightVal.asBoolean() ? 1 : 0));
            } else {
                throw new AssertionError(rightVal);
            }
        }
    }

    public static Node<PrestoExpression> createStringConstant(String text) {
        return new PrestoTextConstant(text);
    }

    public static Node<PrestoExpression> createStringConstant(String text, int size) {
        return new PrestoTextConstant(text, size);
    }

    public static Node<PrestoExpression> createJsonConstant() {
        return new PrestoJsonConstant();
    }

    public static Node<PrestoExpression> createFloatConstant(double val) {
        return new PrestoFloatConstant(val);
    }

    public static Node<PrestoExpression> createDecimalConstant(double val) {
        return new PrestoDecimalConstant(val);
    }

    public static Node<PrestoExpression> createIntConstant(long val) {
        return new PrestoIntConstant(val);
    }

    public static Node<PrestoExpression> createNullConstant() {
        return new PrestoNullConstant();
    }

    public static Node<PrestoExpression> createBooleanConstant(boolean val) {
        return new PrestoBooleanConstant(val);
    }

    public static Node<PrestoExpression> createDateConstant(long integer) {
        return new PrestoDateConstant(integer);
    }

    public static Node<PrestoExpression> createTimeConstant(long integer) {
        return new PrestoTimeConstant(integer);
    }

    public static Node<PrestoExpression> createTimeWithTimeZoneConstant(long integer) {
        return new PrestoTimeWithTimeZoneConstant(integer);
    }

    public static Node<PrestoExpression> createTimestampWithTimeZoneConstant(long integer) {
        return new PrestoTimestampWithTimezoneConstant(integer);
    }

    public static Node<PrestoExpression> createIntervalDayToSecond(long integer) {
        return new PrestoIntervalDayToSecondConstant();
    }

    public static Node<PrestoExpression> createIntervalYearToMonth(long integer) {
        return new PrestoIntervalYearToMonthConstant();
    }

    public static Node<PrestoExpression> createTimestampConstant(long integer) {
        return new PrestoTimestampConstant(integer);
    }

    public static Node<PrestoExpression> createVarbinaryConstant(String string) {
        return new PrestoVarbinaryConstant(string);
    }

}
