package lama.sqlite3.ast;

import lama.Randomly;
import lama.sqlite3.gen.SQLite3Cast;
import lama.sqlite3.schema.SQLite3DataType;
import lama.sqlite3.schema.SQLite3Schema.Column.CollateSequence;

public class SQLite3Function extends SQLite3Expression {

	private final ComputableFunction func;
	private final SQLite3Expression[] args;

	public SQLite3Function(ComputableFunction func, SQLite3Expression[] args) {
		assert args.length == func.getNrArgs();
		this.func = func;
		this.args = args;
	}

	public enum ComputableFunction {

		ABS(1, "ABS") {
			@Override
			public SQLite3Constant apply(SQLite3Constant... args) {
				SQLite3Constant castValue;
				if (args[0].getDataType() == SQLite3DataType.INT) {
					castValue = SQLite3Cast.castToInt(args[0]);
				} else {
					castValue = SQLite3Cast.castToReal(args[0]);
				}
				if (castValue.isNull()) {
					return castValue;
				} else if (castValue.getDataType() == SQLite3DataType.INT) {
					long absVal = Math.abs(castValue.asInt());
					return SQLite3Constant.createIntConstant(absVal);
				} else {
					assert castValue.getDataType() == SQLite3DataType.REAL;
					double absVal = Math.abs(castValue.asDouble());
					return SQLite3Constant.createRealConstant(absVal);
				}
			}
		},

		LOWER(1, "LOWER") {
			@Override
			public SQLite3Constant apply(SQLite3Constant... args) {
				if (args[0].getDataType() == SQLite3DataType.TEXT) {
					StringBuilder text = new StringBuilder(args[0].asString());
					for (int i = 0; i < text.length(); i++) {
						char c = text.charAt(i);
						if (c >= 'A' && c <= 'Z') {
							text.setCharAt(i, Character.toLowerCase(c));
						}
					}
					return SQLite3Constant.createTextConstant(text.toString());
				} else {
					return SQLite3Cast.castToText(args[0]);
				}
			}
		},
		LIKELY(1, "LIKELY") {
			@Override
			public SQLite3Constant apply(SQLite3Constant... args) {
				return args[0];
			}
		},
		LIKELIHOOD(2, "LIKELIHOOD") {
			@Override
			public SQLite3Constant apply(SQLite3Constant... args) {
				return args[0];
			}
		},
		IFNULL(2, "IFNULL") {
			@Override
			public SQLite3Constant apply(SQLite3Constant... args) {
				for (SQLite3Expression arg : args) {
					if (!arg.getExpectedValue().isNull()) {
						return arg.getExpectedValue();
					}
				}
				return SQLite3Constant.createNullConstant();
			}
		},

		UPPER(1, "UPPER") {

			@Override
			public SQLite3Constant apply(SQLite3Constant... args) {
				if (args[0].getDataType() == SQLite3DataType.TEXT) {
					StringBuilder text = new StringBuilder(args[0].asString());
					for (int i = 0; i < text.length(); i++) {
						char c = text.charAt(i);
						if (c >= 'a' && c <= 'z') {
							text.setCharAt(i, Character.toUpperCase(c));
						}
					}
					return SQLite3Constant.createTextConstant(text.toString());
				} else {
					return SQLite3Cast.castToText(args[0]);
				}
			}

		},
		NULLIF(2, "NULLIF") {
			@Override
			public SQLite3Constant apply(SQLite3Constant[] args, CollateSequence collateSequence) {
				if (collateSequence == null) {
					collateSequence = CollateSequence.BINARY;
				}
				SQLite3Constant equals = args[0].applyEquals(args[1], collateSequence);
				if (SQLite3Cast.isTrue(equals).isPresent() && SQLite3Cast.isTrue(equals).get()) {
					return SQLite3Constant.createNullConstant();
				} else {
					return args[0];
				}
			}

			@Override
			public SQLite3Constant apply(SQLite3Constant... args) {
				return apply(args, null);
			}
		},
		TRIM(1, "TRIM") {

			@Override
			public SQLite3Constant apply(SQLite3Constant... args) {
				SQLite3Constant str = SQLite3Cast.castToText(args[0]);
				if (args[0].getDataType() == SQLite3DataType.TEXT) {
					String text = str.asString();
					return SQLite3Constant.createTextConstant(text.trim());
				} else {
					return str;
				}
			}

		},
		TRIM_TWO_ARGS(2, "TRIM") {

			@Override
			public SQLite3Constant apply(SQLite3Constant... args) {
				if (args[0].isNull() || args[1].isNull()) {
					return SQLite3Constant.createNullConstant();
				}
				SQLite3Constant str = SQLite3Cast.castToText(args[0]);
				SQLite3Constant castToText = SQLite3Cast.castToText(args[1]);
				if (str == null || castToText == null) {
					return null;
				}
				String remove = castToText.asString();
				StringBuilder text = new StringBuilder(str.asString());
				int i = 0;
				while (i < text.length()) {
					boolean shouldRemoveChar = false;
					char c = text.charAt(i);
					for (char charToRemove : remove.toCharArray()) {
						if (charToRemove == c) {
							shouldRemoveChar = true;
							break;
						}
					}
					if (shouldRemoveChar) {
						text.deleteCharAt(i);
					} else {
						break;
					}
				}
				i = text.length() - 1;
				while (i >= 0) {
					boolean shouldRemoveChar = false;
					char c = text.charAt(i);
					for (char charToRemove : remove.toCharArray()) {
						if (charToRemove == c) {
							shouldRemoveChar = true;
							break;
						}
					}
					if (shouldRemoveChar) {
						text.deleteCharAt(i);
						i--;
					} else {
						break;
					}
				}
				String string = text.toString();
				assert string != null;
				return SQLite3Constant.createTextConstant(string);
			}
		},
		TYPEOF(1, "TYPEOF") {

			@Override
			public SQLite3Constant apply(SQLite3Constant... args) {
				switch (args[0].getDataType()) {
				case BINARY:
					return SQLite3Constant.createTextConstant("blob");
				case INT:
					return SQLite3Constant.createTextConstant("integer");
				case NULL:
					return SQLite3Constant.createTextConstant("null");
				case REAL:
					return SQLite3Constant.createTextConstant("real");
				case TEXT:
					return SQLite3Constant.createTextConstant("text");
				default:
					throw new AssertionError(args[0]);
				}
			}

		},
		UNLIKELY(1, "UNLIKELY") {
			@Override
			public SQLite3Constant apply(SQLite3Constant... args) {
				return args[0];
			}
		};

		private String functionName;
		final int nrArgs;

		private ComputableFunction(int nrArgs, String functionName) {
			this.nrArgs = nrArgs;
			this.functionName = functionName;
		}

		public int getNrArgs() {
			return nrArgs;
		}

		public abstract SQLite3Constant apply(SQLite3Constant... args);
		
		public SQLite3Constant apply(SQLite3Constant[] evaluatedArgs, CollateSequence collate) {
			return apply(evaluatedArgs);
		}


		public static ComputableFunction getRandomFunction() {
			return Randomly.fromOptions(ComputableFunction.values());
		}

		@Override
		public String toString() {
			return functionName;
		}

	}

	@Override
	public CollateSequence getExplicitCollateSequence() {
		for (SQLite3Expression expr : args) {
			if (expr.getExplicitCollateSequence() != null) {
				return expr.getExplicitCollateSequence();
			}
		}
		return null;
	}

	public SQLite3Expression[] getArgs() {
		return args;
	}

	public ComputableFunction getFunc() {
		return func;
	}

	public SQLite3Constant getExpectedValue() {
		SQLite3Constant[] constants = new SQLite3Constant[args.length];
		for (int i = 0; i < constants.length; i++) {
			constants[i] = args[i].getExpectedValue();
			if (constants[i] == null) {
				return null;
			}
		}
		CollateSequence collate = null;
		for (int i = 0; i < args.length; i++) {
			if (args[i].getExplicitCollateSequence() != null) {
				collate = args[i].getExplicitCollateSequence();
				break;
			}
		}
		return func.apply(constants, collate);

	};

}