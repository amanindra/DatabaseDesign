import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

public class BTree {

	private static final String TEXT = "text";
	private static final String DATE2 = "date";
	private static final String DATETIME = "datetime";
	private long NO_OF_CELLS_HEADER = 0;
	private long START_OF_CELL_HEADER = 0;
	private static final String DOUBLE = "double";
	private static final String REAL = "real";
	public static final int LEAF_NODE = 13;
	private static final String BIGINT = "bigint";
	public final int NODE_TYPE_OFFSET = 1;
	private static final String INT = "int";
	private static final String SMALLINT = "smallint";
	private static final String TINYINT = "tinyint";
	private boolean isTableSchema;

	private String tableName;


	private boolean isLeafPage = false;

	private int lastPage = 1;


	public static final int INTERNAL_NODE = 5;



	private RandomAccessFile fileBinary;

	private static final int pageSize = 512;

	private int currentPage = 1;


	private long pageHeaderOffsetRightPagePointer = 0;
	private long pageHeaderArrayOffset = 0;
	private long pageHeaderOffset = 0;

	private ZoneId zoneId = ZoneId.of("America/Chicago");

	private ArrayList<Integer> routeOfLeafPage = new ArrayList<>();

	private boolean isColumnSchema;


	private String tableKey = "rowid";

	private BTree dbColumnFileTree;

	public BTree(RandomAccessFile file, String tableName) {
		fileBinary = file;
		this.tableName = tableName;
		try {
			if (file.length() > 0) {
				lastPage = (int) (file.length() / 512);
				currentPage = lastPage;
			}

			if (!tableName.equals(MyDatabase.masterColumnTableName)
					&& !tableName.equals(MyDatabase.masterTableName)) {
				dbColumnFileTree = new BTree(new RandomAccessFile(
						Utility.getFilePath("master",
								MyDatabase.masterColumnTableName), "rw"),
						MyDatabase.masterColumnTableName, true, false);

				for (String key : dbColumnFileTree.getSchema(tableName)
						.keySet()) {
					tableKey = key;
					break;
				}

				//dbColumnFileTree.getSchema(tableName).keySet().forEach(k -> tableKey = k);


			}
		} catch (Exception e) {
			System.out.println("Unexpected Error in btree(RandomAccessFile file, String tableName)");
		}
	}

	public BTree(RandomAccessFile file, String tableName, boolean isColSchema,
				 boolean isTableSchema) {
		this(file, tableName);
		this.isColumnSchema = isColSchema;
		this.isTableSchema = isTableSchema;
	}

	public void createNewInterior(int pageNumber, int rowID, int pageRight) {
		try {
			fileBinary.seek(0);
			fileBinary.write(5);
			fileBinary.write(1);
			fileBinary.writeShort(pageSize - 8);
			fileBinary.writeInt(pageRight);
			fileBinary.writeShort(pageSize - 8);
			fileBinary.seek(pageSize - 8);
			fileBinary.writeInt(pageNumber);
			fileBinary.writeInt(rowID);

		} catch (IOException e) {
			System.out.println("Unexpected Error in create new interior)");
		}
	}



	public void deleteCellInterior(int pageLocation, int pageNumber) {
		try {
			fileBinary.seek(pageLocation * pageSize - pageSize + 1);
			short No_OfCells = fileBinary.readByte();
			int pos = No_OfCells;
			for (int i = 0; i < No_OfCells; i++) {
				fileBinary.seek((pageLocation * pageSize - pageSize + 8)
						+ (2 * (i)));
				fileBinary.seek(fileBinary.readUnsignedShort());
				if (pageNumber == fileBinary.readInt()) {
					pos = i;
					fileBinary.seek(pageLocation * pageSize - pageSize + 1);
					fileBinary.write(No_OfCells - 1);
					break;
				}
			}
			int temp;
			while (pos < No_OfCells) {
				fileBinary.seek((pageLocation * pageSize - pageSize + 8)
						+ (2 * (pos + 1)));
				temp = fileBinary.readUnsignedShort();
				fileBinary.seek((pageLocation * pageSize - pageSize + 8)
						+ (2 * (pos)));
				fileBinary.writeShort(temp);
				pos++;
			}
			temp = 0;
			for (int i = 0; i < No_OfCells - 1; i++) {
				fileBinary.seek((pageLocation * pageSize - pageSize + 8)
						+ (2 * (i)));
				if (temp < fileBinary.readUnsignedShort()) {
					fileBinary.seek((pageLocation * pageSize - pageSize + 8)
							+ (2 * (i)));
					temp = fileBinary.readUnsignedShort();
				}
			}
			fileBinary.seek(pageLocation * pageSize - pageSize + 2);
			fileBinary.writeShort(temp);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}



	public void createNewTableLeaf(Map<String, ArrayList<String>> token) {
		try {
			currentPage = 1;
			fileBinary.setLength(0);

			fileBinary.setLength(pageSize);

			writePageHeader(currentPage, true, 0, -1);

			long no_of_Bytes = payloadSizeInBytes(token);

			long cellStartOffset = (currentPage * (pageSize))
					- (no_of_Bytes + 6);

			writeCell(currentPage, token, cellStartOffset, no_of_Bytes);

		} catch (IOException e) {
			System.out.println("Unexpected Error in create table leaf");
		}
	}


	public void createEmptyTable() {

		try {
			currentPage = 1;
			fileBinary.setLength(0);

			fileBinary.setLength(pageSize);

			writePageHeader(currentPage, true, 0, -1);

		} catch (IOException e) {
			System.out.println("Unexpected Error in create empty table");
		}

	}
	public LinkedHashMap<String, ArrayList<String>> getSchema(String tableName) {
		ArrayList<String> val1 = new ArrayList<>();
		val1.add("1");
		val1.add("TEXT");
		val1.add(tableName);
		List<LinkedHashMap<String, ArrayList<String>>> output = searchWithNonPK(val1);

		LinkedHashMap<String, ArrayList<String>> finalResult = new LinkedHashMap<>();

		output.forEach(map -> {
			ArrayList<String> val = map.get("column_name");
			ArrayList<String> defaultText = map.get("default");
			ArrayList<String> nullStringList = map.get("is_nullable");
			ArrayList<String> dataTypeList = map.get("data_type");
			ArrayList<String> value = new ArrayList<>();

			String key = val.get(0);
			String dataType = dataTypeList.get(0);

			boolean isNullable = nullStringList.get(0).equals("yes");
			String nullString = isNullable ? "NULL" : "no";
			boolean hasDefault = defaultText != null
					&& !defaultText.get(0).equalsIgnoreCase("na");

			value.add(dataType);

			if (hasDefault) {
				value.add(defaultText.get(0));
			} else {
				value.add(nullString);
			}

			finalResult.put(key, value);



		});

		/*for (LinkedHashMap<String, ArrayList<String>> map : output) {
			ArrayList<String> val = map.get("column_name");
			ArrayList<String> defaultText = map.get("default");
			ArrayList<String> nullStringList = map.get("is_nullable");
			ArrayList<String> dataTypeList = map.get("data_type");
			ArrayList<String> value = new ArrayList<String>();

			String key = val.get(0);
			String dataType = dataTypeList.get(0);

			boolean isNullable = nullStringList.get(0).equals("yes");
			String nullString = isNullable ? "NULL" : "no";
			boolean hasDefault = defaultText != null
					&& !defaultText.get(0).equalsIgnoreCase("na");

			value.add(dataType);

			if (hasDefault) {
				value.add(defaultText.get(0));
			} else {
				value.add(nullString);
			}

			finalResult.put(key, value);
		}*/

		return finalResult;

	}
	public int getNextMaxRowID() {
		currentPage = 1;
		searchRMLNode();
		readPageHeader(currentPage);
		try {
			fileBinary.seek(NO_OF_CELLS_HEADER);
			int noOfCells = fileBinary.readUnsignedByte();
			fileBinary.seek(pageHeaderArrayOffset + (2 * (noOfCells - 1)));
			long address = fileBinary.readUnsignedShort();
			fileBinary.seek(address);
			fileBinary.readShort();
			return fileBinary.readInt();

		} catch (IOException e) {
			System.out.println("Unexpected Error in get next max row id");
		}
		return -1;

	}

	public boolean isPKPresent(int key) {

		currentPage = 1;

		searchLeafPage(key, false);
		readPageHeader(currentPage);
		long[] result = getCellOffset(key);
		long cellOffset = result[1];
		if (cellOffset > 0) {
			try {
				fileBinary.seek(cellOffset);
				fileBinary.readUnsignedShort();
				int actualRowID = fileBinary.readInt();
				// System.out.println("Row id is " + actualRowID);
				if (actualRowID == key) {

					return true;
				}

			} catch (IOException e) {
				System.out.println("Unexpected Error in isPK present" );
			}

		} else {
			return false;
		}

		return false;
	}

	public LinkedHashMap<String, ArrayList<String>> searchWithPrimaryKey(
			LinkedHashMap<String, ArrayList<String>> token) {

		currentPage = 1;

		LinkedHashMap<String, ArrayList<String>> value = null;
		int rowId = Integer.parseInt(token.get(tableKey).get(1));
		searchLeafPage(rowId, false);
		readPageHeader(currentPage);
		long[] result = getCellOffset(rowId);
		long cellOffset = result[1];
		if (cellOffset > 0) {
			try {
				fileBinary.seek(cellOffset);
				fileBinary.readUnsignedShort();
				int actualRowID = fileBinary.readInt();
				if (actualRowID == rowId) {

					token = getStringArrayListLinkedHashMap();

					value = populateData(cellOffset, token);

				}

			} catch (IOException e) {
				System.out.println("Unexpected Error in search with pk");
			}

			return value;

		} else {
			System.out.println(" No rows matches");
			return null;
		}

	}

	private LinkedHashMap<String, ArrayList<String>> getStringArrayListLinkedHashMap() {
		LinkedHashMap<String, ArrayList<String>> token;
		if (isColumnSchema) {
			token = new LinkedHashMap<>();
			token.put("rowid", null);
			token.put("table_name", null);
			token.put("column_name", null);
			token.put("data_type", null);
			token.put("ordinal_position", null);
			token.put("is_nullable", null);
			token.put("default", null);
			token.put("is_unique", null);
			token.put("auto_increment", null);
		} else if (isTableSchema) {
			token = new LinkedHashMap<>();
			token.put("rowid", null);
			token.put("table_name", null);

		} else {
			token = dbColumnFileTree.getSchema(tableName);
		}
		return token;
	}

	public boolean deleteRecord(LinkedHashMap<String, ArrayList<String>> token) {
		currentPage = 1;
		boolean isDone = false;

		int rowId = Integer.parseInt(token.get(tableKey).get(1));
		searchLeafPage(rowId, false);
		readPageHeader(currentPage);
		long[] retVal = getCellOffset(rowId);
		long cellOffset = retVal[1];
		if (cellOffset > 0) {
			try {
				fileBinary.seek(cellOffset);
				fileBinary.readUnsignedShort();
				int actualRowID = fileBinary.readInt();
				// System.out.println("Row id is " + actualRowID);
				if (actualRowID == rowId) {

					fileBinary.seek(START_OF_CELL_HEADER);
					long startOfCell = fileBinary.readUnsignedShort();
					if (cellOffset == startOfCell) {

						fileBinary.seek(cellOffset);
						int payLoadSize = fileBinary.readUnsignedShort();
						fileBinary.seek(START_OF_CELL_HEADER);
						fileBinary
								.writeShort((int) (startOfCell - payLoadSize - 6));

					}

					fileBinary.seek(NO_OF_CELLS_HEADER);

					int No_OfCells = fileBinary.readUnsignedByte();

					int temp;
					long pos = retVal[0];
					while (pos < No_OfCells) {
						fileBinary.seek((currentPage * pageSize - pageSize + 8)
								+ (2 * (pos + 1)));
						temp = fileBinary.readUnsignedShort();
						fileBinary.seek((currentPage * pageSize - pageSize + 8)
								+ (2 * (pos)));
						fileBinary.writeShort(temp);
						pos++;
					}

					fileBinary.seek(NO_OF_CELLS_HEADER);
					int col = fileBinary.readUnsignedByte();
					fileBinary.seek(NO_OF_CELLS_HEADER);
					fileBinary.writeByte(--col);
					if (col == 0) {

						fileBinary.seek(START_OF_CELL_HEADER);
						fileBinary.writeShort((int) (currentPage * pageSize));

					}
					isDone = true;
				} else {

					System.out.println("No row matches");
				}

			} catch (IOException e) {
				System.out.println("Unexpected Error in delete record");
			}

		} else {
			System.out.println(" No rows matches");
		}
		return isDone;
	}

	private void writeToInterior(int pageLocation, int pageNumber, int rowID,
								 int pageRight) {
		try {

			fileBinary.seek(pageLocation * pageSize - pageSize + 1);
			short No_OfCells = fileBinary.readByte();
			if (No_OfCells < 49) {
				fileBinary.seek(pageLocation * pageSize - pageSize + 4);
				if (fileBinary.readInt() == pageNumber && pageRight != -1) {
					fileBinary.seek(pageLocation * pageSize - pageSize + 4);
					fileBinary.writeInt(pageRight);
					long cellStartOffset = (pageLocation * (pageSize))
							- (8 * (No_OfCells + 1));
					fileBinary.seek(pageLocation * pageSize - pageSize + 2);
					fileBinary.writeShort((int) cellStartOffset);
					fileBinary.seek(pageLocation * pageSize - pageSize + 1);
					fileBinary.write(No_OfCells + 1);
					fileBinary.seek(cellStartOffset);
					fileBinary.writeInt(pageNumber);
					fileBinary.writeInt(rowID);
					fileBinary.seek((pageLocation * pageSize - pageSize + 8)
							+ (2 * No_OfCells));
					fileBinary.writeShort((short) cellStartOffset);
				} else {
					int flag = 0;
					for (int i = 0; i < No_OfCells; i++) {
						fileBinary
								.seek((pageLocation * pageSize - pageSize + 8)
										+ (2 * i));
						fileBinary.seek(fileBinary.readUnsignedShort());
						if (fileBinary.readInt() == pageNumber) {
							flag = 1;
							int tempRowID = fileBinary.readInt();
							fileBinary
									.seek((pageLocation * pageSize - pageSize + 8)
											+ (2 * i));
							fileBinary.seek(fileBinary.readUnsignedShort() + 4);
							fileBinary.writeInt(rowID);
							long cellStartOffset = (pageLocation * (pageSize))
									- (8 * (No_OfCells + 1));
							fileBinary.seek(pageLocation * pageSize - pageSize
									+ 2);
							fileBinary.writeShort((int) cellStartOffset);
							fileBinary.seek(pageLocation * pageSize - pageSize
									+ 1);
							fileBinary.write(No_OfCells + 1);
							fileBinary.seek(cellStartOffset);
							fileBinary.writeInt(pageRight);
							fileBinary.writeInt(tempRowID);
							fileBinary.seek(pageLocation * pageSize - pageSize
									+ 8 + 2 * No_OfCells);
							fileBinary.writeShort((short) cellStartOffset);
						}
					}
					if (flag == 0) {
						long cellStartOffset = (pageLocation * (pageSize))
								- (8 * (No_OfCells + 1));
						fileBinary.seek(pageLocation * pageSize - pageSize + 2);
						fileBinary.writeShort((int) cellStartOffset);
						fileBinary.seek(pageLocation * pageSize - pageSize + 1);
						fileBinary.write(No_OfCells + 1);
						fileBinary.seek(cellStartOffset);
						fileBinary.writeInt(pageNumber);
						fileBinary.writeInt(rowID);
						fileBinary.seek(pageLocation * pageSize - pageSize + 8
								+ 2 * No_OfCells);
						fileBinary.writeShort((short) cellStartOffset);
					}
				}
				int tempAddi, tempAddj, tempi, tempj;
				for (int i = 0; i <= No_OfCells; i++)
					for (int j = i + 1; j <= No_OfCells; j++) {
						fileBinary
								.seek((pageLocation * pageSize - pageSize + 8)
										+ (2 * i));
						tempAddi = fileBinary.readUnsignedShort();
						fileBinary
								.seek((pageLocation * pageSize - pageSize + 8)
										+ (2 * j));
						tempAddj = fileBinary.readUnsignedShort();
						fileBinary.seek(tempAddi + 4);
						tempi = fileBinary.readInt();
						fileBinary.seek(tempAddj + 4);
						tempj = fileBinary.readInt();
						if (tempi > tempj) {
							fileBinary
									.seek((pageLocation * pageSize - pageSize + 8)
											+ (2 * i));
							fileBinary.writeShort(tempAddj);
							fileBinary
									.seek((pageLocation * pageSize - pageSize + 8)
											+ (2 * j));
							fileBinary.writeShort(tempAddi);

						}
					}
			} else {
				fileBinary.seek(pageLocation * pageSize - pageSize + 4);
				if (fileBinary.readInt() == pageNumber && pageRight != -1) {
					fileBinary.seek(pageLocation * pageSize - pageSize + 4);
					fileBinary.writeInt(pageRight);
					long cellStartOffset = (pageLocation * (pageSize))
							- (8 * (No_OfCells + 1));
					fileBinary.seek(pageLocation * pageSize - pageSize + 2);
					fileBinary.writeShort((int) cellStartOffset);
					fileBinary.seek(pageLocation * pageSize - pageSize + 1);
					fileBinary.write(No_OfCells + 1);
					fileBinary.seek(cellStartOffset);
					fileBinary.writeInt(pageNumber);
					fileBinary.writeInt(rowID);
					fileBinary.seek((pageLocation * pageSize - pageSize + 8)
							+ (2 * No_OfCells));
					fileBinary.writeShort((short) cellStartOffset);
				} else {
					int flag = 0;
					for (int i = 0; i < No_OfCells; i++) {
						fileBinary
								.seek((pageLocation * pageSize - pageSize + 8)
										+ (2 * i));
						fileBinary.seek(fileBinary.readUnsignedShort());
						if (fileBinary.readInt() == pageNumber) {
							flag = 1;
							int tempRowID = fileBinary.readInt();
							fileBinary
									.seek((pageLocation * pageSize - pageSize + 8)
											+ (2 * i));
							fileBinary.seek(fileBinary.readUnsignedShort() + 4);
							fileBinary.writeInt(rowID);
							long cellStartOffset = (pageLocation * (pageSize))
									- (8 * (No_OfCells + 1));
							fileBinary.seek(pageLocation * pageSize - pageSize
									+ 2);
							fileBinary.writeShort((int) cellStartOffset);
							fileBinary.seek(pageLocation * pageSize - pageSize
									+ 1);
							fileBinary.write(No_OfCells + 1);
							fileBinary.seek(cellStartOffset);
							fileBinary.writeInt(pageRight);
							fileBinary.writeInt(tempRowID);
							fileBinary.seek(pageLocation * pageSize - pageSize
									+ 8 + 2 * No_OfCells);
							fileBinary.writeShort((short) cellStartOffset);
						}
					}
					if (flag == 0) {
						long cellStartOffset = (pageLocation * (pageSize))
								- (8 * (No_OfCells + 1));
						fileBinary.seek(pageLocation * pageSize - pageSize + 2);
						fileBinary.writeShort((int) cellStartOffset);
						fileBinary.seek(pageLocation * pageSize - pageSize + 1);
						fileBinary.write(No_OfCells + 1);
						fileBinary.seek(cellStartOffset);
						fileBinary.writeInt(pageNumber);
						fileBinary.writeInt(rowID);
						fileBinary.seek(pageLocation * pageSize - pageSize + 8
								+ 2 * No_OfCells);
						fileBinary.writeShort((short) cellStartOffset);
					}
				}
				int tempAddi, tempAddj, tempi, tempj;
				for (int i = 0; i <= No_OfCells; i++)
					for (int j = i + 1; j <= No_OfCells; j++) {
						fileBinary
								.seek((pageLocation * pageSize - pageSize + 8)
										+ (2 * i));
						tempAddi = fileBinary.readUnsignedShort();
						fileBinary
								.seek((pageLocation * pageSize - pageSize + 8)
										+ (2 * j));
						tempAddj = fileBinary.readUnsignedShort();
						fileBinary.seek(tempAddi + 4);
						tempi = fileBinary.readInt();
						fileBinary.seek(tempAddj + 4);
						tempj = fileBinary.readInt();
						if (tempi > tempj) {
							fileBinary
									.seek((pageLocation * pageSize - pageSize + 8)
											+ (2 * i));
							fileBinary.writeShort(tempAddj);
							fileBinary
									.seek((pageLocation * pageSize - pageSize + 8)
											+ (2 * j));
							fileBinary.writeShort(tempAddi);

						}
					}
				if (pageLocation == 1) {
					int x, y;
					fileBinary.seek((pageLocation * pageSize - pageSize + 8)
							+ (2 * 25));
					fileBinary.seek(fileBinary.readUnsignedShort());
					x = fileBinary.readInt();
					y = fileBinary.readInt();
					writePageHeader(lastPage + 1, false, 0, x);
					for (int i = 0; i < 25; i++) {
						fileBinary
								.seek((pageLocation * pageSize - pageSize + 8)
										+ (2 * i));
						fileBinary.seek(fileBinary.readUnsignedShort());
						writeToInterior(lastPage + 1, fileBinary.readInt(),
								fileBinary.readInt(), -1);
					}
					fileBinary.seek(pageLocation * pageSize - pageSize + 4);
					writePageHeader(lastPage + 2, false, 0,
							fileBinary.readInt());
					for (int i = 26; i < 50; i++) {
						fileBinary
								.seek((pageLocation * pageSize - pageSize + 8)
										+ (2 * i));
						fileBinary.seek(fileBinary.readUnsignedShort());
						writeToInterior(lastPage + 2, fileBinary.readInt(),
								fileBinary.readInt(), -1);
					}
					writePageHeader(1, false, 0, lastPage + 2);

					writeToInterior(1, lastPage + 1, y, lastPage + 2);
					lastPage += 2;

				} else {

					int x, y;
					fileBinary.seek((pageLocation * pageSize - pageSize + 8)
							+ (2 * 25));
					fileBinary.seek(fileBinary.readUnsignedShort());
					x = fileBinary.readInt();
					y = fileBinary.readInt();
					fileBinary.seek(pageLocation * pageSize - pageSize + 4);
					writePageHeader(lastPage + 1, false, 0,
							fileBinary.readInt());
					fileBinary.seek(pageLocation * pageSize - pageSize + 4);
					fileBinary.writeInt(x);
					for (int i = 26; i < 50; i++) {
						fileBinary
								.seek((pageLocation * pageSize - pageSize + 8)
										+ (2 * i));
						fileBinary.seek(fileBinary.readUnsignedShort());
						writeToInterior(lastPage + 1, fileBinary.readInt(),
								fileBinary.readInt(), -1);

					}

					fileBinary.seek(pageLocation * pageSize - pageSize + 1);
					fileBinary.write(25);

					int lastInteriorPage = routeOfLeafPage
							.remove(routeOfLeafPage.size() - 1);

					writeToInterior(lastInteriorPage, pageLocation, y,
							lastPage + 1);
					lastPage++;

				}
			}

		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}

	private void searchLeftMostLeafNode() {
		routeOfLeafPage.add(currentPage);
		readPageHeader(currentPage);
		if (isLeafPage) {
			routeOfLeafPage.remove(routeOfLeafPage.size() - 1);
			return;
		} else {
			try {
				fileBinary.seek(NO_OF_CELLS_HEADER);

				int noOfColumns = fileBinary.readUnsignedByte();

				fileBinary.seek(pageHeaderArrayOffset);
				int address;
				if (noOfColumns > 0) {

					address = fileBinary.readUnsignedShort();

					fileBinary.seek(address);
					int pageNumber = fileBinary.readInt();

					currentPage = pageNumber;
					searchLeftMostLeafNode();

				}
			} catch (IOException e) {
				System.out.println("Unexpected Error in search left most node");
			}
		}

	}

	private void searchRMLNode() {

		routeOfLeafPage.add(currentPage);
		readPageHeader(currentPage);
		if (isLeafPage) {

			routeOfLeafPage.remove(routeOfLeafPage.size() - 1);
			return;
		} else {
			try {
				fileBinary.seek(pageHeaderOffsetRightPagePointer);

				currentPage = fileBinary.readInt();

				searchRMLNode();

			} catch (IOException e) {
				System.out.println("Unexpected Error in search rml node");
			}
		}

	}



	public List<LinkedHashMap<String, ArrayList<String>>> printAll() {
		currentPage = 1;
		List<LinkedHashMap<String, ArrayList<String>>> result = new ArrayList<LinkedHashMap<String, ArrayList<String>>>();
		searchLeftMostLeafNode();
		while (currentPage > 0) {
			try {
				readPageHeader(currentPage);
				printCurrentPg(result);

				fileBinary.seek(pageHeaderOffsetRightPagePointer);

				currentPage = fileBinary.readInt();

			} catch (Exception e) {
				System.out.println("Unexpected Error in search all");
			}
		}
		return result;

	}

	public List<LinkedHashMap<String, ArrayList<String>>> searchWithNonPK(
			ArrayList<String> value) {
		currentPage = 1;
		List<LinkedHashMap<String, ArrayList<String>>> result = new ArrayList<LinkedHashMap<String, ArrayList<String>>>();
		searchLeftMostLeafNode();
		while (currentPage > 0) {
			try {
				readPageHeader(currentPage);
				searchCurrentPg(value, result);
				// printRecordsInTheCurrentPage(result);

				fileBinary.seek(pageHeaderOffsetRightPagePointer);

				currentPage = fileBinary.readInt();

			} catch (Exception e) {
				System.out.println("Unexpected Error in search with no pk");
			}
		}

		return result;

	}

	private void printCurrentPg(
			List<LinkedHashMap<String, ArrayList<String>>> result)
			throws Exception {
		fileBinary.seek(NO_OF_CELLS_HEADER);
		int noOfCol = fileBinary.readUnsignedByte();

		fileBinary.seek(pageHeaderArrayOffset);
		long point = fileBinary.getFilePointer();
		int address = fileBinary.readUnsignedShort();

		for (int i = 0; i < noOfCol; i++) {

			fileBinary.seek(address);

			fileBinary.readUnsignedShort();
			int currentRowID = fileBinary.readInt();

			LinkedHashMap<String, ArrayList<String>> token = null;
			token = getStringArrayListLinkedHashMap();

			result.add(populateData(address, token));

			point = (point + 2);
			fileBinary.seek(point);
			address = fileBinary.readUnsignedShort();

		}

	}

	private void searchCurrentPg(ArrayList<String> searchCond,
								 List<LinkedHashMap<String, ArrayList<String>>> result)
			throws Exception {
		fileBinary.seek(NO_OF_CELLS_HEADER);
		int noOfCol = fileBinary.readUnsignedByte();

		fileBinary.seek(pageHeaderArrayOffset);
		long point = fileBinary.getFilePointer();
		int address = fileBinary.readUnsignedShort();

		for (int i = 0; i < noOfCol; i++) {

			fileBinary.seek(address);

			fileBinary.readUnsignedShort();
			int currentRowID = fileBinary.readInt();
			LinkedHashMap<String, ArrayList<String>> token = null;
			token = getStringArrayListLinkedHashMap();
			token = populateResult(searchCond, address, token);
			if (token != null)
				result.add(token);

			point = (point + 2);
			fileBinary.seek(point);
			address = fileBinary.readUnsignedShort();

		}

	}




	private long[] getCellOffset(int rowId) {
		long[] retVal = new long[2];
		int cellOffset = -1;
		try {
			fileBinary.seek(NO_OF_CELLS_HEADER);

			int noOfColumns = fileBinary.readUnsignedByte();

			fileBinary.seek(pageHeaderArrayOffset);
			long point = fileBinary.getFilePointer();
			int address = fileBinary.readUnsignedShort();
			for (int i = 0; i < noOfColumns; i++) {

				fileBinary.seek(address);

				fileBinary.readUnsignedShort();
				int currentRowID = fileBinary.readInt();

				if (rowId == currentRowID) {
					cellOffset = address;
					retVal[0] = i;
					retVal[1] = cellOffset;
					return retVal;

				} else {

					point = (point + 2);
					fileBinary.seek(point);
					address = fileBinary.readUnsignedShort();
				}

			}

		} catch (IOException e) {
			System.out.println("Unexpected Error in get cell offset");
		}

		return retVal;
	}

	public boolean isEmptyTable() throws IOException {
		return fileBinary.length() == 0;
	}

	public void insertNewRecord(Map<String, ArrayList<String>> token)
			throws Exception {
		currentPage = 1;
		int rowId = -1;
		if (isColumnSchema || isTableSchema) {
			tableKey = "rowid";
		}
		rowId = Integer.parseInt(token.get(tableKey).get(1));
		if (rowId < 0)
			throw new Exception("Insertion failed");

		searchLeafPage(rowId, false);

		insertNewRecordInPage(token, rowId, currentPage);

		routeOfLeafPage.clear();

	}

	private void insertNewRecordInPage(Map<String, ArrayList<String>> token,
									   int rowId, int pageNumber) {
		readPageHeader(pageNumber);

		long no_of_Bytes = payloadSizeInBytes(token);
		long cellStartOffset = 0;
		try {
			fileBinary.seek(START_OF_CELL_HEADER);

			cellStartOffset = ((long) fileBinary.readUnsignedShort())
					- (no_of_Bytes + 6);

		} catch (IOException e) {
			System.out.println("Unexpected Error in insert new record");
		}
		if (cellStartOffset < pageHeaderOffset + 2) {

			LinkedList<byte[]> page1Cells = new LinkedList<>();
			LinkedList<byte[]> page2Cells = new LinkedList<>();
			try {
				fileBinary.seek(NO_OF_CELLS_HEADER);
				int no_of_Cells = fileBinary.readUnsignedByte();

				int splitCells = no_of_Cells / 2;
				int loc = 0;
				// long point = pageHeaderArrayOffset;
				splitCells = 1;

				long point = pageHeaderOffset - 2;

				fileBinary.seek(point);

				fileBinary.seek(fileBinary.readUnsignedShort());

				fileBinary.readUnsignedShort();

				int currenRowID = fileBinary.readInt();
				while ((currenRowID > rowId)) {
					splitCells++;
					point = point - 2;
					fileBinary.seek(point);
					fileBinary.seek(fileBinary.readUnsignedShort());
					fileBinary.readUnsignedShort();
					currenRowID = fileBinary.readInt();
				}

				if (point == pageHeaderOffset - 2) {
					splitCells = 0;
					// No need of split the current page
					if (currentPage == 1) {
						point = pageHeaderArrayOffset;
						for (int i = 1; i <= no_of_Cells; i++) {

							point = getPoint(page1Cells, point);

						}

					}

				} else {
					// split the page

					if (currentPage == 1) {
						point = pageHeaderArrayOffset;
						for (int i = 1; i <= no_of_Cells - splitCells; i++) {

							point = getPoint(page1Cells, point);
						}
					}

					for (int i = splitCells; i <= 1; i--) {

						point = pageHeaderOffset - (2 * i);
						fileBinary.seek(point);
						loc = fileBinary.readUnsignedShort();

						fileBinary.seek(point);
						fileBinary.writeShort(0);

						fileBinary.seek(loc);
						byte[] cell = readCell(loc);

						page2Cells.add(cell);
					}
				}

				int rowIdMiddle = 0;
				if (currenRowID > rowId) {
					rowIdMiddle = currenRowID;
				} else {
					rowIdMiddle = rowId;
				}

				if (splitCells > 0) {
					fileBinary.seek(NO_OF_CELLS_HEADER);
					int noOfcells = fileBinary.readUnsignedByte();
					fileBinary.seek(NO_OF_CELLS_HEADER);
					fileBinary.writeByte(noOfcells - splitCells);
				}

				// split the page;
				int[] pageNumbers = splitLeafPage(page1Cells, page2Cells);

				// write Right Sibling for both pages
				fileBinary.seek(((pageNumbers[0] * pageSize) - pageSize) + 4);
				int prevRight = fileBinary.readInt();
				fileBinary.seek(((pageNumbers[0] * pageSize) - pageSize) + 4);
				fileBinary.writeInt(pageNumbers[1]);
				fileBinary.seek(((pageNumbers[1] * pageSize) - pageSize) + 4);
				fileBinary.writeInt(prevRight);

				// ch16PageFileExample.displayBinaryHex(binaryFile);

				// Interior Page Logic

				if (routeOfLeafPage.size() > 0
						&& routeOfLeafPage.get(routeOfLeafPage.size() - 1) > 0) {

					// if interior page exist
					writeToInterior(
							routeOfLeafPage.remove(routeOfLeafPage.size() - 1),
							pageNumbers[0], rowIdMiddle, pageNumbers[1]);

				} else {
					// create new interior page
					currentPage = 1;
					createNewInterior(pageNumbers[0], rowIdMiddle,
							pageNumbers[1]);

				}

				if (rowId < rowIdMiddle) {
					currentPage = pageNumbers[0];
				} else {
					currentPage = pageNumbers[1];
				}
				insertNewRecordInPage(token, rowId, currentPage);

			} catch (IOException e) {
				System.out.println("Unexpected Error insert new record last catch");
			}
		} else {

			writeCell(currentPage, token, cellStartOffset, no_of_Bytes);
		}
	}

	private long getPoint(LinkedList<byte[]> page1Cells, long point) throws IOException {
		int loc;
		fileBinary.seek(point);
		loc = fileBinary.readUnsignedShort();

		fileBinary.seek(point);
		fileBinary.writeShort(0);

		point = fileBinary.getFilePointer();

		fileBinary.seek(loc);
		fileBinary.readUnsignedShort();

		fileBinary.seek(loc);
		byte[] cell = readCell(loc);
		page1Cells.add(cell);
		return point;
	}

	private boolean searchLeafPage(int rowId, boolean isFound) {

		routeOfLeafPage.add(currentPage);
		readPageHeader(currentPage);
		if (isLeafPage) {

			routeOfLeafPage.remove(routeOfLeafPage.size() - 1);
			return true;
		} else {
			try {
				fileBinary.seek(NO_OF_CELLS_HEADER);

				int noOfColumns = fileBinary.readUnsignedByte();

				fileBinary.seek(pageHeaderArrayOffset);
				long currentArrayElementOffset = fileBinary.getFilePointer();
				int address;
				for (int i = 0; i < noOfColumns; i++) {
					fileBinary.seek(currentArrayElementOffset);
					address = fileBinary.readUnsignedShort();
					currentArrayElementOffset = fileBinary.getFilePointer();
					fileBinary.seek(address);
					int pageNumber = fileBinary.readInt();
					int delimiterRowId = fileBinary.readInt();
					if (rowId < delimiterRowId) {
						currentPage = pageNumber;
						isFound = searchLeafPage(rowId, false);

						break;
					}
				}

				if (!isFound) {
					fileBinary.seek(pageHeaderOffsetRightPagePointer);
					currentPage = fileBinary.readInt();
					isFound = searchLeafPage(rowId, false);
				}

			} catch (IOException e) {
				System.out.println("Unexpected Error in search leaf flag ");
			}
			return isFound;
		}

	}

	private byte[] readCell(int loc) {

		try {
			fileBinary.seek(loc);

			int payloadLength = fileBinary.readUnsignedShort();

			byte[] b = new byte[6 + payloadLength];
			fileBinary.seek(loc);

			fileBinary.read(b);
			fileBinary.seek(loc);
			fileBinary.write(new byte[6 + payloadLength]);

			return b;
		} catch (Exception e) {
			System.out.println("Unexpected Error in read cell");
		}

		return null;

	}

	/*long helper(int b, long offSetForData, ArrayList<String> arrayOfValues, LinkedHashMap<String, ArrayList<String>> token, String key) throws IOException {
		fileBinary.seek(offSetForData);
		int p;
		switch (b) {
			case 0:
				p = (fileBinary.readUnsignedByte());
				break;
			case 1:
				p = (fileBinary.readUnsignedShort());
				break;
			case 2:
				p = (fileBinary.readInt());
				break;
			case 3:
				p = (int) (fileBinary.readDouble());
				break;
		}
		arrayOfValues.add("NULL");
		offSetForData = fileBinary.getFilePointer();
		token.put(key, new ArrayList<>(arrayOfValues));
		return offSetForData;
	}*/


	private LinkedHashMap<String, ArrayList<String>> populateData(
			long cellOffset, LinkedHashMap<String, ArrayList<String>> token) {

		ArrayList<String> arrayOfValues = new ArrayList<>();
		try {
			fileBinary.seek(cellOffset);
			int payLoadSize = fileBinary.readUnsignedShort();
			Integer actualRowID = fileBinary.readInt();
			short noOfColumns = fileBinary.readByte();
			payLoadSize -= 1;
			long offsetForSerialType = fileBinary.getFilePointer();
			long offSetForData = (offsetForSerialType + noOfColumns);
			int i = 0;
			for (String key : token.keySet()) {

				if (i == 0) {
					arrayOfValues.add(actualRowID.toString());
					token.put(key, new ArrayList<>(arrayOfValues));
					i++;
					arrayOfValues.clear();
					continue;
				}

				fileBinary.seek(offsetForSerialType);
				short b = fileBinary.readByte();
				offsetForSerialType = fileBinary.getFilePointer();

				switch (b) {
					case 0: {

						//offSetForData= helper(b,offSetForData, arrayOfValues, token, key);


						offSetForData = getOffset(token, arrayOfValues, offSetForData, key);

						break;
					}
					case 1: {

						offSetForData = getOffset1(token, arrayOfValues, offSetForData, key);
						//offSetForData= helper(b,offSetForData, arrayOfValues, token, key);

						break;
					}
					case 2: {
						offSetForData = getOffset3(token, arrayOfValues, offSetForData, key);
						//offSetForData= helper(b,offSetForData, arrayOfValues, token, key);
						break;
					}
					case 3: {

						offSetForData = getOffset4(token, arrayOfValues, offSetForData, key);
						//offSetForData= helper(b,offSetForData, arrayOfValues, token, key);
						break;
					}
					case 12:
						arrayOfValues.add("NULL");
						token.put(key, new ArrayList<>(arrayOfValues));
						break;
					case 4:
						fileBinary.seek(offSetForData);
						arrayOfValues.add(Integer.toString(fileBinary
								.readUnsignedByte()));
						offSetForData = fileBinary.getFilePointer();
						token.put(key, new ArrayList<>(arrayOfValues));
						break;
					case 5:
						fileBinary.seek(offSetForData);
						arrayOfValues.add(Integer.toString(fileBinary
								.readUnsignedShort()));
						offSetForData = fileBinary.getFilePointer();
						token.put(key, new ArrayList<String>(arrayOfValues));
						break;
					case 6:
						fileBinary.seek(offSetForData);
						arrayOfValues.add(Integer.toString(fileBinary.readInt()));
						offSetForData = fileBinary.getFilePointer();
						token.put(key, new ArrayList<String>(arrayOfValues));
						break;
					case 7:
						fileBinary.seek(offSetForData);
						arrayOfValues.add(Long.toString(fileBinary.readLong()));
						offSetForData = fileBinary.getFilePointer();
						token.put(key, new ArrayList<String>(arrayOfValues));
						break;
					case 8:

						fileBinary.seek(offSetForData);
						arrayOfValues.add(Float.toString(fileBinary.readFloat()));
						offSetForData = fileBinary.getFilePointer();
						token.put(key, new ArrayList<String>(arrayOfValues));
						break;
					case 9:

						fileBinary.seek(offSetForData);
						arrayOfValues.add(Double.toString(fileBinary.readDouble()));
						offSetForData = fileBinary.getFilePointer();
						token.put(key, new ArrayList<String>(arrayOfValues));

						break;
					case 10: {
						fileBinary.seek(offSetForData);
						// arrayOfValues.add(Long.toString(binaryFile.readLong()));

						long timeInEpoch = fileBinary.readLong();
						Instant ii = Instant.ofEpochSecond(timeInEpoch);
						ZonedDateTime zdt2 = ZonedDateTime.ofInstant(ii, zoneId);
						offSetForData = getOffset5(token, arrayOfValues, key, zdt2);
						break;
					}
					case 11: {
						fileBinary.seek(offSetForData);
						long timeInEpoch = fileBinary.readLong();
						Instant ii = Instant.ofEpochSecond(timeInEpoch);
						ZonedDateTime zdt2 = ZonedDateTime.ofInstant(ii, zoneId);
						offSetForData = getOffset6(token, arrayOfValues, key, zdt2);
						break;
					}
					default:
						byte[] text = new byte[b - 12];
						fileBinary.seek(offSetForData);

						fileBinary.read(text);
						arrayOfValues.add(new String(text));
						offSetForData = fileBinary.getFilePointer();

						token.put(key, new ArrayList<String>(arrayOfValues));

						break;
				}
				arrayOfValues.clear();
			}

		} catch (Exception e) {
			System.out.println("Unexpected Error in populate data");
		}

		return token;
	}


	private int[] splitLeafPage(LinkedList<byte[]> page1Cells,
								LinkedList<byte[]> page2Cells) {

		int[] pageNumbers = new int[2];

		// add existing Page
		try {
			if (currentPage != 1) {
				pageNumbers[0] = currentPage;
				pageHeaderOffset = pageHeaderArrayOffset;
				if (page1Cells.size() > 0) {
					fileBinary.seek(START_OF_CELL_HEADER);
					fileBinary.writeShort(currentPage * (pageSize));
				}
				for (byte[] s : page1Cells) {

					long cellStartOffset = 0;

					fileBinary.seek(START_OF_CELL_HEADER);

					cellStartOffset = ((long) fileBinary.readUnsignedShort())
							- (s.length);
					writeCellInBytes(currentPage, s, cellStartOffset);

				}
			} else {

				// create new Page
				lastPage += 1;

				pageNumbers[0] = lastPage;
				currentPage = lastPage;
				createPage(page1Cells);
			}
		} catch (IOException e) {
			System.out.println("Unexpected Error in splitleaf page");

		}

		// create new Page
		lastPage += 1;

		pageNumbers[1] = lastPage;
		currentPage = lastPage;
		createPage(page2Cells);
		return pageNumbers;

	}

	// token<ColumnName, <data_type,value>>
	private void writeCell(int pageLocation,
						   Map<String, ArrayList<String>> token, long cellStartOffset,
						   long no_of_Bytes) {

		try {
			fileBinary.seek(START_OF_CELL_HEADER);
			fileBinary.writeShort((int) cellStartOffset);

			fileBinary.seek(NO_OF_CELLS_HEADER);
			short current_Cell_size = fileBinary.readByte();
			fileBinary.seek(NO_OF_CELLS_HEADER);
			fileBinary.write(current_Cell_size + 1);

		} catch (IOException e1) {
			e1.printStackTrace();
		}

		writeToHeaderArray(cellStartOffset,
				Integer.parseInt(token.get(tableKey).get(1)));
		try {
			fileBinary.seek(cellStartOffset);
			/**
			 * Write cell header
			 */
			int rowId_or_pageNo = Integer.parseInt(token.get(tableKey).get(1));
			fileBinary.writeShort((int) no_of_Bytes);
			fileBinary.writeInt(rowId_or_pageNo);
		} catch (IOException e) {
			System.out.println("Unexpected Error in write cell");
		}

		writeCellContent(pageLocation, token);
	}

	private void writeCellInBytes(int pageLocation, byte[] b,
								  long cellStartOffset) {

		try {
			fileBinary.seek(START_OF_CELL_HEADER);
			fileBinary.writeShort((int) cellStartOffset);
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		byte[] rowId = Arrays.copyOfRange(b, 2, 6);
		int id = java.nio.ByteBuffer.wrap(rowId).getInt();
		writeToHeaderArray(cellStartOffset, id);
		try {
			fileBinary.seek(cellStartOffset);
			fileBinary.write(b);
		} catch (IOException e) {
			System.out.println("Unexpected Error in write cell in bytes");
		}

	}

	private long payloadSizeInBytes(Map<String, ArrayList<String>> token) {
		long no_of_Bytes = 0;
		for (String key : token.keySet()) {
			if (key.equals(tableKey))
				continue; // Primary Key not needed in payload
			ArrayList<String> data_type = token.get(key);

			switch (data_type.get(0).trim().toLowerCase()) {
				case TINYINT:
					no_of_Bytes += 1;
					break;
				case SMALLINT:
					no_of_Bytes += 2;
					break;
				case INT:
					no_of_Bytes += 4;
					break;
				case BIGINT:
					no_of_Bytes += 8;
					break;
				case REAL:
					no_of_Bytes += 4;
					break;
				case DOUBLE:
					no_of_Bytes += 8;
					break;
				case DATETIME:
					no_of_Bytes += 8;
					break;
				case DATE2:
					no_of_Bytes += 8;
					break;
				case TEXT:
					no_of_Bytes += data_type.get(1).length() + 10;
					break;
			}

		}
		// 14
		no_of_Bytes += token.size(); // 1-byte TINYINT for no of columns + n
		// byte serial-type-code
		return no_of_Bytes;
	}

	private void writeToHeaderArray(long cellStartOffset, int rowID) {

		try {
			fileBinary.seek(NO_OF_CELLS_HEADER);
			int No_OfCells = fileBinary.readUnsignedByte();
			// pageHeaderOffset = binaryFile.getFilePointer();

			int pos = 0;
			for (int i = 0; i < No_OfCells; i++) {
				fileBinary.seek((currentPage * pageSize - pageSize + 8)
						+ (2 * i));
				fileBinary.seek(fileBinary.readUnsignedShort() + 2);
				if (rowID < fileBinary.readInt()) {
					pos = i;
					break;
				}
			}
			while (pos < No_OfCells) {
				fileBinary.seek((currentPage * pageSize - pageSize + 8)
						+ (2 * (No_OfCells - 1)));
				fileBinary.writeShort(fileBinary.readUnsignedShort());
				No_OfCells--;
			}
			fileBinary.seek((currentPage * pageSize - pageSize + 8)
					+ (2 * (pos)));
			fileBinary.writeShort((int) cellStartOffset);

		}

		catch (Exception e) {
			System.out.println("Unexpected Error in write header array");
		}
	}

	private void writeCellContent(int pageLocation2,
								  Map<String, ArrayList<String>> token) {
		try {

			fileBinary.write(token.size() - 1);// no of columns

			writeSrl(token);

			writePayload(token);
		} catch (Exception e) {
			System.out.println("Unexpected Error in write cell content");
		}

	}

	private void writePayload(Map<String, ArrayList<String>> token)
			throws IOException{
		// Payload

		for (String key : token.keySet()) {
			if (key.equals(tableKey))
				continue; // primary key not needed

			ArrayList<String> data_type = token.get(key);

			switch (data_type.get(0).trim().toLowerCase()) {

				case TINYINT:
					if (data_type.get(1) != null
							&& !data_type.get(1).trim().equalsIgnoreCase("null")) {
						fileBinary.write(Integer.parseInt(data_type.get(1)));
					} else {
						fileBinary.write(128);

					}

					break;
				case SMALLINT:
					if (data_type.get(1) != null
							&& !data_type.get(1).trim().equalsIgnoreCase("null")) {
						fileBinary.writeShort(Integer.parseInt(data_type.get(1)));
					} else {
						fileBinary.writeShort(-1);
					}

					break;
				case INT:
					if (data_type.get(1) != null
							&& !data_type.get(1).trim().equalsIgnoreCase("null")) {
						fileBinary.writeInt(Integer.parseInt(data_type.get(1)));
					} else {
						fileBinary.writeInt(-1);
					}
					break;
				case BIGINT:
					if (data_type.get(1) != null
							&& !data_type.get(1).trim().equalsIgnoreCase("null")) {
						fileBinary.writeLong(Long.parseLong(data_type.get(1)));
					} else {
						fileBinary.writeLong(-1);
					}

					break;
				case REAL:
					if (data_type.get(1) != null
							&& !data_type.get(1).trim().equalsIgnoreCase("null")) {
						fileBinary.writeFloat(Float.parseFloat((data_type.get(1))));
					} else {
						fileBinary.writeFloat(-1);
					}

					break;
				case DOUBLE:
					if (data_type.get(1) != null
							&& !data_type.get(1).trim().equalsIgnoreCase("null")) {
						fileBinary
								.writeDouble(Double.parseDouble((data_type.get(1))));
					} else {
						fileBinary.writeDouble(-1);
					}

					break;
				case DATETIME:
					if (data_type.get(1) != null
							&& !data_type.get(1).trim().equalsIgnoreCase("null")) {

						SimpleDateFormat df = new SimpleDateFormat(
								"yyyy-MM-dd_HH:mm:ss");

						Date date;
						try {
							date = df.parse(data_type.get(1));

							ZonedDateTime zdt = ZonedDateTime.ofInstant(
									date.toInstant(), zoneId);

							long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;

							fileBinary.writeLong(epochSeconds);
						} catch (ParseException e) {
							System.out.println("Unexpected Error in write payload date time");
						}
					} else {
						fileBinary.writeLong(-1);

					}

					break;
				case DATE2:
					if (data_type.get(1) != null
							&& !data_type.get(1).trim().equalsIgnoreCase("null")) {

						SimpleDateFormat d = new SimpleDateFormat("yyyy-MM-dd");

						Date date;
						try {
							date = d.parse(data_type.get(1));

							ZonedDateTime zdt = ZonedDateTime.ofInstant(
									date.toInstant(), zoneId);

							long epochSeconds = zdt.toInstant().toEpochMilli() / 1000;

							fileBinary.writeLong(epochSeconds);
						} catch (ParseException e) {
							System.out.println("Unexpected Error in write payload date2");
						}
					} else {
						fileBinary.writeLong(-1);

					}

					break;
				case TEXT:
					if (data_type.get(1) != null) {
						String s = data_type.get(1);
						byte[] b = s.getBytes("UTF-8");
						for (byte bb : b)
							fileBinary.write(bb);
					}

					break;

			}

		}
	}

	private LinkedHashMap<String, ArrayList<String>> populateResult(
			ArrayList<String> searchCond, long cellOffset,
			LinkedHashMap<String, ArrayList<String>> token) {

		ArrayList<String> arrayOfValues = new ArrayList<String>();
		try {
			fileBinary.seek(cellOffset);
			int payLoadSize = fileBinary.readUnsignedShort();
			Integer actualRowID = fileBinary.readInt();
			short noOfColumns = fileBinary.readByte();
			payLoadSize -= 1;
			long offsetForSerialType = fileBinary.getFilePointer();
			long offset = (offsetForSerialType + noOfColumns);

			boolean matchFound = false;
			int i = 0;

			String seachCol = searchCond.get(0);
			String searchDataType = searchCond.get(1);
			String valueToBeSearched = searchCond.get(2);

			String value = null;

			long offsetForSerialTypeMatch = offsetForSerialType;
			long offSetForDataMatch = (offset);

			int colIndex = Integer.parseInt(seachCol);

			int currentColIndex = 1;

			for (String key : token.keySet()) {

				fileBinary.seek(offsetForSerialType);
				short b = fileBinary.readByte();
				offsetForSerialType = fileBinary.getFilePointer();
				switch (b) {
					case 0: {

						fileBinary.seek(offset);
						int p = (fileBinary.readUnsignedByte());
						value = "NULL";
						offset = fileBinary.getFilePointer();
						token.put(key, new ArrayList<String>(arrayOfValues));

						break;
					}
					case 1: {

						fileBinary.seek(offset);
						int p = (fileBinary.readUnsignedShort());
						value = "NULL";
						offset = fileBinary.getFilePointer();
						token.put(key, new ArrayList<String>(arrayOfValues));

						break;
					}
					case 2: {
						fileBinary.seek(offset);
						int p = (fileBinary.readInt());
						value = "NULL";
						offset = fileBinary.getFilePointer();
						token.put(key, new ArrayList<String>(arrayOfValues));
						break;
					}
					case 3: {

						fileBinary.seek(offset);
						int p = (int) (fileBinary.readDouble());
						value = "NULL";
						offset = fileBinary.getFilePointer();
						token.put(key, new ArrayList<String>(arrayOfValues));

						break;
					}
					case 12:
						value = "NULL";

						break;
					case 4:
						fileBinary.seek(offset);
						value = Integer.toString(fileBinary.readUnsignedByte());
						offset = fileBinary.getFilePointer();
						break;
					case 5:
						fileBinary.seek(offset);
						value = (Integer.toString(fileBinary.readUnsignedShort()));
						offset = fileBinary.getFilePointer();
						break;
					case 6:
						fileBinary.seek(offset);
						value = (Integer.toString(fileBinary.readInt()));
						offset = fileBinary.getFilePointer();
						break;
					case 7:
						fileBinary.seek(offset);
						value = (Long.toString(fileBinary.readLong()));
						offset = fileBinary.getFilePointer();
						break;
					case 8:

						fileBinary.seek(offset);
						value = (Float.toString(fileBinary.readFloat()));
						offset = fileBinary.getFilePointer();
						break;
					case 9:

						fileBinary.seek(offset);
						value = (Double.toString(fileBinary.readDouble()));
						offset = fileBinary.getFilePointer();

						break;
					case 10: {
						fileBinary.seek(offset);

						long timeInEpoch = fileBinary.readLong();

						value = Long.toString(timeInEpoch);
						offset = fileBinary.getFilePointer();
						break;
					}
					case 11: {
						fileBinary.seek(offset);

						long timeInEpoch = fileBinary.readLong();
						value = Long.toString(timeInEpoch);

						// value = (Long.toString(binaryFile.readLong()));
						offset = fileBinary.getFilePointer();
						break;
					}
					default:
						byte[] text = new byte[b - 12];
						fileBinary.seek(offset);

						fileBinary.read(text);
						value = (new String(text));
						offset = fileBinary.getFilePointer();

						break;
				}

				if (currentColIndex == colIndex) {

					boolean aNull1 = value != null
							&& value.equalsIgnoreCase("null")
							&& value.equalsIgnoreCase(valueToBeSearched);
					boolean aNull = value == null && value == valueToBeSearched || aNull1;
					switch (searchDataType.trim().toLowerCase()) {

						case TINYINT:
							matchFound = isMatchFound(matchFound, valueToBeSearched, value);
							break;
						case SMALLINT:
							matchFound = isMatchFound(matchFound, valueToBeSearched, value);
							break;
						case INT:
							matchFound = isMatchFound(matchFound, valueToBeSearched, value);
							break;
						case BIGINT:
							if (aNull) {
								matchFound = true;
							} else if (value != null
									&& valueToBeSearched != null
									&& !value.equalsIgnoreCase("null")
									&& Long.parseLong(valueToBeSearched) == Long
									.parseLong(value)) {
								matchFound = true;
							}
							break;
						case REAL:
							if (aNull) {
								matchFound = true;
							} else if (value != null
									&& valueToBeSearched != null
									&& !value.equalsIgnoreCase("null")
									&& Float.parseFloat(valueToBeSearched) == Float
									.parseFloat(value)) {
								matchFound = true;
							}
							break;
						case DOUBLE:
							if (value == null && value == valueToBeSearched) {
								matchFound = true;
							} else if (aNull1) {
								matchFound = true;
							} else if (value != null
									&& valueToBeSearched != null
									&& !value.equalsIgnoreCase("null")
									&& Double.parseDouble(valueToBeSearched) == Double
									.parseDouble(value)) {
								matchFound = true;
							}
							break;
						case DATETIME:
							long epochSeconds = 0;

							if (aNull1) {
								matchFound = true;
								break;
							}

							if (value != null && !value.equalsIgnoreCase("null")) {
								SimpleDateFormat df = new SimpleDateFormat(
										"yyyy-MM-dd_HH:mm:ss");

								Date date;
								try {
									date = df.parse(valueToBeSearched);

									ZonedDateTime zdt = ZonedDateTime.ofInstant(
											date.toInstant(), zoneId);

									epochSeconds = zdt.toInstant().toEpochMilli() / 1000;
								} catch (Exception e) {
									System.out.println("Unexpected error in populate result datetime");

								}

							}

							if (value == null && value == valueToBeSearched) {
								matchFound = true;
							} else if (value != null && valueToBeSearched != null
									&& !value.equalsIgnoreCase("null")
									&& (epochSeconds) == Long.parseLong(value)) {

								Instant ii = Instant.ofEpochSecond(epochSeconds);
								ZonedDateTime zdt2 = ZonedDateTime.ofInstant(ii,
										zoneId);
								SimpleDateFormat sdf = new SimpleDateFormat(
										"yyyy-MM-dd_HH:mm:ss");
								Date date = Date.from(zdt2.toInstant());
								value = sdf.format(date);

								matchFound = true;
							}
							break;
						case DATE2:
							long epocSecs = 0;
							if (aNull1) {
								matchFound = true;
								break;
							}

							if (value != null && !value.equalsIgnoreCase("null")) {
								SimpleDateFormat df = new SimpleDateFormat(
										"yyyy-MM-dd");

								Date date;
								try {
									date = df.parse(valueToBeSearched);

									ZonedDateTime zdt = ZonedDateTime.ofInstant(
											date.toInstant(), zoneId);

									epocSecs = zdt.toInstant().toEpochMilli() / 1000;
								} catch (Exception e) {
									System.out.println("Unexpected error in populate result date 2");

								}

							}

							if (value == null && value == valueToBeSearched) {
								matchFound = true;
							} else if (value != null && valueToBeSearched != null
									&& !value.equalsIgnoreCase("null")
									&& (epocSecs) == Long.parseLong(value)) {

								Instant ii = Instant.ofEpochSecond(epocSecs);
								ZonedDateTime zdt2 = ZonedDateTime.ofInstant(ii,
										zoneId);
								SimpleDateFormat sdf = new SimpleDateFormat(
										"yyyy-MM-dd");
								Date date = Date.from(zdt2.toInstant());
								value = sdf.format(date);
								matchFound = true;
							}
							break;
						case TEXT:
							if (value == null && value == valueToBeSearched) {
								matchFound = true;
							} else if (value != null && valueToBeSearched != null
									&& valueToBeSearched.equalsIgnoreCase(value)) {
								matchFound = true;
							}

							break;
					}

					break;
				}
				currentColIndex++;
			}

			if (matchFound) {
				offsetForSerialType = offsetForSerialTypeMatch;
				offset = offSetForDataMatch;

				for (String key : token.keySet()) {

					if (i == 0) {
						arrayOfValues.add(actualRowID.toString());
						token.put(key, new ArrayList<String>(arrayOfValues));
						i++;
						arrayOfValues.clear();
						continue;
					}

					fileBinary.seek(offsetForSerialType);
					short b = fileBinary.readByte();
					offsetForSerialType = fileBinary.getFilePointer();
					switch (b) {
						case 0:

							offset = getOffset(token, arrayOfValues, offset, key);

							break;
						case 1:

							offset = getOffset1(token, arrayOfValues, offset, key);

							break;
						case 2:
							offset = getOffset3(token, arrayOfValues, offset, key);
							break;
						case 3:

							offset = getOffset4(token, arrayOfValues, offset, key);

							break;
						case 12:
							arrayOfValues.add("NULL");
							token.put(key, new ArrayList<String>(arrayOfValues));
							break;
						case 4:
							fileBinary.seek(offset);
							arrayOfValues.add(Integer.toString(fileBinary
									.readUnsignedByte()));
							offset = fileBinary.getFilePointer();
							token.put(key, new ArrayList<String>(arrayOfValues));
							break;
						case 5:
							fileBinary.seek(offset);
							arrayOfValues.add(Integer.toString(fileBinary
									.readUnsignedShort()));
							offset = fileBinary.getFilePointer();
							token.put(key, new ArrayList<String>(arrayOfValues));
							break;
						case 6:
							fileBinary.seek(offset);
							arrayOfValues
									.add(Integer.toString(fileBinary.readInt()));
							offset = fileBinary.getFilePointer();
							token.put(key, new ArrayList<String>(arrayOfValues));
							break;
						case 7:
							fileBinary.seek(offset);
							arrayOfValues.add(Long.toString(fileBinary.readLong()));
							offset = fileBinary.getFilePointer();
							token.put(key, new ArrayList<String>(arrayOfValues));
							break;
						case 8:

							fileBinary.seek(offset);
							arrayOfValues
									.add(Float.toString(fileBinary.readFloat()));
							offset = fileBinary.getFilePointer();
							token.put(key, new ArrayList<String>(arrayOfValues));
							break;
						case 9:

							fileBinary.seek(offset);
							arrayOfValues.add(Double.toString(fileBinary
									.readDouble()));
							offset = fileBinary.getFilePointer();
							token.put(key, new ArrayList<>(arrayOfValues));

							break;
						case 10: {
							fileBinary.seek(offset);

							Instant ii = Instant.ofEpochSecond(fileBinary
									.readLong());
							ZonedDateTime zdt2 = ZonedDateTime
									.ofInstant(ii, zoneId);
							offset = getOffset5(token, arrayOfValues, key, zdt2);
							break;
						}
						case 11: {
							fileBinary.seek(offset);
							// arrayOfValues.add(Long.toString(binaryFile.readLong()));

							Instant ii = Instant.ofEpochSecond(fileBinary
									.readLong());
							ZonedDateTime zdt2 = ZonedDateTime
									.ofInstant(ii, zoneId);
							offset = getOffset6(token, arrayOfValues, key, zdt2);
							break;
						}
						default:
							byte[] text = new byte[b - 12];
							fileBinary.seek(offset);

							fileBinary.read(text);
							arrayOfValues.add(new String(text));
							offset = fileBinary.getFilePointer();

							token.put(key, new ArrayList<>(arrayOfValues));

							break;
					}
					arrayOfValues.clear();
				}
			}

			if (!matchFound)
				token = null;

		} catch (Exception e) {
			System.out.println("Unexpected Error in populate result");
		}

		return token;
	}

	private long getOffset6(LinkedHashMap<String, ArrayList<String>> token, ArrayList<String> arrayOfValues, String key, ZonedDateTime zdt2) throws IOException {
		long offset;
		SimpleDateFormat sdf = new SimpleDateFormat(
				"yyyy-MM-dd");
		Date date = Date.from(zdt2.toInstant());

		arrayOfValues.add(sdf.format(date));

		offset = fileBinary.getFilePointer();
		token.put(key, new ArrayList<>(arrayOfValues));
		return offset;
	}

	private long getOffset5(LinkedHashMap<String, ArrayList<String>> token, ArrayList<String> arrayOfValues, String key, ZonedDateTime zdt2) throws IOException {
		long offset;
		SimpleDateFormat sdf = new SimpleDateFormat(
				"yyyy-MM-dd_HH:mm:ss");
		Date date = Date.from(zdt2.toInstant());
		arrayOfValues.add(sdf.format(date));

		offset = fileBinary.getFilePointer();
		token.put(key, new ArrayList<>(arrayOfValues));
		return offset;
	}

	private long getOffset4(LinkedHashMap<String, ArrayList<String>> token, ArrayList<String> arrayOfValues, long offset, String key) throws IOException {
		fileBinary.seek(offset);
		int p = (int) (fileBinary.readDouble());
		arrayOfValues.add("NULL");
		offset = fileBinary.getFilePointer();
		token.put(key, new ArrayList<>(arrayOfValues));
		return offset;
	}

	private long getOffset3(LinkedHashMap<String, ArrayList<String>> token, ArrayList<String> arrayOfValues, long offset, String key) throws IOException {
		fileBinary.seek(offset);
		int p = (fileBinary.readInt());
		arrayOfValues.add("NULL");
		offset = fileBinary.getFilePointer();
		token.put(key, new ArrayList<>(arrayOfValues));
		return offset;
	}

	private long getOffset1(LinkedHashMap<String, ArrayList<String>> token, ArrayList<String> arrayOfValues, long offset, String key) throws IOException {
		fileBinary.seek(offset);
		int p = (fileBinary.readUnsignedShort());
		arrayOfValues.add("NULL");
		offset = fileBinary.getFilePointer();
		token.put(key, new ArrayList<>(arrayOfValues));
		return offset;
	}

	private long getOffset(LinkedHashMap<String, ArrayList<String>> token, ArrayList<String> arrayOfValues, long offset, String key) throws IOException {
		fileBinary.seek(offset);
		int p = (fileBinary.readUnsignedByte());
		arrayOfValues.add("NULL");
		offset = fileBinary.getFilePointer();
		token.put(key, new ArrayList<>(arrayOfValues));
		return offset;
	}

	private boolean isMatchFound(boolean matchFound, String valueToBeSearched, String value) {
		if (value == null && value == valueToBeSearched) {
			matchFound = true;
		} else if (value != null
				&& value.equalsIgnoreCase("null")
				&& value.equalsIgnoreCase(valueToBeSearched)) {
			matchFound = true;
		} else if (value != null
				&& valueToBeSearched != null
				&& !value.equalsIgnoreCase("null")
				&& Integer.parseInt(valueToBeSearched) == Integer
				.parseInt(value)) {
			matchFound = true;
		}
		return matchFound;
	}


	/**
	 * FYI: Header format - https://sqlite.org/fileformat2.html
	 */
	private void writePageHeader(int pageLocation, boolean isLeaf,
								 int no_of_Cells, int rightPage) {
		int type = LEAF_NODE;
		int pointer = -1;
		if (!isLeaf) {
			type = INTERNAL_NODE;
			pointer = rightPage;
			no_of_Cells = 0;
		}
		try {
			fileBinary.seek((pageLocation - 1) * pageSize);

			fileBinary.write(type);
			NO_OF_CELLS_HEADER = fileBinary.getFilePointer();
			fileBinary.write(no_of_Cells);
			START_OF_CELL_HEADER = fileBinary.getFilePointer();
			fileBinary.writeShort((int) (pageLocation * pageSize));
			pageHeaderOffsetRightPagePointer = fileBinary.getFilePointer();
			fileBinary.writeInt(pointer);
			pageHeaderArrayOffset = fileBinary.getFilePointer();
			pageHeaderOffset = pageHeaderArrayOffset;
		} catch (Exception e) {
			System.out.println("Error in write page header: " + e.getMessage());
		}
	}

	private void readPageHeader(int pageLocation) {
		try {
			int currentPageIdx = currentPage - 1;
			fileBinary.seek(currentPageIdx * pageSize);

			int flag = fileBinary.readUnsignedByte();

			isLeafPage = flag == LEAF_NODE;

			NO_OF_CELLS_HEADER = (currentPageIdx * pageSize)
					+ NODE_TYPE_OFFSET;
			int noOfCells = fileBinary.readUnsignedByte();
			START_OF_CELL_HEADER = fileBinary.getFilePointer();
			fileBinary.readUnsignedShort();
			pageHeaderOffsetRightPagePointer = fileBinary.getFilePointer();
			fileBinary.readInt();
			pageHeaderArrayOffset = fileBinary.getFilePointer();
			pageHeaderOffset = fileBinary.getFilePointer() + (2 * noOfCells);

		} catch (Exception e) {
			System.out.println("Unexpected Error in read page header" + e.getMessage());

		}

	}

	public boolean close() {
		try {
			fileBinary.close();
			return true;
		} catch (IOException e) {
			System.out.println("Unexpected Error in close");
			return false;
		}
	}

	private void createPage(LinkedList<byte[]> pageCells) {
		try {
			fileBinary.setLength(pageSize * currentPage);
			writePageHeader(currentPage, true, pageCells.size(), -1);
			readPageHeader(currentPage);

			pageHeaderOffset = pageHeaderArrayOffset;
			ListIterator<byte[]> iterator = pageCells.listIterator(pageCells
					.size());

			long cellStartOffset = 0;

			fileBinary.seek(START_OF_CELL_HEADER);
			fileBinary.writeShort(currentPage * (pageSize));
			while (iterator.hasPrevious()) {
				byte[] s = iterator.previous();

				fileBinary.seek(START_OF_CELL_HEADER);

				cellStartOffset = ((long) fileBinary.readUnsignedShort())
						- (s.length);
				writeCellInBytes(currentPage, s, cellStartOffset);

			}
		} catch (Exception e) {
			System.out.println("Unexpected Error in create page");
		}
	}

	private void writeSrl(Map<String, ArrayList<String>> token)
			throws IOException {
		// n - bytes Serial code Types , one for each column
		for (String key : token.keySet()) {
			if (key.equals(tableKey))
				continue; // primary key not needed
			ArrayList<String> data_type = token.get(key);

			switch (data_type.get(0).trim().toLowerCase()) {

				case TINYINT:
					if (data_type.get(1) != null
							&& !data_type.get(1).trim().equalsIgnoreCase("null")) {
						fileBinary.write(4);
					} else {
						fileBinary.write(0);
					}
					break;
				case SMALLINT:
					if (data_type.get(1) != null
							&& !data_type.get(1).trim().equalsIgnoreCase("null")) {
						fileBinary.write(5);
					} else {
						fileBinary.write(1);
					}
					break;
				case INT:
					if (data_type.get(1) != null
							&& !data_type.get(1).trim().equalsIgnoreCase("null")) {
						fileBinary.write(6);
					} else {
						fileBinary.write(2);
					}
					break;
				case BIGINT:
					if (data_type.get(1) != null
							&& !data_type.get(1).trim().equalsIgnoreCase("null")) {
						fileBinary.write(7);
					} else {
						fileBinary.write(3);
					}
					break;
				case REAL:
					if (data_type.get(1) != null
							&& !data_type.get(1).trim().equalsIgnoreCase("null")) {
						fileBinary.write(8);
					} else {
						fileBinary.write(2);
					}
					break;
				case DOUBLE:
					if (data_type.get(1) != null
							&& !data_type.get(1).trim().equalsIgnoreCase("null")) {
						fileBinary.write(9);
					} else {
						fileBinary.write(3);
					}
					break;
				case DATETIME:
					if (data_type.get(1) != null
							&& !data_type.get(1).trim().equalsIgnoreCase("null")) {
						fileBinary.write(10);
					} else {
						fileBinary.write(3);
					}
					break;
				case DATE2:
					if (data_type.get(1) != null
							&& !data_type.get(1).trim().equalsIgnoreCase("null")) {
						fileBinary.write(11);
					} else {
						fileBinary.write(3);
					}
					break;
				case TEXT:
					if (data_type.get(1) != null
							&& !data_type.get(1).trim().equalsIgnoreCase("null")) {
						fileBinary.write(12 + (data_type.get(1).length()));
					} else {
						fileBinary.write(12);
					}
					break;

			}

		}
	}

	public String getPrimaryKey() {
		return tableKey;
	}

}
