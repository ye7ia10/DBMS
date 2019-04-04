package eg.edu.alexu.csd.oop.db.cs61;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;

import eg.edu.alexu.csd.oop.db.Command;
import eg.edu.alexu.csd.oop.db.Database;

public class IDatabase implements Database {
	private String dataBase = "";
	private Validation verify = new Validation();
	private Command objectCommand;
	private String path = "";
	private String tableName = "";
	private boolean inside = false;

	@Override
	public String createDatabase(String databaseName, boolean dropIfExists) {
		databaseName = databaseName.toLowerCase();
		File f = new File(databaseName);
		int index = databaseName.length() - 1;
		for (int i = databaseName.length() - 1; i >= 0; i--) {
			if (databaseName.charAt(i) == '\\') {
				index = i;
				break;
			}
		}
		dataBase = databaseName.substring(index + 1, databaseName.length());
		path = databaseName.substring(0, index + 1);
		if (dropIfExists && f.exists()) {
			System.out.println("hyms7777");
			objectCommand = new DropDB();
			objectCommand.setPath(path);
			objectCommand.setDataBase(dataBase);
			try {
				objectCommand.execute("DROP DATABASE " + dataBase);
			} catch (Exception e) {

			}
		}
		if (inside) {
			objectCommand = new CreateDB();
			System.out.println(dataBase + "   Name");
			System.out.println(path + dataBase + System.getProperty("file.separator") + "   Path");
			path = path + dataBase + System.getProperty("file.separator");
			Cache_Pool.getInstance().setDatabase(path);
			objectCommand.setPath(path);
			objectCommand.setDataBase("");
			try {
				objectCommand.execute("CREATE DATABASE " + "");
			} catch (Exception e) {

			}
		} else {
			objectCommand = new CreateDB();
			System.out.println(dataBase + "   Name");
			System.out.println(path + "   Path");
			Cache_Pool.getInstance().setDatabase(databaseName);
			objectCommand.setPath(path);
			objectCommand.setDataBase(dataBase);
			try {
				objectCommand.execute("CREATE DATABASE " + dataBase);
			} catch (Exception e) {

			}
		}
		return databaseName;
	}

	@Override
	public boolean executeStructureQuery(String query) throws SQLException {
		try {
			query = query.toLowerCase();
			objectCommand = verify.createVerify(query);
			System.out.println(query + "    QQQQQ");
			if (query.contains("create") && query.contains("database")) {
				System.out.println(query + "    NOOO");
				// path = "";
				String s = "";
				boolean flag = false;
				for (int i = query.length() - 1; i >= 0; i--) {
					if (flag) {
						flag = false;
						break;
					}
					if (query.charAt(i) == ' ') {
						continue;
					} else {
						for (int j = i; j >= 0; j--) {
							if (query.charAt(j) == ' ') {
								flag = true;
								break;
							}
							s = query.charAt(j) + s;
						}
					}
				}
				dataBase = s;
				if (inside) {
					Cache_Pool.getInstance().setDatabase(this.path + this.dataBase);
				} else {
					Cache_Pool.getInstance().setDatabase(s);
				}
			} else if (dataBase == "") {
				throw new SQLException();
			}
			objectCommand.setDataBase(dataBase);
			objectCommand.setPath(path);
			// not yet handled that return true always untill now
			return (boolean) objectCommand.execute(query);
		} catch (Exception e) {
			throw new SQLException();
		}
	}

	@Override
	public Object[][] executeQuery(String query) throws SQLException {
		// query = query.toLowerCase();
		if (dataBase == "") {
			throw new SQLException();
		}

		try {

			objectCommand = verify.selectVerify(query);
			objectCommand.setDataBase(dataBase);
			objectCommand.setPath(path);

			return (Object[][]) objectCommand.execute(query);

		} catch (Exception e) {
			throw new SQLException();
		}
	}

	@Override
	public int executeUpdateQuery(String query) throws SQLException {
		// query = query.toLowerCase();
		if (dataBase == "") {
			throw new SQLException();
		}
		try {

			objectCommand = verify.updateVerify(query);
			objectCommand.setDataBase(dataBase);
			objectCommand.setPath(path);
			return (int) objectCommand.execute(query);

		} catch (Exception e) {
			throw new SQLException();
		}
	}

	@Override
	public ArrayList<String> getMetaData() {
		// TODO Auto-generated method stub
		/*
		 * Table table = null; tableName = Select.getTableName(); try { table =
		 * Cache_Pool.getInstance().getTable(tableName); } catch (SQLException e) { //
		 * TODO Auto-generated catch block e.printStackTrace(); } System.out.println(
		 * "11111111111111111111111111111111111111111111111111111111111111111111111111")
		 * ; System.out.println(tableName); LinkedHashMap<String, String> hash = null;
		 * ArrayList<String>arr = new ArrayList<>(); if(table.getData().size() >= 1) {
		 * hash = table.getData().get(0); for (String key : hash.keySet()) {
		 * arr.add(key); } } Cache_Pool.getInstance().returnTable(table);
		 */
		return Select.getColumnName();
	}

	@Override
	public String getTableName() {
		// TODO Auto-generated method stub
		tableName = Select.getTableName();
		return tableName;
	}

	@Override
	public void save() {
		// TODO Auto-generated method stub
		Cache_Pool.getInstance().saveFiles();
	}

	@Override
	public void setInside(boolean inside) {
		this.inside = inside;
	}

}
