package com.ba.boost;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public class App {

	private final String user = "postgres";
	private final String password = "root";
	private final String url = "jdbc:postgresql://localhost:5432/dvdrental";

	public Connection connect() {

		Connection conn = null;
		try {
			conn = DriverManager.getConnection(url, user, password);
			System.out.println("Connected to PostgreSQL server.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	public String getActorCount() {
		int count = 0;
		String sql = "SELECT COUNT(*) AS toplam FROM actor;";
		try (Connection conn = connect();
				Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(sql);) { // try with resources, kendi kendine bu şekilde yazılırsa. //
															// close işlemini yapar.
			rs.next();
			count = rs.getInt("toplam");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return "Toplam actor sayisi: " + count;
	}

	public void getActor() {
		String sql = "SELECT actor_id, first_name, last_name FROM actor;";
		try (Connection conn = connect();
				Statement stmt = conn.createStatement();
				ResultSet rs = stmt.executeQuery(sql);) { // try with resources, kendi kendine bu şekilde yazılırsa
															// close işlemini yapar.
			while (rs.next()) {
				System.out.print(rs.getString("actor_id") + " - " + rs.getString("first_name") + " "
						+ rs.getString("last_name"));
				System.out.println();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public void findActorById(int id) {
		String sql = "SELECT actor_id, first_name, last_name FROM actor WHERE actor_id = ?;";
		try (Connection conn = connect(); PreparedStatement pstmt = conn.prepareStatement(sql);) {
			pstmt.setInt(1, id);
			ResultSet rs = pstmt.executeQuery();
			rs.next();
			System.out.print(rs.getString(1) + " - " + rs.getString(2) + " " + rs.getString(3));
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int insertActor(Actor actor) {
		int id = 0;
		String sql = "INSERT INTO actor(first_name, last_name) VALUES (?, ?);";

		try (Connection conn = connect();
				PreparedStatement pstmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);) {

			pstmt.setString(1, actor.getFirstName());
			pstmt.setString(2, actor.getLastName());

			int resultRow = pstmt.executeUpdate();

			if (resultRow > 0) {
				try (ResultSet rs = pstmt.getGeneratedKeys()) {
					if (rs.next()) {
						id = rs.getInt(1);
					}
				}
			}

			System.out.print("Etkilenen satir sayisi: " + resultRow + ", id: ");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return id;
	}

	public void insertActors(List<Actor> lists) {
		String sql = "INSERT INTO actor(first_name, last_name) VALUES (?, ?);";

		try (Connection conn = connect(); PreparedStatement pstmt = conn.prepareStatement(sql);) {
			int count = 0;

			for (Actor a : lists) {
				pstmt.setString(1, a.getFirstName());
				pstmt.setString(2, a.getLastName());

				pstmt.addBatch();
				count++;
				if (count % 100 == 0 || count == lists.size()) {
					pstmt.executeBatch();
				}
			}

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public void deleteActor(int id) {
		String sql = "DELETE FROM actor WHERE actor.actor_id = ?;";

		try (Connection conn = connect(); PreparedStatement pstmt = conn.prepareStatement(sql);) {
			pstmt.setInt(1, id);
			int rs = pstmt.executeUpdate();
			System.out.println("Affected row is " + rs);

		} catch (SQLException e) {
			e.printStackTrace();
		}

	}

	public void updateLastName(int id, String lastName) {
		String sql = "UPDATE actor SET last_name=? WHERE actor.actor_id = ?;";

		try (Connection conn = connect(); PreparedStatement pstmt = conn.prepareStatement(sql);) {
			pstmt.setString(1, lastName);
			pstmt.setInt(2, id);
			int rs = pstmt.executeUpdate();
			System.out.println("Affected row is " + rs);

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {

		App app = new App();
//		app.connect();
//		System.out.println(app.getActorCount());
//		app.getActor();
//		app.findActorById(100);

//		LocalDateTime a = LocalDateTime.now();
//		DateTimeFormatter f = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SS");
//		System.out.println(a.format(f));

//		Actor actor = new Actor("Cristiano","Ronaldo");
//		System.out.println(app.insertActor(actor));
//		app.findActorById(206);

//		List<Actor> actorLists = new ArrayList<>();
//		actorLists.add(new Actor("Tarik", "Akan"));
//		actorLists.add(new Actor("Brad", "Pitt"));
//		
//		app.insertActors(actorLists);

//		app.getActor();
//		app.deleteActor(206);
//		app.getActor();
//		app.deleteActor(205);
//		app.updateLastName(203, "Atakan");
		app.getActor();
	}

}
