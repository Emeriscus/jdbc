package activitytracker;

import org.flywaydb.core.Flyway;
import org.mariadb.jdbc.MariaDbDataSource;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
//language=sql

public class ActivityTrackerMain {

    public void insertActivityToDatabase(DataSource dataSource, Activity activity) {

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement
                     ("insert into activities(start_time,description,activity_type) values (?,?,?)")) {
            stmt.setTimestamp(1, Timestamp.valueOf(activity.getStartTime()));
            stmt.setString(2, activity.getDescription());
            stmt.setString(3, activity.getType().name());
            stmt.executeUpdate();
        } catch (SQLException sqle) {
            throw new IllegalStateException("Cannot insert activity", sqle);
        }
    }

    public List<Activity> listAllActivities(DataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("select * from activities;")) {

            return ListActivities(rs);

        } catch (SQLException sqle) {
            throw new IllegalStateException("Cannot find data", sqle);
        }
    }

    private List<Activity> ListActivities(ResultSet rs) throws SQLException {
        List<Activity> result = new ArrayList<>();
        while (rs.next()) {
            long id = rs.getLong("id");
            LocalDateTime startTime = rs.getTimestamp("start_time").toLocalDateTime();
            String description = rs.getString("description");
            String type = rs.getString("activity_type");
            result.add(new Activity(id, startTime, description, Type.valueOf(type)));
        }
        return result;
    }

    public Activity getActivityById(DataSource dataSource, long id) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement("select * from activities where id = ?")) {
            stmt.setLong(1, id);
            ResultSet rs = stmt.executeQuery();
            return getActivityFromStatement(rs);
        } catch (SQLException sqle) {
            throw new IllegalStateException("Cannot find ID", sqle);
        }
    }

    private Activity getActivityFromStatement(ResultSet rs) throws SQLException {
        if (rs.next()) {
            long id = rs.getLong("id");
            LocalDateTime startTime = rs.getTimestamp("start_time").toLocalDateTime();
            String description = rs.getString("description");
            String type = rs.getString("activity_type");
            rs.close();
            return new Activity(id, startTime, description, Type.valueOf(type));

        }
        throw new IllegalArgumentException("Cannot find id");
    }

    public static void main(String[] args) {

        MariaDbDataSource dataSource = new MariaDbDataSource();

        try {
            dataSource.setUrl("jdbc:mariadb://localhost:3306/activitytracker?useUnicode=true");
            dataSource.setUser("activitytracker");
            dataSource.setPassword("activitytracker");
        } catch (SQLException se) {
            throw new IllegalStateException("Cannot connect to database.", se);
        }

        Flyway flyway = Flyway.configure().dataSource(dataSource).load();
        flyway.clean();
        flyway.migrate();

        ActivityTrackerMain activityTrackerMain = new ActivityTrackerMain();

        Activity activity1 = new Activity(LocalDateTime.of(2022, 1, 12, 13, 50),
                "Running in the wood", Type.RUNNING);
        Activity activity2 = new Activity(LocalDateTime.of(2022, 2, 15, 10, 40),
                "Basketball int the garden", Type.BASKETBALL);
        Activity activity3 = new Activity(LocalDateTime.of(2022, 2, 20, 6, 10),
                "Biking in the streets", Type.BIKING);

        activityTrackerMain.insertActivityToDatabase(dataSource, activity1);
        activityTrackerMain.insertActivityToDatabase(dataSource, activity2);
        activityTrackerMain.insertActivityToDatabase(dataSource, activity3);

        List<Activity> activities = activityTrackerMain.listAllActivities(dataSource);
        System.out.println(activities);

        for (Activity actual : activities) {
            System.out.println(activityTrackerMain.getActivityById(dataSource, actual.getId()));
        }

    }
}
