package activitytracker;

import javax.sql.DataSource;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
//language=sql

public class ActivityDao {

    private DataSource dataSource;

    public ActivityDao(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public void saveActivity(Activity activity) {
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

    public Activity findActivityById(long id) {
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

    public List<Activity> listActivities() {
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

    public Activity saveActivityAndReturnGeneratedKeys(Activity activity) {

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement
                     ("insert into activities(start_time,description,activity_type) values (?,?,?)",
                             Statement.RETURN_GENERATED_KEYS)) {
            stmt.setTimestamp(1, Timestamp.valueOf(activity.getStartTime()));
            stmt.setString(2, activity.getDescription());
            stmt.setString(3, activity.getType().name());
            stmt.executeUpdate();
            ResultSet rs = stmt.getGeneratedKeys();
            if (rs.next()) {
                return new Activity(rs.getLong(1), activity.getStartTime(),
                        activity.getDescription(), activity.getType());
            }
            throw new IllegalStateException("Cannot get generated key");

        } catch (SQLException sqle) {
            throw new IllegalStateException("Cannot insert", sqle);
        }
    }

    public void saveActivityAndTrackPoints(Activity activity) {

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false);

            try {
                long acivityid = saveActivitywithTrackPoint(activity, conn);
                saveTrackPoints(acivityid, activity, conn);
                conn.commit();
            } catch (SQLException sqle) {
                conn.rollback();
                throw new IllegalArgumentException("Transaction not succeeded!", sqle);
            }
        } catch (SQLException sqle) {

            throw new IllegalStateException("Cannot insert", sqle);
        }
    }

    private long saveActivitywithTrackPoint(Activity activity, Connection conn) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement
                ("insert into activities(start_time,description,activity_type) values (?,?,?)",
                        Statement.RETURN_GENERATED_KEYS)) {
            stmt.setTimestamp(1, Timestamp.valueOf(activity.getStartTime()));
            stmt.setString(2, activity.getDescription());
            stmt.setString(3, activity.getType().name());
            stmt.executeUpdate();

            ResultSet rs = stmt.getGeneratedKeys();
            if (rs.next()) {
                return rs.getLong(1);
            }
            throw new IllegalStateException("Cannot get ID");
        }
    }

    private void saveTrackPoints(long activityId, Activity activity, Connection conn) throws SQLException {

        try (PreparedStatement stmt = conn.prepareStatement
                ("insert into track_point(id,tp_time,lat,lon) values(?,?,?,?)")) {

            for (TrackPoint actual : activity.getTrackpoints()) {
                if (actual.getLat() < -90.0 || actual.getLat() > 90.0 || actual.getLon() < -180 || actual.getLon() > 180.0) {
                    throw new IllegalArgumentException("Transaction not succeeded!");
                }
                stmt.setLong(1, activityId);
                stmt.setDate(2, Date.valueOf(actual.getTime()));
                stmt.setDouble(3, actual.getLat());
                stmt.setDouble(4, actual.getLon());
                stmt.executeUpdate();
            }
        }
    }

    public Activity findActivityWithTrackPointsById(long id) {

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement("select * from activities where id = ?")) {
            stmt.setLong(1, id);
            ResultSet rs = stmt.executeQuery();
            return getActivityWithTrackFromStatement(rs, conn);

        } catch (SQLException sqle) {
            throw new IllegalStateException("Cannot find ID", sqle);
        }
    }

    private Activity getActivityWithTrackFromStatement(ResultSet rs, Connection conn) throws SQLException {

        if (rs.next()) {
            long id = rs.getLong("id");
            LocalDateTime startTime = rs.getTimestamp("start_time").toLocalDateTime();
            String description = rs.getString("description");
            String type = rs.getString("activity_type");
            rs.close();
            List<TrackPoint> trackPoints = getTrackPoints(id, conn);

            return new Activity(id, startTime, description, Type.valueOf(type), trackPoints);
        }
        throw new IllegalArgumentException("No activity with this id.");
    }

    private List<TrackPoint> getTrackPoints(long id, Connection conn) throws SQLException {
        List<TrackPoint> result = new ArrayList<>();
        try (PreparedStatement stmt = conn.prepareStatement("select * from track_point where id = ?")) {

            stmt.setLong(1, id);
            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                LocalDate date = rs.getDate("tp_time").toLocalDate();
                double lat = rs.getDouble("lat");
                double lon = rs.getDouble("lon");
                result.add(new TrackPoint(date, lat, lon));
            }
        }
        return result;
    }
}
