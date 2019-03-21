package dal;

import dal.dto.IUserDTO;
import dal.dto.UserDTO;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDAOImplS133967 implements IUserDAO {
    private Connection createConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://ec2-52-30-211-3.eu-west-1.compute.amazonaws.com/s133967?"
                + "user=s133967&password=8JPOJuQcgUpUVIVHY4S2H");
    }

    @Override
    public void createUser(IUserDTO user) throws DALException {
        String sql = "INSERT INTO user (id, name, ini)" +
                "VALUES ( ? , ? , ? )";
        updateIndhold(sql,
                user.getUserId(), user.getUserName(), user.getIni());

        sql = "INSERT INTO role (name)" +
                "VALUES ( ? )";
        for(String r : extractNewRoles( queryAllRoles(), user.getRoles() )) {
            updateIndhold(sql, r);
        }

        sql = "INSERT INTO role_user (user_id, user_role)" +
                "VALUES ( ? , ? )";
        for (String role : user.getRoles()){
            updateIndhold(sql, user.getUserId(), role.trim());
        }
    }

    @Override
    public IUserDTO getUser(int userId) throws DALException {
        UserDTO user = new UserDTO();

        String sql = "SELECT user.id AS id, user.name AS name, user.ini AS ini" +
                " FROM user" +
                " WHERE user.id = ?";
        queryUserWithoutRoles(sql, user, userId);

        sql = "SELECT role_user.user_role AS role" +
                " FROM role_user" +
                " WHERE role_user.user_id = ?";
        queryUserRoles(sql, user);
        return user;
    }

    @Override
    public List<IUserDTO> getUserList() throws DALException {
        String sql = "SELECT user.id AS id" +
                " FROM user";
        List<Integer> ids = queryUserIds(sql);
        List<IUserDTO> users = new ArrayList<>();
        for(Integer id : ids){
            users.add( getUser(id) );
        }
        return users;
    }

    @Override
    public void updateUser(IUserDTO user) throws DALException {
        String sql = "UPDATE user" +
                " SET name = ?" +
                " WHERE id = ?";
        updateIndhold(sql, user.getUserName(), user.getUserId());

        sql = "UPDATE user" +
                " SET ini = ?" +
                " WHERE id = ?";
        updateIndhold(sql, user.getIni(), user.getUserId());

        sql = "DELETE FROM role_user WHERE user_id = ?";
        updateIndhold(sql, user.getUserId());

        sql = "INSERT INTO role (name)" +
                "VALUES ( ? )";
        for(String r : extractNewRoles( queryAllRoles(), user.getRoles() )) {
            updateIndhold(sql, r);
        }

        sql = "INSERT INTO role_user (user_id, user_role)" +
                "VALUES ( ? , ? )";
        for (String role : user.getRoles()){
            updateIndhold(sql, user.getUserId(), role);
        }
    }

    @Override
    public void deleteUser(int userId) throws DALException {
        String sql = "DELETE FROM role_user WHERE user_id = ?";
        updateIndhold(sql, userId);

        sql = "DELETE FROM user WHERE id = ?";
        updateIndhold(sql, userId);
    }
    private void updateIndhold(String sql, int param01) throws DALException {
        try( Connection conn = createConnection(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, param01);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DALException(e.getMessage());
        }
    }
    private void updateIndhold(String sql, String param01, int param02) throws DALException {
        try( Connection conn = createConnection(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, param01);
            pstmt.setInt(2, param02);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DALException(e.getMessage());
        }
    }
    //
    private List<Integer> queryUserIds(String sql) throws DALException {
        List<Integer> ids = new ArrayList<>();
        try( Connection conn = createConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)){
            while(rs.next()){
                int id  = rs.getInt("id");
                ids.add(id);
            }
        } catch (SQLException e) {
            throw new DALException(e.getMessage());
        }
        return ids;
    }
    //
    private UserDTO queryUserRoles(String sql, UserDTO userDTO) throws DALException {
        try(Connection conn = createConnection(); PreparedStatement pstmt = conn.prepareStatement(sql)){
            pstmt.setInt(1, userDTO.getUserId());
            try(ResultSet rs = pstmt.executeQuery()){
                List<String> roles = new ArrayList<>();
                while(rs.next()){
                    String role = rs.getString("role");
                    roles.add(role);
                }
                userDTO.setRoles(roles);
            } catch (SQLException e) {
                throw new DALException(e.getMessage());
            }
        } catch (SQLException e) {
            throw new DALException(e.getMessage());
        }
        return userDTO;
    }
    private UserDTO queryUserWithoutRoles(String sql, UserDTO userDTO, int userId) throws DALException {
        try( Connection conn = createConnection(); PreparedStatement pstmt = conn.prepareStatement(sql)){
            pstmt.setInt(1, userId);

            ResultSet rs = pstmt.executeQuery();
            while(rs.next()){
                int id  = rs.getInt("id");
                String name = rs.getString("name");
                String ini = rs.getString("ini");

                userDTO.setUserId(id);
                userDTO.setUserName(name);
                userDTO.setIni(ini);
            }
        } catch (SQLException e) {
            throw new DALException(e.getMessage());
        }
        return userDTO;
    }
    //
    private void updateIndhold(String sql, String param01) throws DALException {
        try( Connection conn = createConnection(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, param01);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DALException(e.getMessage());
        }
    }
    private List<String> queryAllRoles() throws DALException {
        String sql = "SELECT name FROM role";
        return queryStringParams(sql, "name");
    }
    private List<String> queryStringParams(String sql, String key) throws DALException {
        List<String> params = new ArrayList<>();
        try(Connection conn = createConnection(); Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(sql)) {
            while(rs.next()){
                String param = rs.getString(key);
                params.add(param);
            }
        } catch (SQLException e) {
            throw new DALException(e.getMessage());
        }
        return params;
    }
    private void updateIndhold(String sql, int param01, String param02) throws DALException {
        try(Connection conn = createConnection(); PreparedStatement pstmt = conn.prepareStatement(sql)){
            pstmt.setInt(1, param01);
            pstmt.setString(2, param02);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new DALException(e.getMessage());
        }
    }
    private List<String> extractNewRoles(List<String> rolesInTheDB, List<String> userRoles) {
        List<String> newRoles = new ArrayList<>();
        for (String r : userRoles){
            if(!rolesInTheDB.contains(r))
                newRoles.add(r.trim());
        }
        return newRoles;
    }
    private void updateIndhold(String sql, int param01, String param02, String param03) throws DALException {
        try( Connection conn = createConnection(); PreparedStatement pstmt = conn.prepareStatement(sql) ){
            pstmt.setInt(1, param01);
            pstmt.setString(2, param02);
            pstmt.setString(3, param03);
            pstmt.executeUpdate();

        } catch (SQLException e) {
            throw new DALException(e.getMessage());
        }
    }
}
