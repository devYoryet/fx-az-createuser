package com.userrolemgmt.dao;

import com.userrolemgmt.model.Role;
import com.userrolemgmt.model.User;
import com.userrolemgmt.util.DatabaseConnection;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class UserDAO {
    private static final Logger LOGGER = Logger.getLogger(UserDAO.class.getName());
    private final Connection connection;

    public UserDAO() {
        this.connection = DatabaseConnection.getInstance().getConnection();
    }

    // Crear un nuevo usuario
    public User createUser(User user) throws SQLException {
        // Enfoque Oracle mejorado
        String oracleQuery = "BEGIN " +
                "  INSERT INTO users (username, email, password_hash, first_name, last_name, active) " +
                "  VALUES (?, ?, ?, ?, ?, ?) " +
                "  RETURNING user_id, created_at, updated_at INTO ?, ?, ?; " +
                "END;";

        try (CallableStatement cstmt = connection.prepareCall(oracleQuery)) {
            // Parámetros de entrada
            cstmt.setString(1, user.getUsername());
            cstmt.setString(2, user.getEmail());
            cstmt.setString(3, user.getPasswordHash());
            cstmt.setString(4, user.getFirstName());
            cstmt.setString(5, user.getLastName());
            cstmt.setString(6, user.isActive() ? "Y" : "N"); // Usar String para CHAR(1)

            // Registrar parámetros de salida
            cstmt.registerOutParameter(7, Types.NUMERIC); // user_id
            cstmt.registerOutParameter(8, Types.TIMESTAMP); // created_at
            cstmt.registerOutParameter(9, Types.TIMESTAMP); // updated_at

            cstmt.execute();

            // Obtener valores devueltos
            user.setUserId(cstmt.getLong(7));
            user.setCreatedAt(cstmt.getTimestamp(8));
            user.setUpdatedAt(cstmt.getTimestamp(9));

            // Asignar roles
            if (user.getRoles() != null && !user.getRoles().isEmpty()) {
                for (Role role : user.getRoles()) {
                    assignRoleToUser(user.getUserId(), role.getRoleId());
                }
            }

            return user;

        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error al crear usuario", e);
            throw e;
        }
    }

    // Asignar un rol a un usuario
    public boolean assignRoleToUser(long userId, long roleId) throws SQLException {
        String query = "INSERT INTO user_roles (user_id, role_id) VALUES (?, ?)";

        try (PreparedStatement pstmt = connection.prepareStatement(query)) {
            pstmt.setLong(1, userId);
            pstmt.setLong(2, roleId);

            int rowsAffected = pstmt.executeUpdate();
            return rowsAffected > 0;
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error al asignar rol ID: " + roleId + " a usuario ID: " + userId, e);
            throw e;
        }
    }
}