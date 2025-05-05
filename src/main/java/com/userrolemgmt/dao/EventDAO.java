package com.userrolemgmt.dao;

import com.userrolemgmt.util.DatabaseConnection;
import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class EventDAO {
    private static final Logger LOGGER = Logger.getLogger(EventDAO.class.getName());
    private final Connection connection;

    public EventDAO() {
        this.connection = DatabaseConnection.getInstance().getConnection();
    }

    /**
     * Registra un nuevo evento en el Event Store de manera simple
     * 
     * @param eventType Tipo de evento
     * @param subject   Asunto del evento
     * @param data      Datos del evento en formato JSON
     * @return ID del evento registrado
     */
    public long storeEvent(String eventType, String subject, String data) throws SQLException {
        // Consulta simple de inserción
        String query = "INSERT INTO event_store (event_type, subject, data) VALUES (?, ?, ?)";

        try (PreparedStatement pstmt = connection.prepareStatement(query, new String[] { "event_id" })) {
            pstmt.setString(1, eventType);
            pstmt.setString(2, subject);
            pstmt.setString(3, data);

            // Ejecutar la inserción
            int rowsAffected = pstmt.executeUpdate();

            if (rowsAffected > 0) {
                // Recuperar el ID generado
                try (ResultSet rs = pstmt.getGeneratedKeys()) {
                    if (rs.next()) {
                        long eventId = rs.getLong(1);
                        LOGGER.log(Level.INFO, "Evento {0} registrado con ID: {1}",
                                new Object[] { eventType, eventId });
                        return eventId;
                    }
                }
            }

            throw new SQLException("No se pudo obtener el ID generado");
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error al registrar evento " + eventType + ": " + e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Marca un evento como procesado de manera simple
     * 
     * @param eventId      ID del evento
     * @param success      Indica si el procesamiento fue exitoso
     * @param errorMessage Mensaje de error (si hubo)
     */
    public void markEventProcessed(long eventId, boolean success, String errorMessage) throws SQLException {
        // Consulta simple de actualización
        String query = "UPDATE event_store SET is_processed = ?, process_time = CURRENT_TIMESTAMP, " +
                "attempts = attempts + 1, error_msg = ? WHERE event_id = ?";

        try (PreparedStatement pstmt = connection.prepareStatement(query)) {
            pstmt.setString(1, success ? "Y" : "N");
            pstmt.setString(2, errorMessage);
            pstmt.setLong(3, eventId);

            int rowsUpdated = pstmt.executeUpdate();
            LOGGER.log(Level.INFO, "Evento ID: {0} marcado como {1}",
                    new Object[] { eventId, success ? "procesado" : "fallido" });
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error al actualizar estado de evento ID: " + eventId, e);
            throw e;
        }
    }

    /**
     * Recupera eventos no procesados para reintentos
     * 
     * @param maxAttempts Número máximo de intentos
     * @param limit       Límite de eventos a recuperar
     * @return ResultSet con los eventos
     */
    public ResultSet getUnprocessedEvents(int maxAttempts, int limit) throws SQLException {
        String query = "SELECT event_id, event_type, subject, data, attempts " +
                "FROM event_store WHERE is_processed = 'N' AND attempts < ? " +
                "ORDER BY event_time ASC FETCH FIRST ? ROWS ONLY";

        try {
            PreparedStatement pstmt = connection.prepareStatement(query);
            pstmt.setInt(1, maxAttempts);
            pstmt.setInt(2, limit);

            return pstmt.executeQuery();
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error al recuperar eventos no procesados", e);
            throw e;
        }
    }
}