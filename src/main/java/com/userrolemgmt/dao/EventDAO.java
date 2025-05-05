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
     * Registra un nuevo evento en el Event Store
     * 
     * @param eventType Tipo de evento (ej. UserCreated, RoleCreated)
     * @param subject   Asunto del evento (ej. users/create)
     * @param data      Datos del evento en formato JSON
     * @return ID del evento registrado
     */
    public long storeEvent(String eventType, String subject, String data) throws SQLException {
        String query = "INSERT INTO event_store (event_type, subject, data) VALUES (?, ?, ?) RETURNING event_id INTO ?";

        try (CallableStatement cstmt = connection.prepareCall(query)) {
            cstmt.setString(1, eventType);
            cstmt.setString(2, subject);
            cstmt.setString(3, data);
            cstmt.registerOutParameter(4, Types.NUMERIC); // event_id

            cstmt.execute();

            // Obtener el ID del evento
            long eventId = cstmt.getLong(4);
            LOGGER.log(Level.INFO, "Evento {0} registrado con ID: {1}", new Object[] { eventType, eventId });

            return eventId;
        } catch (SQLException e) {
            LOGGER.log(Level.SEVERE, "Error al registrar evento " + eventType, e);
            throw e;
        }
    }

    /**
     * Marca un evento como procesado
     * 
     * @param eventId      ID del evento
     * @param success      Indica si el procesamiento fue exitoso
     * @param errorMessage Mensaje de error (si hubo)
     */
    public void markEventProcessed(long eventId, boolean success, String errorMessage) throws SQLException {
        String query = "UPDATE event_store SET processed = ?, process_time = CURRENT_TIMESTAMP, " +
                "processing_attempts = processing_attempts + 1, error_message = ? WHERE event_id = ?";

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
        String query = "SELECT event_id, event_type, subject, data, processing_attempts " +
                "FROM event_store WHERE processed = 'N' AND processing_attempts < ? " +
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