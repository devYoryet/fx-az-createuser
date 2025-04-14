package com.userrolemgmt;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.functions.*;
import com.microsoft.azure.functions.annotation.*;
import com.userrolemgmt.dao.UserDAO;
import com.userrolemgmt.model.User;

import java.util.Optional;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CreateUserFunction {
    private static final Logger LOGGER = Logger.getLogger(CreateUserFunction.class.getName());
    private final Gson gson = new GsonBuilder().setDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ").create();
    private final UserDAO userDAO = new UserDAO();

    @FunctionName("createUser")
    public HttpResponseMessage run(
            @HttpTrigger(name = "req", methods = {
                    HttpMethod.POST }, authLevel = AuthorizationLevel.ANONYMOUS, route = "users") HttpRequestMessage<Optional<String>> request,
            final ExecutionContext context) {

        context.getLogger().info("Solicitud recibida para crear un nuevo usuario");

        String requestBody = request.getBody().orElse("");
        if (requestBody.isEmpty()) {
            return request.createResponseBuilder(HttpStatus.BAD_REQUEST)
                    .body("Por favor proporcione datos de usuario en el cuerpo de la solicitud")
                    .build();
        }

        try {
            User user = gson.fromJson(requestBody, User.class);
            User createdUser = userDAO.createUser(user);

            return request.createResponseBuilder(HttpStatus.CREATED)
                    .header("Content-Type", "application/json")
                    .body(gson.toJson(createdUser))
                    .build();
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Error al crear usuario", e);
            return request.createResponseBuilder(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Error al crear usuario: " + e.getMessage())
                    .build();
        }
    }
}