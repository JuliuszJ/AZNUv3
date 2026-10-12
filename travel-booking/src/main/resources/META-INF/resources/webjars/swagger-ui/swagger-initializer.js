window.onload = function () {
    window.ui = SwaggerUIBundle({
        url: "/api/api-doc",
        dom_id: "#swagger-ui",
        deepLinking: true,
        queryConfigEnabled: true,
        validatorUrl: null,
        presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],
        layout: "StandaloneLayout"
    });
};
