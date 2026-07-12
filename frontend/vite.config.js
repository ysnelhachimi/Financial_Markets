import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// En développement, on proxifie les appels /api vers le backend FastAPI
// (évite les soucis de CORS et garde des URLs relatives dans le frontend).
export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://localhost:8000",
        changeOrigin: true,
      },
    },
  },
});
