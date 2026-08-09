import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  // Relative base so the built site works from a subpath (GitHub Pages project
  // sites) as well as a domain root, without a rebuild.
  base: "./",
  build: { outDir: "dist", sourcemap: false, chunkSizeWarningLimit: 400 },
  server: { port: 5173, open: false },
});
