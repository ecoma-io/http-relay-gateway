import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";

export default defineConfig({
  plugins: [vue()],
  server: {
    // Local development: proxy API calls to a running gateway's admin plane,
    // so `pnpm --filter web dev` needs no build and no CORS on either side.
    proxy: {
      "/api": "http://127.0.0.1:20131",
    },
  },
});
