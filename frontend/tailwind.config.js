/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        ink: { 950: "#07090f", 900: "#0b0f19", 850: "#0f1422", 800: "#141a2b", 700: "#1c2438", 600: "#273049", 500: "#3a4560" },
        mist: { 100: "#e6ebf5", 200: "#c4cddf", 300: "#94a0b8", 400: "#6b7790" },
        stream: { 400: "#3fe0f0", 500: "#18c8dc", 600: "#0ea5b7" },
        amber: { 400: "#fbbf24", 500: "#f59e0b" },
        coral: { 400: "#ff7a6b", 500: "#f2554a" },
        emerald: { 400: "#34d399", 500: "#10b981" },
      },
      fontFamily: {
        sans: ["'IBM Plex Sans'", "ui-sans-serif", "system-ui", "Segoe UI", "sans-serif"],
        mono: ["'JetBrains Mono'", "ui-monospace", "Consolas", "monospace"],
      },
      boxShadow: { panel: "0 1px 0 0 rgba(255,255,255,0.03) inset, 0 8px 24px -12px rgba(0,0,0,0.6)" },
    },
  },
  plugins: [],
};
