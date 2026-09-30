import React from "react";
import ReactDOM from "react-dom/client";
import App from "./App";
import { startNet } from "./ws";
import "./styles.css";

startNet();

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
);
