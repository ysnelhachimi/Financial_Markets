import React from "react";
import ReactDOM from "react-dom/client";
import { BrowserRouter, Routes, Route } from "react-router-dom";

import { AuthProvider } from "./context/AuthContext.jsx";
import App from "./App.jsx";
import Home from "./pages/Home.jsx";
import Login from "./pages/Login.jsx";
import Register from "./pages/Register.jsx";
import Dashboard from "./pages/Dashboard.jsx";
import Pricer from "./pages/Pricer.jsx";
import Portfolio from "./pages/Portfolio.jsx";
import Billing from "./pages/Billing.jsx";
import BillingResult from "./pages/BillingResult.jsx";
import ProtectedRoute from "./components/ProtectedRoute.jsx";
import "./styles.css";

ReactDOM.createRoot(document.getElementById("root")).render(
  <React.StrictMode>
    <BrowserRouter>
      <AuthProvider>
        <Routes>
          <Route path="/" element={<App />}>
            <Route index element={<Home />} />
            <Route path="login" element={<Login />} />
            <Route path="register" element={<Register />} />
            <Route path="billing" element={<ProtectedRoute><Billing /></ProtectedRoute>} />
            <Route path="billing/success" element={<BillingResult ok />} />
            <Route path="billing/failure" element={<BillingResult ok={false} />} />
            <Route
              path="dashboard"
              element={<ProtectedRoute><Dashboard /></ProtectedRoute>}
            />
            <Route path="pricer" element={<ProtectedRoute><Pricer /></ProtectedRoute>} />
            <Route path="portfolio" element={<ProtectedRoute><Portfolio /></ProtectedRoute>} />
          </Route>
        </Routes>
      </AuthProvider>
    </BrowserRouter>
  </React.StrictMode>
);
