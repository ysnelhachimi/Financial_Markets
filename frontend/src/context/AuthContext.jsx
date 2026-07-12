import { createContext, useContext, useEffect, useState, useCallback } from "react";
import { api, getToken, setToken } from "../api.js";

const AuthContext = createContext(null);

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null);
  const [subscription, setSubscription] = useState(null);
  const [loading, setLoading] = useState(true);

  const refresh = useCallback(async () => {
    if (!getToken()) {
      setUser(null);
      setSubscription(null);
      return;
    }
    try {
      const [me, sub] = await Promise.all([api.me(), api.subscription().catch(() => null)]);
      setUser(me);
      setSubscription(sub);
    } catch {
      setToken(null);
      setUser(null);
      setSubscription(null);
    }
  }, []);

  useEffect(() => {
    refresh().finally(() => setLoading(false));
  }, [refresh]);

  const login = async (email, password) => {
    const { access_token } = await api.login(email, password);
    setToken(access_token);
    await refresh();
  };

  const register = async (email, password, fullName) => {
    await api.register(email, password, fullName);
    await login(email, password);
  };

  const logout = () => {
    setToken(null);
    setUser(null);
    setSubscription(null);
  };

  const hasActiveSubscription =
    subscription && ["trialing", "active"].includes(subscription.status) &&
    new Date(subscription.current_period_end) > new Date();

  return (
    <AuthContext.Provider
      value={{ user, subscription, loading, login, register, logout, refresh, hasActiveSubscription }}
    >
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  return useContext(AuthContext);
}
