import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from "react";
import { api, refreshSession, setAccessToken, setSessionExpiredHandler } from "./api";
import type { User } from "../types/api";

type AuthState = {
  user: User | null;
  ready: boolean;
  expired: boolean;
  login: (username: string, password: string) => Promise<void>;
  logout: () => Promise<void>;
  isAdmin: boolean;
};

const AuthContext = createContext<AuthState | null>(null);

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [ready, setReady] = useState(false);
  const [expired, setExpired] = useState(false);

  useEffect(() => {
    setSessionExpiredHandler(() => {
      setUser(null);
      setExpired(true);
    });
    // restore session from the httpOnly refresh cookie (if any)
    refreshSession()
      .then((b) => b && setUser(b.user as User))
      .catch(() => undefined)
      .finally(() => setReady(true));
    return () => setSessionExpiredHandler(null);
  }, []);

  const login = useCallback(async (username: string, password: string) => {
    const body = await api<{ access_token: string; user: User }>("/auth/login", { body: { username, password } });
    setAccessToken(body.access_token);
    setExpired(false);
    setUser(body.user);
  }, []);

  const logout = useCallback(async () => {
    try {
      await api("/auth/logout", { method: "POST" });
    } finally {
      setAccessToken(null);
      setUser(null);
    }
  }, []);

  const value = useMemo(
    () => ({ user, ready, expired, login, logout, isAdmin: user?.role === "ADMIN" }),
    [user, ready, expired, login, logout],
  );
  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>;
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth outside AuthProvider");
  return ctx;
}
