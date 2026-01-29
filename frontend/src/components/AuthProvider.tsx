import Keycloak from "keycloak-js";
import React, { createContext, useContext, useEffect, useState, ReactNode, useMemo } from "react";

export interface IAuthContextProps {
  keycloak: Keycloak;
  isAuthenticated: boolean;
  login: () => void;
  logout: () => void;
  refreshToken: () => Promise<void>;
}

const AuthContext = createContext<IAuthContextProps | undefined>(undefined);

const keycloakConfig = {
  url: process.env.REACT_APP_KEYCLOAK_URL,
  realm: process.env.REACT_APP_KEYCLOAK_REALM||"",
  clientId: process.env.REACT_APP_KEYCLOAK_CLIENT_ID||""
};

export const AuthProvider: React.FC<{ children: ReactNode }> = ({ children }) => {
  const [isAuthenticated, setAuthenticated] = useState(false);
  const keycloak = useMemo(() => new Keycloak(keycloakConfig), [])
  const UPDATE_TOKEN_TIME = 30;

  useEffect(() => {
    keycloak
      .init({ checkLoginIframe: false, pkceMethod: 'S256'})
      .then((authenticated) => setAuthenticated(authenticated))
      .catch((error) => console.error("Keycloak initialization failed:", error));

    keycloak.onTokenExpired = () => {
      keycloak.updateToken(UPDATE_TOKEN_TIME).catch(() => keycloak.logout());
    };
  }, [keycloak]);

  const login = () => keycloak.login();
  const logout = () => keycloak.logout();
  const refreshToken = async () => {
    await keycloak.updateToken(UPDATE_TOKEN_TIME);
  };

  return (
    <AuthContext.Provider value={{ keycloak, isAuthenticated, login, logout, refreshToken }}>
        {children}
    </AuthContext.Provider>
  );
};

export const useAuth = (): IAuthContextProps => {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
};
