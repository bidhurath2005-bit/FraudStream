import type { ReactNode } from "react";
import { Navigate, Route, Routes, useLocation } from "react-router-dom";
import { useAuth } from "./services/auth";
import { AppLayout } from "./layouts/AppLayout";
import { LoginPage } from "./pages/Login";
import { OverviewPage } from "./pages/Overview";
import { LiveStreamPage } from "./pages/LiveStream";
import { InvestigationCenterPage } from "./pages/Investigations";
import { AlertDetailPage, InvestigationDetailPage } from "./pages/Details";
import { SimulationLabPage } from "./pages/SimulationLab";
import { StreamingPage } from "./pages/Streaming";
import { ModelsPage } from "./pages/Models";
import { RiskAnalyticsPage } from "./pages/RiskAnalytics";
import { FraudMapPage } from "./pages/FraudMap";
import { ReportsPage } from "./pages/Reports";
import { SettingsPage } from "./pages/Settings";
import { EmptyState, Loading } from "./components/ui";

function RequireAuth({ children }: { children: ReactNode }) {
  const { user, ready } = useAuth();
  const loc = useLocation();
  if (!ready) return <Loading label="Restoring session" />;
  if (!user) return <Navigate to="/login" state={{ from: loc.pathname + loc.search }} replace />;
  return <>{children}</>;
}

export function App() {
  return (
    <Routes>
      <Route path="/login" element={<LoginPage />} />
      <Route element={<RequireAuth><AppLayout /></RequireAuth>}>
        <Route index element={<OverviewPage />} />
        <Route path="live" element={<LiveStreamPage />} />
        <Route path="investigations" element={<InvestigationCenterPage />} />
        <Route path="investigations/:uid" element={<InvestigationDetailPage />} />
        <Route path="alerts/:uid" element={<AlertDetailPage />} />
        <Route path="simulation" element={<SimulationLabPage />} />
        <Route path="streaming" element={<StreamingPage />} />
        <Route path="models" element={<ModelsPage />} />
        <Route path="risk" element={<RiskAnalyticsPage />} />
        <Route path="map" element={<FraudMapPage />} />
        <Route path="reports" element={<ReportsPage />} />
        <Route path="settings" element={<SettingsPage />} />
        <Route path="*" element={<EmptyState title="Page not found" />} />
      </Route>
    </Routes>
  );
}
