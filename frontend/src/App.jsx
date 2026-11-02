import { Routes, Route, NavLink, useLocation } from 'react-router-dom';
import { useEffect, useState } from 'react';
import { api } from './api';

import Admin from './pages/Admin';
import Dashboard from './pages/Dashboard';
import Countries from './pages/Countries';
import RoaLookup from './pages/RoaLookup';
import RoaByAsn from './pages/RoaByAsn';
import Aspa from './pages/Aspa';
import CaExplorer from './pages/CaExplorer';
import Alerts from './pages/Alerts';
import Coverage from './pages/Coverage';
import OrgCoverage from './pages/OrgCoverage';
import OrgLookup from './pages/OrgLookup';
import ValidityObjects from './pages/ValidityObjects';

const navSections = [
  {
    title: 'Overview',
    items: [{ to: '/', label: 'Dashboard' }],
  },
  {
    title: 'Analysis',
    items: [{ to: '/analysis/countries', label: 'Countries' }],
  },
  {
    title: 'ROA Lookup',
    items: [
      { to: '/roa/lookup', label: 'Prefix Lookup' },
      { to: '/roa/by-asn', label: 'By ASN' },
      { to: '/roa/org', label: 'Org Lookup' },
      { to: '/roa/coverage', label: 'ASN Coverage' },
      { to: '/roa/org-coverage', label: 'Org Coverage' },
    ],
  },
  {
    title: 'ASPA',
    items: [{ to: '/aspa', label: 'ASPA Observatory' }],
  },
  {
    title: 'CA Hierarchy',
    items: [{ to: '/ca', label: 'CA Explorer' }],
  },
  {
    title: 'Validity',
    items: [{ to: '/validity', label: 'RP-Invalid Objects' }],
  },
  {
    title: 'Alerts',
    items: [{ to: '/alerts', label: 'Recent Changes' }],
  },
];

function HealthDot() {
  const [health, setHealth] = useState(null);

  useEffect(() => {
    const check = () => api.health().then(setHealth).catch(() => setHealth(null));
    check();
    const id = setInterval(check, 300_000); // 5 min
    return () => clearInterval(id);
  }, []);

  if (!health) {
    return (
      <div className="flex items-center gap-2 text-xs text-cf-text-muted">
        <span className="w-2 h-2 rounded-full bg-gray-300" />
        Checking...
      </div>
    );
  }

  const ok = health.status === 'ok';
  return (
    <div className="flex items-center gap-2 text-xs text-cf-text-muted">
      <span className={`w-2 h-2 rounded-full ${ok ? 'bg-emerald-500' : 'bg-red-500'}`} />
      {ok ? `${(health.live_vrp_total || 0).toLocaleString()} live VRPs` : 'API error'}
    </div>
  );
}

function Sidebar({ open, onClose }) {
  return (
    <>
      {/* Overlay for mobile */}
      {open && (
        <div className="fixed inset-0 bg-black/30 z-30 lg:hidden" onClick={onClose} />
      )}
      <aside className={`
        w-56 h-screen fixed left-0 top-0 bg-cf-surface border-r border-cf-border flex flex-col z-40
        transition-transform duration-200 ease-out
        ${open ? 'translate-x-0' : '-translate-x-full'}
        lg:translate-x-0
      `}>
        {/* Logo + close button */}
        <div className="px-5 py-5 border-b border-cf-border flex items-center justify-between">
          <div className="flex items-center gap-2.5">
            <img src="/favicon.svg" className="w-7 h-7" alt="RPKI" />
            <div>
              <div className="text-sm font-bold text-cf-text">RPKI Warehouse</div>
              <div className="text-[10px] text-cf-text-muted">netsecurelab.org</div>
            </div>
          </div>
          <button onClick={onClose} className="lg:hidden text-cf-text-muted hover:text-cf-text text-lg p-1">
            &#10005;
          </button>
        </div>

        {/* Nav */}
        <nav className="flex-1 overflow-y-auto py-3 px-3">
          {navSections.map((section) => (
            <div key={section.title} className="mb-4">
              <div className="text-[11px] font-semibold uppercase tracking-widest text-cf-text-muted px-2 mb-1.5">
                {section.title}
              </div>
              {section.items.map((item) => (
                <NavLink
                  key={item.to}
                  to={item.to}
                  end={item.to === '/'}
                  onClick={onClose}
                  className={({ isActive }) =>
                    `block px-3 py-1.5 rounded-r-md text-sm transition-colors mb-0.5 ${
                      isActive
                        ? 'bg-cf-orange-light text-cf-orange border-l-2 border-cf-orange'
                        : 'text-cf-text-secondary hover:bg-cf-surface-2 hover:text-cf-text border-l-2 border-transparent'
                    }`
                  }
                >
                  {item.label}
                </NavLink>
              ))}
            </div>
          ))}
        </nav>

        {/* Footer — health */}
        <div className="px-5 py-4 border-t border-cf-border/50">
          <HealthDot />
        </div>
      </aside>
    </>
  );
}

export default function App() {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const location = useLocation();

  // Admin dashboard is standalone (own token gate, no sidebar)
  if (location.pathname === '/admin') {
    return (
      <Routes>
        <Route path="/admin" element={<Admin />} />
      </Routes>
    );
  }

  return (
    <div className="flex">
      <Sidebar open={sidebarOpen} onClose={() => setSidebarOpen(false)} />

      {/* Mobile header bar */}
      <div className="fixed top-0 left-0 right-0 h-14 bg-cf-surface border-b border-cf-border flex items-center px-4 gap-3 z-20 lg:hidden">
        <button onClick={() => setSidebarOpen(true)} className="text-cf-text p-1">
          <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round"><line x1="3" y1="6" x2="21" y2="6"/><line x1="3" y1="12" x2="21" y2="12"/><line x1="3" y1="18" x2="21" y2="18"/></svg>
        </button>
        <img src="/favicon.svg" className="w-6 h-6" alt="RPKI" />
        <span className="text-sm font-bold text-cf-text">RPKI Warehouse</span>
      </div>

      <main className="lg:ml-56 flex-1 min-w-0 min-h-screen p-4 pt-18 lg:p-8 lg:pt-8 max-w-[1200px] flex flex-col">
        <div className="flex-1 min-w-0">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/analysis/countries" element={<Countries />} />
            <Route path="/roa/lookup" element={<RoaLookup />} />
            <Route path="/roa/by-asn" element={<RoaByAsn />} />
            <Route path="/roa/org" element={<OrgLookup />} />
            <Route path="/roa/coverage" element={<Coverage />} />
            <Route path="/roa/org-coverage" element={<OrgCoverage />} />
            <Route path="/aspa" element={<Aspa />} />
            <Route path="/ca" element={<CaExplorer />} />
            <Route path="/validity" element={<ValidityObjects />} />
            <Route path="/alerts" element={<Alerts />} />
          </Routes>
        </div>

        {/* Footer */}
        <footer className="mt-12 pt-6 pb-8 border-t border-cf-border/50">
          <div className="flex flex-col sm:flex-row items-center justify-between gap-4">
            <div className="flex items-center gap-3">
              <a href="https://www.nsf.gov" target="_blank" rel="noopener noreferrer" className="shrink-0">
                <img src="/nsf-logo.png" alt="NSF" className="h-10 w-10 opacity-80" />
              </a>
              <a href="https://www.arin.net/about/community_grants/" target="_blank" rel="noopener noreferrer" className="shrink-0">
                <img src="/arin-logo.svg" alt="ARIN" className="h-8 w-auto opacity-80" />
              </a>
              <div className="text-[11px] text-cf-text-muted leading-snug">
                Supported by the{' '}
                <a href="https://www.nsf.gov" target="_blank" rel="noopener noreferrer" className="text-cf-text-secondary hover:underline font-medium whitespace-nowrap">
                  National Science Foundation
                </a>
                <br />
                and the{' '}
                <a href="https://www.arin.net/about/community_grants/" target="_blank" rel="noopener noreferrer" className="text-cf-text-secondary hover:underline font-medium whitespace-nowrap">
                  ARIN Community Grant Program
                </a>
              </div>
            </div>
            <div className="text-xs text-cf-text-muted text-center sm:text-right leading-relaxed">
              <a href="https://taejoong.github.io" target="_blank" rel="noopener noreferrer" className="text-cf-orange hover:underline">NetSecure Lab</a>
              {' '}&middot; Virginia Tech
              <br />
              <span className="text-[10px]">RPKI Warehouse</span>
            </div>
          </div>
        </footer>
      </main>
    </div>
  );
}
