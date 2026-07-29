// Client HTTP minimal pour l'API backend.
// Le jeton JWT est conservé dans localStorage et injecté dans les requêtes.

const TOKEN_KEY = "kanyon_token";

export function getToken() {
  return localStorage.getItem(TOKEN_KEY);
}

export function setToken(token) {
  if (token) localStorage.setItem(TOKEN_KEY, token);
  else localStorage.removeItem(TOKEN_KEY);
}

async function request(path, { method = "GET", body, form, auth = true } = {}) {
  const headers = {};
  const token = getToken();
  if (auth && token) headers["Authorization"] = `Bearer ${token}`;

  let payload;
  if (form) {
    // application/x-www-form-urlencoded (login OAuth2)
    headers["Content-Type"] = "application/x-www-form-urlencoded";
    payload = new URLSearchParams(form).toString();
  } else if (body !== undefined) {
    headers["Content-Type"] = "application/json";
    payload = JSON.stringify(body);
  }

  const resp = await fetch(`/api${path}`, { method, headers, body: payload });

  if (resp.status === 204) return null;
  const data = await resp.json().catch(() => null);
  if (!resp.ok) {
    const detail = (data && data.detail) || resp.statusText;
    const err = new Error(typeof detail === "string" ? detail : "Erreur");
    err.status = resp.status;
    throw err;
  }
  return data;
}

export const api = {
  register: (email, password, full_name) =>
    request("/auth/register", { method: "POST", body: { email, password, full_name }, auth: false }),
  login: (email, password) =>
    request("/auth/login", { method: "POST", form: { username: email, password }, auth: false }),
  me: () => request("/auth/me"),
  plans: () => request("/plans", { auth: false }),
  subscription: () => request("/subscription"),
  subscribe: (plan_code) => request("/billing/subscribe", { method: "POST", body: { plan_code } }),
  cancel: () => request("/billing/cancel", { method: "POST" }),
  market: (resource, params) => {
    const qs = new URLSearchParams(params).toString();
    return request(`/market/${resource}?${qs}`);
  },
  priceBond: (bond) => request("/pricer/price", { method: "POST", body: bond }),
  tenors: (date_marche) => request(`/pricer/tenors?date_marche=${date_marche}`),
  factsheetHtml: async (body) => {
    const headers = { "Content-Type": "application/json" };
    const token = getToken();
    if (token) headers["Authorization"] = `Bearer ${token}`;
    const resp = await fetch("/api/reporting/factsheet", {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });
    if (!resp.ok) {
      const err = new Error("Erreur de génération de la fiche");
      err.status = resp.status;
      throw err;
    }
    return resp.text();
  },
  factsheetPdf: async (body) => {
    const headers = { "Content-Type": "application/json" };
    const token = getToken();
    if (token) headers["Authorization"] = `Bearer ${token}`;
    const resp = await fetch("/api/reporting/factsheet.pdf", {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });
    if (!resp.ok) {
      const err = new Error("Erreur de génération du PDF");
      err.status = resp.status;
      throw err;
    }
    return resp.blob();
  },
  optimizeEquities: (debut, fin, objective) =>
    request(`/portfolio/equities/optimize?debut=${debut}&fin=${fin}&objective=${objective}`),
  strategies: () => request("/portfolio/strategies"),
  optimize: (body) => request("/portfolio/optimize", { method: "POST", body }),
  stress: (body) => request("/portfolio/stress", { method: "POST", body }),
  compliance: (body) => request("/portfolio/compliance", { method: "POST", body }),
};
