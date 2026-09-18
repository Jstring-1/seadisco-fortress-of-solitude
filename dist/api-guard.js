// ── Global outgoing-API kill switch ──────────────────────────────────
// One flag every third-party call checks: Discogs (client + loggedFetch),
// YouTube, LOC / Chronicling America, archive.org, lyrics scraping and
// the discography scrapers. Clerk (auth / user admin) is deliberately NOT
// covered — the switch exists to stop data traffic, not sign-in.
//
// The admin toggle persists the value in app_settings ("api_kill_switch")
// so a redeploy doesn't silently turn traffic back on; search-api.ts loads
// it at boot via setApiKilled().
let _killed = false;
export function isApiKilled() {
    return _killed;
}
export function setApiKilled(v) {
    _killed = !!v;
}
// Throw before an outgoing request when the switch is on.
export function assertApiAllowed(service) {
    if (_killed)
        throw new Error(`API kill switch is active — ${service} request blocked`);
}
