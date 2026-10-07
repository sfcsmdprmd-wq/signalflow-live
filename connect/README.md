# SignalFlow Connect prototype

Separate from SignalFlow Live. Standalone mobile-friendly interface for a reporter to select an administrator-defined studio destination, test their real microphone and explore a simulated call flow.

## Running
`npm start` (Node 20+). Open http://localhost:8080. Microphone permissions require HTTPS on remote devices; Railway supplies HTTPS.

## Configuring destination names
Set `CONNECT_DESTINATIONS` to a JSON array, e.g.
`[{"id":"studio1","name":"Studio 1","description":"Main on-air studio"},{"id":"studio2","name":"Studio 2","description":"Secondary studio"}]`

## Prototype limitations
No authentication, SIP registration, RTP media, or actual outbound calling is implemented. All calls are clearly labelled DEMO. Do not use as a broadcast contribution link yet. Real calls require an authenticated WebRTC/SIP media gateway, SIP provider credentials, Opus codec negotiation, STUN/TURN, exclusive-call server lock, security review and tests against the intended studio codec. This prototype deliberately never sends microphone audio off-device.
