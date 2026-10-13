## Release 0.18.0

Bumps both crates from 0.17.1 to 0.18.0.

### Changes
- fix: SDK now sends metrics when receiving 304 responses from the server (#140)
- feat: etags are now respected to Unleash/Edge (#139)
- feat: back off polling based on HTTP status codes (#137)
