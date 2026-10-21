## Release 0.18.0

Bumps both crates from 0.17.1 to 0.18.0.

### Changes
- feat: re-export `ClientFeature` and `ClientFeatures` (#143)
- chore: upgrade yggdrasil to 0.21.6 (#142)
- feat: add support for `list_features` (#141)
- fix: SDK now sends metrics when receiving 304 responses from the server (#140)
- feat: etags are now respected to Unleash/Edge (#139)
- feat: back off polling based on HTTP status codes (#137)
